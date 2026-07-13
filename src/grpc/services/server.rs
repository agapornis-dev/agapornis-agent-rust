use super::*;
use crate::paths;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

#[derive(Clone)]
pub struct ServerService(pub AppState);
#[tonic::async_trait]
impl proto::server_management_server::ServerManagement for ServerService {
    async fn create_server(
        &self,
        request: Request<CreateServerRequest>,
    ) -> Result<Response<CreateServerResponse>, Status> {
        let (server_id, spec) = create_spec(request.into_inner());
        Ok(Response::new(match self.0.docker.create(spec).await {
            Ok(port) => CreateServerResponse {
                success: true,
                assigned_host_port: port,
                error_message: String::new(),
            },
            Err(e) => {
                error!(
                    server = %server_id,
                    error = %format!("{e:#}"),
                    "create failed"
                );
                CreateServerResponse {
                    success: false,
                    assigned_host_port: 0,
                    error_message: e.to_string(),
                }
            }
        }))
    }

    type CreateServerStreamStream = ResponseStream<CreateServerProgress>;

    async fn create_server_stream(
        &self,
        request: Request<CreateServerRequest>,
    ) -> Result<Response<Self::CreateServerStreamStream>, Status> {
        let (server_id, spec) = create_spec(request.into_inner());
        let docker = self.0.docker.clone();
        let (sender, receiver) = mpsc::unbounded_channel();
        let progress_sender = sender.clone();

        tokio::spawn(async move {
            let result = docker
                .create_with_progress(spec, move |phase, progress, message| {
                    let _ = progress_sender.send(Ok(CreateServerProgress {
                        phase: phase.into(),
                        progress,
                        message: message.into(),
                        ..Default::default()
                    }));
                })
                .await;

            let final_message = match result {
                Ok(port) => CreateServerProgress {
                    phase: "complete".into(),
                    progress: 100,
                    message: "Server container is ready".into(),
                    complete: true,
                    success: true,
                    assigned_host_port: port,
                    ..Default::default()
                },
                Err(error) => {
                    error!(
                        server = %server_id,
                        error = %format!("{error:#}"),
                        "streamed create failed"
                    );
                    CreateServerProgress {
                        phase: "failed".into(),
                        progress: 100,
                        message: "Server provisioning failed".into(),
                        complete: true,
                        success: false,
                        error_message: error.to_string(),
                        ..Default::default()
                    }
                }
            };
            let _ = sender.send(Ok(final_message));
        });

        Ok(Response::new(Box::pin(UnboundedReceiverStream::new(
            receiver,
        ))))
    }
    async fn start_server(
        &self,
        r: Request<ServerActionRequest>,
    ) -> Result<Response<ServerActionResponse>, Status> {
        Ok(Response::new(action(
            self.0.docker.start(&r.into_inner().server_id).await,
        )))
    }
    async fn stop_server(
        &self,
        r: Request<ServerActionRequest>,
    ) -> Result<Response<ServerActionResponse>, Status> {
        Ok(Response::new(action(
            self.0.docker.stop(&r.into_inner().server_id).await,
        )))
    }
    async fn restart_server(
        &self,
        r: Request<ServerActionRequest>,
    ) -> Result<Response<ServerActionResponse>, Status> {
        Ok(Response::new(action(
            self.0.docker.restart(&r.into_inner().server_id).await,
        )))
    }
    async fn recreate_server(
        &self,
        r: Request<ServerActionRequest>,
    ) -> Result<Response<ServerActionResponse>, Status> {
        Ok(Response::new(
            match self.0.docker.recreate(&r.into_inner().server_id).await {
                Ok(update) => ServerActionResponse {
                    success: true,
                    error_message: String::new(),
                    image: update.image,
                    previous_image_id: update.previous_image_id,
                    image_id: update.image_id,
                    image_changed: update.image_changed,
                },
                Err(error) => ServerActionResponse {
                    success: false,
                    error_message: error.to_string(),
                    ..Default::default()
                },
            },
        ))
    }
    async fn delete_server(
        &self,
        r: Request<ServerActionRequest>,
    ) -> Result<Response<ServerActionResponse>, Status> {
        let id = r.into_inner().server_id;
        let result = self.0.docker.delete(&id).await;
        if result.is_ok() {
            self.0.console.remove(&id).await;
        }
        Ok(Response::new(action(result)))
    }
    async fn update_server_resources(
        &self,
        r: Request<UpdateServerResourcesRequest>,
    ) -> Result<Response<ServerActionResponse>, Status> {
        let r = r.into_inner();
        Ok(Response::new(action(
            self.0
                .docker
                .update_resources(
                    &r.server_id,
                    r.memory_bytes,
                    r.cpu_limit_percentage,
                    r.cpu_cores,
                    r.disk_limit_bytes,
                    r.cpu_pinning,
                    &r.cpu_pinned_threads,
                    r.swap_memory_bytes,
                    &r.swap_memory_storage,
                )
                .await,
        )))
    }
    async fn update_server_ports(
        &self,
        r: Request<UpdateServerPortsRequest>,
    ) -> Result<Response<ServerActionResponse>, Status> {
        let r = r.into_inner();
        let mappings = r
            .port_mappings
            .into_iter()
            .map(|mapping| (mapping.internal_port, mapping.host_port))
            .collect();
        Ok(Response::new(action(
            self.0.docker.update_ports(&r.server_id, mappings).await,
        )))
    }
    async fn get_node_stats(
        &self,
        _: Request<NodeStatsRequest>,
    ) -> Result<Response<NodeStatsResponse>, Status> {
        Ok(Response::new(match node::stats().await {
            Ok(s) => NodeStatsResponse {
                cpu_percentage: s.cpu,
                memory_usage_bytes: s.memory_used,
                memory_total_bytes: s.memory_total,
                disk_usage_bytes: s.disk_used,
                disk_total_bytes: s.disk_total,
                status: "healthy".into(),
                error_message: "".into(),
                uptime_seconds: s.uptime,
                cpu_count: s.cpus,
            },
            Err(e) => NodeStatsResponse {
                status: "unhealthy".into(),
                error_message: e.to_string(),
                ..Default::default()
            },
        }))
    }
    async fn get_crowd_sec_alerts(
        &self,
        _: Request<CrowdSecAlertsRequest>,
    ) -> Result<Response<CrowdSecAlertsResponse>, Status> {
        Ok(Response::new(node::crowdsec(&self.0.config).await))
    }
    async fn get_server_stats(
        &self,
        r: Request<ServerActionRequest>,
    ) -> Result<Response<ServerMetricsResponse>, Status> {
        let id = r.into_inner().server_id;
        let m = self.0.docker.metrics(&id).await.map_err(internal)?;
        Ok(Response::new(server_metrics_response(m)))
    }
    async fn send_command(
        &self,
        r: Request<SendCommandRequest>,
    ) -> Result<Response<ServerActionResponse>, Status> {
        let r = r.into_inner();
        if let Err(error) = paths::validate_id(&r.server_id) {
            return Ok(Response::new(failure(&error.to_string())));
        }
        if r.command.trim().is_empty() {
            return Ok(Response::new(failure("command cannot be empty")));
        }
        if r.command.len() > 4096 {
            return Ok(Response::new(failure(
                "command exceeds the 4096 character limit",
            )));
        }
        if !self.0.protection.accept_command(&r.server_id) {
            return Ok(Response::new(failure(
                "console command rate limit exceeded; retry in a few seconds",
            )));
        }
        Ok(Response::new(action(
            self.0.docker.send_command(&r.server_id, &r.command).await,
        )))
    }
    async fn test_database_connection(
        &self,
        r: Request<DatabaseConnectionTestRequest>,
    ) -> Result<Response<DatabaseConnectionTestResponse>, Status> {
        let r = r.into_inner();
        let result = self
            .0
            .docker
            .test_database_connection(DatabaseConnectionSpec {
                server_id: &r.server_id,
                database_type: &r.database_type,
                host: &r.host,
                port: r.port,
                database_name: &r.database_name,
                username: &r.username,
                password: &r.password,
                docker_image: &r.docker_image,
            })
            .await;
        Ok(Response::new(match result {
            Ok(latency_ms) => DatabaseConnectionTestResponse {
                success: true,
                error_message: String::new(),
                latency_ms,
            },
            Err(error) => DatabaseConnectionTestResponse {
                success: false,
                error_message: error.to_string(),
                latency_ms: 0,
            },
        }))
    }
    type StreamConsoleStream = ResponseStream<ConsoleMessage>;
    async fn stream_console(
        &self,
        r: Request<ServerActionRequest>,
    ) -> Result<Response<Self::StreamConsoleStream>, Status> {
        let id = r.into_inner().server_id;

        // ConsoleHub retains a sender, history buffer, and reader task per
        // server. Verify the identifier and container before allocating that
        // state so malformed or stale requests cannot create retrying tasks.
        paths::validate_id(&id).map_err(|error| Status::invalid_argument(error.to_string()))?;
        self.0.docker.inspect(&id).await.map_err(internal)?;

        let (history, receiver) = self.0.console.subscribe(&id).await;
        let replay = tokio_stream::iter(
            history
                .into_iter()
                .map(|line| Ok(ConsoleMessage { log_line: line })),
        );
        let live = BroadcastStream::new(receiver).filter_map(|v| async move {
            match v {
                Ok(line) => Some(Ok(ConsoleMessage { log_line: line })),
                Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)) => {
                    Some(Ok(ConsoleMessage {
                        log_line: format!("[agent] console stream dropped {n} lines"),
                    }))
                }
            }
        });
        Ok(Response::new(Box::pin(replay.chain(live))))
    }
    async fn get_update_status(
        &self,
        _: Request<UpdateStatusRequest>,
    ) -> Result<Response<UpdateStatusResponse>, Status> {
        let s = self.0.update.status();
        Ok(Response::new(UpdateStatusResponse {
            version: s.version,
            runtime_identifier: s.runtime,
            executable_path: s.executable,
            staging_directory: s.staging,
            restart_required: s.restart_required,
            pending_artifact: s.pending,
        }))
    }
    async fn apply_update(
        &self,
        r: Request<ApplyUpdateRequest>,
    ) -> Result<Response<ApplyUpdateResponse>, Status> {
        let r = r.into_inner();
        Ok(Response::new(
            match self.0.update.stage(&r.artifact_url, &r.sha256).await {
                Ok(v) => ApplyUpdateResponse {
                    success: true,
                    message: v.message,
                    staged_path: v.staged,
                    restart_required: v.restart_required,
                },
                Err(e) => ApplyUpdateResponse {
                    success: false,
                    message: e.to_string(),
                    staged_path: "".into(),
                    restart_required: false,
                },
            },
        ))
    }
    async fn install_certificate(
        &self,
        r: Request<InstallCertificateRequest>,
    ) -> Result<Response<CertificateActionResponse>, Status> {
        let r = r.into_inner();
        Ok(Response::new(
            match self
                .0
                .certificates
                .install(
                    &r.certificate_pem,
                    &r.private_key_pem,
                    &r.ca_certificate_pem,
                    &r.expected_fingerprint,
                )
                .await
            {
                Ok(fp) => CertificateActionResponse {
                    success: true,
                    fingerprint: fp,
                    error_message: "".into(),
                },
                Err(e) => CertificateActionResponse {
                    success: false,
                    fingerprint: "".into(),
                    error_message: e.to_string(),
                },
            },
        ))
    }
    async fn rollback_certificate(
        &self,
        _: Request<RollbackCertificateRequest>,
    ) -> Result<Response<CertificateActionResponse>, Status> {
        Ok(Response::new(match self.0.certificates.rollback().await {
            Ok(fp) => CertificateActionResponse {
                success: true,
                fingerprint: fp,
                error_message: "".into(),
            },
            Err(e) => CertificateActionResponse {
                success: false,
                fingerprint: "".into(),
                error_message: e.to_string(),
            },
        }))
    }
}

fn create_spec(r: CreateServerRequest) -> (String, CreateSpec) {
    let server_id = r.server_id.clone();
    let spec = CreateSpec {
        server_id: server_id.clone(),
        image: r.docker_image,
        internal_port: r.internal_port,
        env: r.env_vars,
        memory_bytes: r.memory_bytes,
        cpu_limit_percentage: r.cpu_limit_percentage,
        cpu_cores: r.cpu_cores,
        disk_limit_bytes: r.disk_limit_bytes,
        cpu_pinning: r.cpu_pinning,
        cpu_pinned_threads: r.cpu_pinned_threads,
        swap_memory_bytes: r.swap_memory_bytes,
        swap_memory_storage: r.swap_memory_storage,
        startup_command: r.startup_command,
        stop_command: r.stop_command,
        startup_done: r.startup_done,
        install_image: r.install_image,
        install_entrypoint: r.install_entrypoint,
        install_script: r.install_script,
        config_files_json: r.config_files_json,
        host_port: r.host_port,
        network_owner_id: r.network_owner_id,
        expose_public_port: r.expose_public_port,
        port_mappings: r
            .port_mappings
            .into_iter()
            .map(|port| (port.internal_port, port.host_port))
            .collect(),
    };
    (server_id, spec)
}

fn server_metrics_response(metrics: crate::docker::Metrics) -> ServerMetricsResponse {
    ServerMetricsResponse {
        memory_usage_bytes: metrics.memory_usage,
        memory_limit_bytes: metrics.memory_limit,
        cpu_percentage: metrics.cpu_percent,
        network_read_bytes: metrics.network_read,
        network_write_bytes: metrics.network_write,
        disk_usage_bytes: metrics.disk_usage,
        disk_limit_bytes: metrics.disk_limit,
        status: metrics.status,
        uptime_seconds: metrics.uptime_seconds,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_metrics_are_copied_into_the_grpc_contract() {
        let response = server_metrics_response(crate::docker::Metrics {
            memory_usage: 900,
            memory_limit: 2_000,
            cpu_percent: 40.0,
            network_read: 440,
            network_write: 550,
            disk_usage: 600,
            disk_limit: 700,
            status: "starting".into(),
            uptime_seconds: 800,
        });

        assert_eq!(response.cpu_percentage, 40.0);
        assert_eq!(response.memory_usage_bytes, 900);
        assert_eq!(response.memory_limit_bytes, 2_000);
        assert_eq!(response.network_read_bytes, 440);
        assert_eq!(response.network_write_bytes, 550);
    }
}
