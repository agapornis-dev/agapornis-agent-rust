use super::*;

#[derive(Clone)]
pub struct ServerService(pub AppState);
#[tonic::async_trait]
impl proto::server_management_server::ServerManagement for ServerService {
    async fn create_server(
        &self,
        request: Request<CreateServerRequest>,
    ) -> Result<Response<CreateServerResponse>, Status> {
        let r = request.into_inner();
        let spec = CreateSpec {
            server_id: r.server_id.clone(),
            image: r.docker_image,
            internal_port: r.internal_port,
            env: r.env_vars,
            memory_bytes: r.memory_bytes,
            cpu_limit_percentage: r.cpu_limit_percentage,
            cpu_cores: r.cpu_cores,
            disk_limit_bytes: r.disk_limit_bytes,
            startup_command: r.startup_command,
            install_image: r.install_image,
            install_entrypoint: r.install_entrypoint,
            install_script: r.install_script,
            config_files_json: r.config_files_json,
            host_port: r.host_port,
            network_owner_id: r.network_owner_id,
            expose_public_port: r.expose_public_port,
            port_mappings: r.port_mappings.into_iter()
                .map(|port| (port.internal_port, port.host_port)).collect(),
        };
        Ok(Response::new(match self.0.docker.create(spec).await {
            Ok(port) => CreateServerResponse {
                success: true,
                assigned_host_port: port,
                error_message: String::new(),
            },
            Err(e) => {
                error!(server=%r.server_id,error=%e,"create failed");
                CreateServerResponse {
                    success: false,
                    assigned_host_port: 0,
                    error_message: e.to_string(),
                }
            }
        }))
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
    async fn delete_server(
        &self,
        r: Request<ServerActionRequest>,
    ) -> Result<Response<ServerActionResponse>, Status> {
        Ok(Response::new(action(
            self.0.docker.delete(&r.into_inner().server_id).await,
        )))
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
                )
                .await,
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
        let m = self.0.docker.metrics(&id).await.unwrap_or_default();
        Ok(Response::new(ServerMetricsResponse {
            memory_usage_bytes: m.memory_usage,
            memory_limit_bytes: m.memory_limit,
            cpu_percentage: m.cpu_percent,
            network_read_bytes: m.network_read,
            network_write_bytes: m.network_write,
            disk_usage_bytes: m.disk_usage,
            disk_limit_bytes: m.disk_limit,
            status: m.status,
        }))
    }
    async fn send_command(
        &self,
        r: Request<SendCommandRequest>,
    ) -> Result<Response<ServerActionResponse>, Status> {
        let r = r.into_inner();
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
