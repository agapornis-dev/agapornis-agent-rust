use super::*;

use bollard::{
    config::RestartPolicyNameEnum,
    errors::Error as BollardError,
    models::{ContainerUpdateBody, RestartPolicy},
    query_parameters::{
        KillContainerOptionsBuilder, RemoveContainerOptionsBuilder, StopContainerOptionsBuilder,
    },
};

mod recreation;

impl DockerManager {
    pub async fn start(&self, id: &str) -> Result<()> {
        paths::validate_id(id)?;

        let inspect = self.inspect(id).await.ok();
        let needs_bind_repair = inspect
            .as_ref()
            .and_then(|inspect| {
                inspect
                    .pointer("/State/Error")
                    .and_then(Value::as_str)
                    .map(stale_docker_desktop_bind_message)
            })
            .unwrap_or(false);
        let needs_runtime_repair = inspect.as_ref().is_some_and(|inspect| {
            inspect.pointer("/State/Running").and_then(Value::as_bool) != Some(true)
                && !runtime_configuration_ready(inspect)
        });
        if needs_bind_repair || needs_runtime_repair {
            tracing::warn!(
                container_id = %id,
                needs_bind_repair,
                needs_runtime_repair,
                "repairing stale managed Docker container configuration before start"
            );
            self.recreate_with_fresh_bind_mounts(id).await?;
        }

        self.apply_server_config(id).await?;
        self.ensure_disk(id).await?;
        self.protection.manual_recovery(id);
        self.startup_ready.lock().await.remove(id);
        self.startup_checks.lock().await.remove(id);

        self.apply_restart_policy(id).await?;
        match self.start_with_console(id).await {
            Ok(()) => Ok(()),
            Err(error) if stale_docker_desktop_bind_error(&error) => {
                tracing::warn!(
                    container_id = %id,
                    "Docker Desktop bind source disappeared; recreating the container with fresh mounts"
                );
                self.recreate_with_fresh_bind_mounts(id).await?;
                self.start_with_console(id).await
            }
            Err(error) => Err(error),
        }
    }

    pub async fn stop(&self, id: &str) -> Result<()> {
        paths::validate_id(id)?;

        let inspect = self.inspect(id).await?;
        if inspect.pointer("/State/Running").and_then(Value::as_bool) != Some(true) {
            self.detach_console(id).await;
            return Ok(());
        }

        let stop_command = inspect
            .pointer("/Config/Labels/agapornis.stop_command")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim();

        if !stop_command.is_empty() {
            self.disable_restart_policy(id).await?;
        }

        if stop_command == "^C" {
            let options = KillContainerOptionsBuilder::default()
                .signal("SIGINT")
                .build();
            if let Err(error) = self.docker.kill_container(id, Some(options)).await {
                tracing::warn!(
                    container_id = %id,
                    "failed to send configured SIGINT: {error}"
                );
            }
        } else if !stop_command.is_empty()
            && let Err(error) = self.send_command(id, stop_command).await
        {
            tracing::warn!(
                container_id = %id,
                "failed to send configured stop command: {error}"
            );
        }

        if !stop_command.is_empty() && self.wait_until_stopped(id, Duration::from_secs(10)).await? {
            self.detach_console(id).await;
            self.startup_ready.lock().await.remove(id);
            self.startup_checks.lock().await.remove(id);
            return Ok(());
        }

        self.force_graceful_stop(id).await?;
        self.detach_console(id).await;
        self.startup_ready.lock().await.remove(id);
        self.startup_checks.lock().await.remove(id);
        Ok(())
    }

    pub async fn restart(&self, id: &str) -> Result<()> {
        paths::validate_id(id)?;

        self.stop(id).await?;
        self.start(id).await
    }

    /// Pull the configured image and replace only the Docker container. The
    /// managed server directory is a bind mount, so files and database data
    /// remain intact.
    pub async fn recreate(&self, id: &str) -> Result<ContainerRecreation> {
        paths::validate_id(id)?;

        let inspect = self.inspect(id).await?;
        let image = inspect
            .pointer("/Config/Image")
            .and_then(Value::as_str)
            .filter(|image| !image.trim().is_empty())
            .context("container has no image to update")?
            .to_owned();
        let was_running = inspect
            .pointer("/State/Running")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let previous_image_id = inspect
            .pointer("/Image")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();

        // Pull before stopping the workload to keep downtime limited to the
        // actual container replacement.
        self.pull_image(&image).await?;

        if was_running {
            self.stop(id).await?;
        }

        self.recreate_with_fresh_bind_mounts(id).await?;
        let updated_image_id = self
            .inspect(id)
            .await?
            .pointer("/Image")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        self.forget_runtime_state(id).await;

        if was_running {
            self.start(id).await?;
        }
        Ok(ContainerRecreation {
            image,
            image_changed: !previous_image_id.is_empty() && previous_image_id != updated_image_id,
            previous_image_id,
            image_id: updated_image_id,
        })
    }

    pub async fn delete(&self, id: &str) -> Result<()> {
        paths::validate_id(id)?;

        self.detach_console(id).await;

        let options = RemoveContainerOptionsBuilder::default()
            .force(true)
            .v(true)
            .build();

        match self.docker.remove_container(id, Some(options)).await {
            Ok(()) => {}

            // Allow cleanup when the Docker container has already vanished.
            Err(err) if docker_status(&err) == Some(404) => {}

            Err(err) => {
                return Err(err).with_context(|| format!("remove Docker container {id}"));
            }
        }

        let dir = paths::server_dir(id)?;

        if dir.exists() {
            fs::remove_dir_all(&dir)
                .await
                .with_context(|| format!("remove server directory {}", dir.display()))?;
        }

        let limit = paths::disk_limit_path(id)?;

        if let Err(err) = fs::remove_file(&limit).await
            && err.kind() != std::io::ErrorKind::NotFound
        {
            return Err(err)
                .with_context(|| format!("remove disk limit metadata {}", limit.display()));
        }

        let config = paths::config_files_path(id)?;
        if let Err(err) = fs::remove_file(&config).await
            && err.kind() != std::io::ErrorKind::NotFound
        {
            return Err(err)
                .with_context(|| format!("remove configuration metadata {}", config.display()));
        }

        self.disk_cache.lock().await.remove(id);
        self.startup_ready.lock().await.remove(id);
        self.startup_checks.lock().await.remove(id);
        self.protection.remove(id);

        Ok(())
    }

    pub(crate) async fn forget_runtime_state(&self, id: &str) {
        self.detach_console(id).await;
        self.disk_cache.lock().await.remove(id);
        self.startup_ready.lock().await.remove(id);
        self.startup_checks.lock().await.remove(id);
        self.protection.remove(id);
    }

    pub async fn update_resources(&self, spec: ResourceUpdateSpec) -> Result<()> {
        let id = spec.server_id.as_str();
        paths::validate_id(id)?;

        let cpus = effective_cpus(spec.cpu_limit_percentage, spec.cpu_cores);
        let cpuset_cpus = Some(if spec.cpu_pinning {
            pinned_cpu_set(&spec.cpu_pinned_threads)?.unwrap_or_default()
        } else {
            String::new()
        });

        let memory = (spec.memory_bytes > 0).then_some(spec.memory_bytes);

        let nano_cpus = if cpus > 0.0 {
            if !cpus.is_finite() {
                bail!("calculated CPU limit is not finite");
            }

            let nano_cpus = cpus * 1_000_000_000.0;

            if nano_cpus > i64::MAX as f64 {
                bail!("calculated CPU limit is too large");
            }

            Some(nano_cpus.round() as i64)
        } else {
            None
        };

        if memory.is_some() || nano_cpus.is_some() {
            let update = ContainerUpdateBody {
                memory,
                memory_swap: memory
                    .map(|value| value.saturating_add(spec.swap_memory_bytes.max(0))),
                cpuset_cpus,

                // Docker represents --cpus in billionths of one CPU.
                nano_cpus,

                ..Default::default()
            };

            self.docker
                .update_container(id, update)
                .await
                .with_context(|| format!("update resources for Docker container {id}"))?;

            // Docker accepting an update request is not enough for the panel to
            // claim success. Read the effective HostConfig back so API state is
            // only persisted after the daemon reports the requested limits.
            let inspect = self.inspect(id).await?;
            if let Some(expected) = memory {
                let actual = inspect
                    .pointer("/HostConfig/Memory")
                    .and_then(Value::as_i64);
                if actual != Some(expected) {
                    bail!(
                        "Docker reported memory limit {:?}, expected {expected}",
                        actual
                    )
                }
            }
            if let Some(expected) = nano_cpus {
                let actual = inspect
                    .pointer("/HostConfig/NanoCpus")
                    .and_then(Value::as_i64);
                if actual != Some(expected) {
                    bail!(
                        "Docker reported CPU limit {:?}, expected {expected}",
                        actual
                    )
                }
            }
        }

        let disk = effective_disk_limit(
            spec.disk_limit_bytes,
            spec.swap_memory_bytes,
            &spec.swap_memory_storage,
        )?;
        if disk > 0 {
            let path = paths::disk_limit_path(id)?;

            let parent = path
                .parent()
                .context("disk limit metadata path has no parent")?;

            fs::create_dir_all(parent).await?;

            fs::write(&path, disk.to_string())
                .await
                .with_context(|| format!("write disk limit metadata {}", path.display()))?;
            let applied = fs::read_to_string(&path).await?.trim().parse::<i64>()?;
            if applied != disk {
                bail!("disk limit metadata reported {applied}, expected {disk}")
            }
        }

        self.disk_cache.lock().await.remove(id);

        Ok(())
    }

    async fn apply_restart_policy(&self, id: &str) -> Result<()> {
        let update = ContainerUpdateBody {
            restart_policy: Some(RestartPolicy {
                name: Some(RestartPolicyNameEnum::ON_FAILURE),
                maximum_retry_count: Some(2),
            }),
            ..Default::default()
        };

        self.docker
            .update_container(id, update)
            .await
            .with_context(|| format!("set restart policy for Docker container {id}"))
    }

    async fn apply_server_config(&self, id: &str) -> Result<()> {
        let metadata = paths::config_files_path(id)?;
        let descriptor = match fs::read_to_string(&metadata).await {
            Ok(descriptor) => descriptor,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };

        if descriptor.trim().is_empty() || descriptor.trim() == "{}" {
            return Ok(());
        }

        let inspect = self.inspect(id).await?;
        let network = inspect
            .pointer("/Config/Labels/agapornis.network")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| {
                std::env::var("AGAPORNIS_DOCKER_NETWORK").unwrap_or_else(|_| "agapornis_ntw".into())
            });
        let docker_interface = docker_network_interface(&self.docker, &network).await?;
        let (root, _, _, _) = self.root(id).await?;

        apply_config_files(&root, &descriptor, &docker_interface).await
    }

    async fn disable_restart_policy(&self, id: &str) -> Result<()> {
        let update = ContainerUpdateBody {
            restart_policy: Some(RestartPolicy {
                name: Some(RestartPolicyNameEnum::NO),
                maximum_retry_count: Some(0),
            }),
            ..Default::default()
        };

        self.docker
            .update_container(id, update)
            .await
            .with_context(|| {
                format!("disable restart policy before stopping Docker container {id}")
            })
    }

    async fn force_graceful_stop(&self, id: &str) -> Result<()> {
        let options = StopContainerOptionsBuilder::default().t(10).build();

        match self.docker.stop_container(id, Some(options)).await {
            Ok(()) => Ok(()),
            Err(err) if docker_status(&err) == Some(304) => Ok(()),
            Err(err) => Err(err).with_context(|| format!("stop Docker container {id}")),
        }
    }

    async fn wait_until_stopped(&self, id: &str, timeout: Duration) -> Result<bool> {
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            let inspect = self.inspect(id).await?;
            if inspect.pointer("/State/Running").and_then(Value::as_bool) != Some(true) {
                return Ok(true);
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(false);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn ensure_disk(&self, id: &str) -> Result<()> {
        let (usage, limit) = self.disk_force(id).await?;

        if limit > 0 && usage > limit {
            self.protection.mark(id, "disk-limit-exceeded");

            bail!(
                "Server disk limit exceeded \
                 ({usage} / {limit} bytes). \
                 Delete files before starting the server."
            );
        }

        self.protection.clear_disk(id);

        Ok(())
    }
}

pub(super) fn stale_docker_desktop_bind_error(error: &anyhow::Error) -> bool {
    stale_docker_desktop_bind_message(&format!("{error:#}"))
}

fn stale_docker_desktop_bind_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("docker-desktop-bind-mounts")
        && message.contains("no such file or directory")
        && (message.contains("error mounting") || message.contains("mount src="))
}

fn docker_status(err: &BollardError) -> Option<u16> {
    match err {
        BollardError::DockerResponseServerError { status_code, .. } => Some(*status_code),

        _ => None,
    }
}
