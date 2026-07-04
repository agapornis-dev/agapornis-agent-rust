use super::*;

use bollard::{
    config::RestartPolicyNameEnum,
    errors::Error as BollardError,
    models::{ContainerUpdateBody, RestartPolicy},
    query_parameters::{
        RemoveContainerOptionsBuilder,
        RestartContainerOptionsBuilder,
        StopContainerOptionsBuilder,
    },
};

impl DockerManager {
    pub async fn start(&self, id: &str) -> Result<()> {
        paths::validate_id(id)?;

        self.ensure_disk(id).await?;
        self.protection.manual_recovery(id);

        self.apply_restart_policy(id).await?;
        self.start_with_console(id).await
    }

    pub async fn stop(&self, id: &str) -> Result<()> {
        paths::validate_id(id)?;

        self.detach_console(id).await;

        let options = StopContainerOptionsBuilder::default()
            .t(10)
            .build();

        match self.docker.stop_container(id, Some(options)).await {
            Ok(()) => Ok(()),

            // Docker returns 304 when the container is already stopped.
            Err(err) if docker_status(&err) == Some(304) => Ok(()),

            Err(err) => Err(err)
                .with_context(|| format!("stop Docker container {id}")),
        }
    }

    pub async fn restart(&self, id: &str) -> Result<()> {
        paths::validate_id(id)?;

        self.detach_console(id).await;
        self.ensure_disk(id).await?;
        self.protection.manual_recovery(id);

        self.apply_restart_policy(id).await?;

        let options = RestartContainerOptionsBuilder::default()
            .t(10)
            .build();

        self.docker
            .restart_container(id, Some(options))
            .await
            .with_context(|| format!("restart Docker container {id}"))?;

        self.attach_running_console(id).await
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
                return Err(err)
                    .with_context(|| format!("remove Docker container {id}"));
            }
        }

        let dir = paths::server_dir(id)?;

        if dir.exists() {
            fs::remove_dir_all(&dir)
                .await
                .with_context(|| {
                    format!(
                        "remove server directory {}",
                        dir.display()
                    )
                })?;
        }

        let limit = paths::disk_limit_path(id)?;

        if let Err(err) = fs::remove_file(&limit).await
            && err.kind() != std::io::ErrorKind::NotFound
        {
            return Err(err).with_context(|| {
                format!(
                    "remove disk limit metadata {}",
                    limit.display()
                )
            });
        }

        self.disk_cache.lock().await.remove(id);
        self.protection.remove(id);

        Ok(())
    }

    pub async fn update_resources(
        &self,
        id: &str,
        memory: i64,
        percent: i32,
        cores: f64,
        disk: i64,
    ) -> Result<()> {
        paths::validate_id(id)?;

        let cpus = effective_cpus(percent, cores);

        let memory = (memory > 0).then_some(memory);

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
                memory_swap: memory,

                // Docker represents --cpus in billionths of one CPU.
                nano_cpus,

                ..Default::default()
            };

            self.docker
                .update_container(id, update)
                .await
                .with_context(|| {
                    format!(
                        "update resources for Docker container {id}"
                    )
                })?;
        }

        if disk > 0 {
            let path = paths::disk_limit_path(id)?;

            let parent = path
                .parent()
                .context("disk limit metadata path has no parent")?;

            fs::create_dir_all(parent).await?;

            fs::write(&path, disk.to_string())
                .await
                .with_context(|| {
                    format!(
                        "write disk limit metadata {}",
                        path.display()
                    )
                })?;
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
            .with_context(|| {
                format!(
                    "set restart policy for Docker container {id}"
                )
            })
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

fn docker_status(err: &BollardError) -> Option<u16> {
    match err {
        BollardError::DockerResponseServerError {
            status_code,
            ..
        } => Some(*status_code),

        _ => None,
    }
}