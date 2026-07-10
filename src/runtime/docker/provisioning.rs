use super::*;
#[cfg(unix)]
use crate::process;
use bollard::{
    Docker,
    query_parameters::{CreateContainerOptionsBuilder, CreateImageOptionsBuilder},
};
use futures_util::StreamExt;

mod container;
mod installer;
mod ports;

use container::build_container;
use ports::add_port_mapping;

#[cfg(test)]
pub(super) use installer::{append_tail, installer_exit_status};
#[cfg(test)]
pub(super) use ports::PortReservation;

impl DockerManager {
    pub async fn connect_with_retry(protection: Arc<ProtectionState>) -> Self {
        loop {
            match Self::new(protection.clone()) {
                Ok(manager) => return manager,
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        retry_seconds = DOCKER_CONNECT_RETRY_INTERVAL.as_secs(),
                        "Docker Engine is unavailable; the agent will retry"
                    );
                    tokio::time::sleep(DOCKER_CONNECT_RETRY_INTERVAL).await;
                }
            }
        }
    }

    pub fn new(protection: Arc<ProtectionState>) -> Result<Self> {
        let docker = Docker::connect_with_local_defaults().context("connect to Docker Engine")?;

        Ok(Self {
            docker,
            protection,
            disk_cache: Arc::new(Mutex::new(HashMap::new())),
            console_bindings: Arc::new(Mutex::new(HashMap::new())),
            reserved_ports: Arc::new(Mutex::new(HashSet::new())),
            startup_ready: Arc::new(Mutex::new(HashSet::new())),
            startup_checks: Arc::new(Mutex::new(HashMap::new())),
            disk_scans: Arc::new(Semaphore::new(1)),
        })
    }

    pub async fn create(&self, spec: CreateSpec) -> Result<i32> {
        self.create_with_progress(spec, |_, _, _| {}).await
    }

    pub async fn create_with_progress<F>(&self, spec: CreateSpec, report: F) -> Result<i32>
    where
        F: Fn(&str, i32, &str) + Send + Sync + 'static,
    {
        let report: ProvisioningReporter = Arc::new(report);
        report(
            "validating",
            5,
            "Validating server and Docker configuration",
        );
        paths::validate_id(&spec.server_id)?;

        if spec.image.trim().is_empty() {
            bail!("Docker image is required");
        }

        report("allocating-port", 10, "Reserving the server network port");
        let (host_port, _port_reservation) = self.reserve_host_port(&spec).await?;

        report(
            "preparing-storage",
            15,
            "Preparing server storage and resource metadata",
        );
        let host = paths::server_dir(&spec.server_id)?;
        fs::create_dir_all(&host).await?;

        if spec.disk_limit_bytes > 0 {
            let metadata = paths::disk_limit_path(&spec.server_id)?;

            fs::create_dir_all(
                metadata
                    .parent()
                    .context("disk limit metadata path has no parent")?,
            )
            .await?;

            fs::write(metadata, spec.disk_limit_bytes.to_string()).await?;
        }

        let config_metadata = paths::config_files_path(&spec.server_id)?;
        fs::create_dir_all(
            config_metadata
                .parent()
                .context("configuration metadata path has no parent")?,
        )
        .await?;
        fs::write(&config_metadata, &spec.config_files_json).await?;

        if !spec.install_image.trim().is_empty() && !spec.install_script.trim().is_empty() {
            self.run_installer(&spec, &host, &report).await?;
        }

        report("configuring-network", 62, "Preparing the Docker network");
        let network =
            std::env::var("AGAPORNIS_DOCKER_NETWORK").unwrap_or_else(|_| "agapornis_ntw".into());

        self.ensure_network(&network).await?;
        let docker_interface = docker_network_interface(&self.docker, &network).await?;

        report(
            "configuring-server",
            67,
            "Applying generated server configuration",
        );
        apply_config_files(&host, &spec.config_files_json, &docker_interface).await?;
        validate_startup(&host, &spec.startup_command)?;

        report(
            "pulling-runtime-image",
            72,
            "Pulling the selected runtime image",
        );
        self.pull_image(&spec.image).await?;

        #[cfg(unix)]
        {
            // Containers run as uid/gid 999. Linux bind mounts preserve host
            // inode ownership, so the host directory must be writable by that
            // numeric identity before Docker attaches it to the container.
            let _ = process::run("chown", ["-R", "999:999", host.to_string_lossy().as_ref()]).await;
        }

        let data_path = paths::data_path(&spec.image, &spec.env);
        let config = build_container(&spec, &host, &network, data_path, host_port)?;

        let options = CreateContainerOptionsBuilder::default()
            .name(&spec.server_id)
            .build();

        report(
            "creating-container",
            88,
            "Creating the final server container",
        );
        let response = self
            .docker
            .create_container(Some(options), config)
            .await
            .with_context(|| format!("create Docker container {}", spec.server_id))?;
        for warning in response.warnings {
            tracing::warn!(
                container_id = %spec.server_id,
                "Docker container creation warning: {warning}"
            );
        }

        self.disk_cache.lock().await.remove(&spec.server_id);

        report(
            "container-ready",
            95,
            "Docker container created successfully",
        );
        Ok(host_port)
    }

    pub(super) async fn pull_image(&self, image: &str) -> Result<()> {
        let mut builder = CreateImageOptionsBuilder::default().from_image(image);

        // The Engine API treats an untagged image differently from the
        // `docker pull IMAGE` CLI command. Explicitly request "latest" when
        // the reference contains neither a tag nor a digest.
        if !image_has_tag_or_digest(image) {
            builder = builder.tag("latest");
        }

        let options = builder.build();

        let mut stream = self.docker.create_image(Some(options), None, None);

        while let Some(result) = stream.next().await {
            result.with_context(|| format!("pull Docker image {image}"))?;
        }

        Ok(())
    }
}

fn image_has_tag_or_digest(image: &str) -> bool {
    if image.contains('@') {
        return true;
    }

    image
        .rsplit('/')
        .next()
        .is_some_and(|last| last.contains(':'))
}
