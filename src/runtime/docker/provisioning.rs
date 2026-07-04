use super::*;

use bollard::{
    container::AttachContainerResults,
    models::{
        ContainerCreateBody, EndpointSettings, HealthConfig, HostConfig,
        Mount, MountType, NetworkingConfig,
        PortBinding, RestartPolicy, RestartPolicyNameEnum,
    },
    query_parameters::{
        AttachContainerOptionsBuilder, CreateContainerOptionsBuilder,
        CreateImageOptionsBuilder, RemoveContainerOptionsBuilder,
        WaitContainerOptionsBuilder,
    },
    Docker,
};
use rand::RngExt;
use futures_util::StreamExt;

impl DockerManager {
    pub fn new(protection: Arc<ProtectionState>) -> Result<Self> {
        let docker = Docker::connect_with_local_defaults()
            .context("connect to Docker Engine")?;

        Ok(Self {
            docker,
            protection,
            disk_cache: Arc::new(Mutex::new(HashMap::new())),
            console_bindings: Arc::new(Mutex::new(HashMap::new())),
            reserved_ports: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    pub async fn create(&self, spec: CreateSpec) -> Result<i32> {
        paths::validate_id(&spec.server_id)?;

        if spec.image.trim().is_empty() {
            bail!("Docker image is required");
        }

        let host_port = if spec.expose_public_port {
            if spec.host_port > 0 {
                let port = u16::try_from(spec.host_port)
                    .context("host port is outside the valid range")?;

                ensure_port(port)?;

                self.reserved_ports.lock().await.insert(port);

                spec.host_port
            } else {
                i32::from(self.find_port().await?)
            }
        } else {
            0
        };

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

        if !spec.install_image.trim().is_empty()
            && !spec.install_script.trim().is_empty()
        {
            self.run_installer(&spec, &host).await?;
        }

        apply_config_files(&host, &spec.config_files_json).await?;
        validate_startup(&host, &spec.startup_command)?;

        self.pull_image(&spec.image).await?;

        #[cfg(unix)]
        {
            let _ = process::run(
                "chown",
                [
                    "-R",
                    "999:999",
                    host.to_string_lossy().as_ref(),
                ],
            )
            .await;
        }

        let network = std::env::var("AGAPORNIS_DOCKER_NETWORK")
            .unwrap_or_else(|_| "agapornis_ntw".into());

        self.ensure_network(&network).await?;

        let data_path = paths::data_path(&spec.image, &spec.env);
        let host_source = host.to_string_lossy().into_owned();

        let mut targets = vec![
            data_path.clone(),
            paths::HOME_CONTAINER_PATH.into(),
            paths::DATA_CONTAINER_PATH.into(),
        ];

        targets.sort();
        targets.dedup();

        let mounts = targets
            .into_iter()
            .map(|target| Mount {
                target: Some(target),
                source: Some(host_source.clone()),
                typ: Some(MountType::BIND),
                read_only: Some(false),
                ..Default::default()
            })
            .collect::<Vec<_>>();

        let mut labels = HashMap::new();

        labels.insert(
            "agapornis.server_id".into(),
            spec.server_id.clone(),
        );
        labels.insert(
            "agapornis.disk_limit_bytes".into(),
            spec.disk_limit_bytes.to_string(),
        );
        labels.insert(
            "agapornis.cpu_cores".into(),
            spec.cpu_cores.to_string(),
        );
        labels.insert(
            "agapornis.cpu_limit_percentage".into(),
            spec.cpu_limit_percentage.to_string(),
        );
        labels.insert(
            "agapornis.data_path".into(),
            data_path.clone(),
        );
        labels.insert(
            "agapornis.network".into(),
            network.clone(),
        );

        if !spec.network_owner_id.trim().is_empty() {
            labels.insert(
                "agapornis.network_owner_id".into(),
                spec.network_owner_id.clone(),
            );
        }

        let mut exposed_ports = Vec::<String>::new();
        let mut port_bindings =
            HashMap::<String, Option<Vec<PortBinding>>>::new();

        if spec.expose_public_port && !spec.port_mappings.is_empty() {
            for (internal_port, mapped_host_port) in &spec.port_mappings {
                add_port_mapping(
                    &mut exposed_ports,
                    &mut port_bindings,
                    internal_port,
                    Some(*mapped_host_port),
                )?;
            }
        } else {
            let internal_port =
                effective_internal_port(&spec.internal_port, &spec.env)?;

            if let Some(internal_port) = internal_port {
                let mapped_port = spec
                    .expose_public_port
                    .then_some(host_port);

                add_port_mapping(
                    &mut exposed_ports,
                    &mut port_bindings,
                    &internal_port,
                    mapped_port,
                )?;
            }
        }

        let cpus =
            effective_cpus(spec.cpu_limit_percentage, spec.cpu_cores);

        let healthcheck =
            database_health_command(&spec.env).map(|command| HealthConfig {
                test: Some(vec![
                    "CMD-SHELL".into(),
                    command,
                ]),
                interval: Some(10_000_000_000),
                timeout: Some(5_000_000_000),
                start_period: Some(30_000_000_000),
                retries: Some(5),
                ..Default::default()
            });

        let host_config = HostConfig {
            mounts: Some(mounts),
            network_mode: Some(network.clone()),
            pids_limit: Some(512),
            restart_policy: Some(RestartPolicy {
                name: Some(RestartPolicyNameEnum::ON_FAILURE),
                maximum_retry_count: Some(2),
            }),
            security_opt: Some(vec![
                "no-new-privileges".into(),
            ]),
            memory: (spec.memory_bytes > 0)
                .then_some(spec.memory_bytes),
            memory_swap: (spec.memory_bytes > 0)
                .then_some(spec.memory_bytes),
            nano_cpus: (cpus > 0.0)
                .then_some((cpus * 1_000_000_000.0).round() as i64),
            port_bindings: (!port_bindings.is_empty())
                .then_some(port_bindings),
            ..Default::default()
        };

        let mut endpoint_configs = HashMap::new();

        endpoint_configs.insert(
            network.clone(),
            EndpointSettings {
                aliases: Some(vec![spec.server_id.clone()]),
                ..Default::default()
            },
        );

        let networking_config = NetworkingConfig {
            endpoints_config: Some(endpoint_configs),
        };

        let command = if let Some(db_port) = database_port(&spec.env) {
            Some(vec![format!("--port={db_port}")])
        } else if !spec.startup_command.trim().is_empty() {
            Some(vec![
                "/bin/sh".into(),
                "-lc".into(),
                format!("exec {}", spec.startup_command),
            ])
        } else {
            None
        };

        let config = ContainerCreateBody {
            image: Some(spec.image.clone()),
            user: Some("999:999".into()),
            working_dir: Some(data_path),

            // Keep stdin open even when the agent disconnects.
            open_stdin: Some(true),

            // Critical: do not let Docker permanently close stdin after
            // one attach connection disconnects.
            stdin_once: Some(false),

            // TTY can remain enabled because the console connection will use
            // the Engine attach API instead of a piped Docker CLI process.
            tty: Some(true),

            attach_stdin: Some(true),
            attach_stdout: Some(true),
            attach_stderr: Some(true),

            env: (!spec.env.is_empty())
                .then_some(spec.env.clone()),
            cmd: command,
            healthcheck,
            labels: Some(labels),
            exposed_ports: (!exposed_ports.is_empty())
                .then_some(exposed_ports),
            host_config: Some(host_config),
            networking_config: Some(networking_config),
            ..Default::default()
        };

        let options = CreateContainerOptionsBuilder::default()
            .name(&spec.server_id)
            .build();

        let response = self
            .docker
            .create_container(Some(options), config)
            .await
            .with_context(|| {
                format!(
                    "create Docker container {}",
                    spec.server_id
                )
            })?;
        for warning in response.warnings {
            tracing::warn!(
                container_id = %spec.server_id,
                "Docker container creation warning: {warning}"
            );
        }

        self.disk_cache.lock().await.remove(&spec.server_id);

        Ok(host_port)
    }

    pub(super) async fn pull_image(
        &self,
        image: &str,
    ) -> Result<()> {
        let mut builder =
            CreateImageOptionsBuilder::default().from_image(image);

        // The Engine API treats an untagged image differently from the
        // `docker pull IMAGE` CLI command. Explicitly request "latest" when
        // the reference contains neither a tag nor a digest.
        if !image_has_tag_or_digest(image) {
            builder = builder.tag("latest");
        }

        let options = builder.build();

        let mut stream =
            self.docker.create_image(Some(options), None, None);

        while let Some(result) = stream.next().await {
            result.with_context(|| {
                format!("pull Docker image {image}")
            })?;
        }

        Ok(())
    }


    async fn find_port(&self) -> Result<u16> {
        let mut reserved = self.reserved_ports.lock().await;
        let mut rng = rand::rng();

        for _ in 0..50 {
            let port = rng.random_range(25000..26000);

            if !reserved.contains(&port)
                && TcpListener::bind(("0.0.0.0", port)).is_ok()
            {
                reserved.insert(port);
                return Ok(port);
            }
        }

        bail!("No open ports found.")
    }

    async fn run_installer(
        &self,
        spec: &CreateSpec,
        host: &Path,
    ) -> Result<()> {
        self.pull_image(&spec.install_image).await?;

        let name = format!(
            "{}-install-{}",
            spec.server_id,
            Uuid::new_v4().simple()
        );

        let script_path = std::env::temp_dir().join(format!(
            "agapornis-install-{}.sh",
            Uuid::new_v4()
        ));

        fs::write(
            &script_path,
            spec.install_script.replace("\r\n", "\n"),
        )
        .await?;

        let host_source = host.to_string_lossy().into_owned();
        let script_source =
            script_path.to_string_lossy().into_owned();

        let shell_parts = if spec.install_entrypoint.trim().is_empty() {
            vec!["/bin/sh".to_owned()]
        } else {
            spec.install_entrypoint
                .split_whitespace()
                .map(str::to_owned)
                .collect::<Vec<_>>()
        };

        let mut command = shell_parts;
        command.push("/tmp/agapornis-install.sh".into());

        let mut environment = vec![
            "SERVER_DIR=/mnt/server".into(),
        ];
        environment.extend(spec.env.iter().cloned());

        let config = ContainerCreateBody {
            image: Some(spec.install_image.clone()),
            working_dir: Some("/mnt/server".into()),

            // A one-element empty entrypoint resets the image entrypoint.
            entrypoint: Some(vec![String::new()]),

            cmd: Some(command),
            env: Some(environment),

            host_config: Some(HostConfig {
                mounts: Some(vec![
                    Mount {
                        target: Some("/mnt/server".into()),
                        source: Some(host_source),
                        typ: Some(MountType::BIND),
                        read_only: Some(false),
                        ..Default::default()
                    },
                    Mount {
                        target: Some(
                            "/tmp/agapornis-install.sh".into(),
                        ),
                        source: Some(script_source),
                        typ: Some(MountType::BIND),
                        read_only: Some(true),
                        ..Default::default()
                    },
                ]),
                ..Default::default()
            }),

            attach_stdout: Some(true),
            attach_stderr: Some(true),
            ..Default::default()
        };

        let create_options =
            CreateContainerOptionsBuilder::default()
                .name(&name)
                .build();

        let result = async {
            self.docker
                .create_container(Some(create_options), config)
                .await
                .context("create installer container")?;

            // Attach before starting so no installer output is missed.
            let attach_options =
                AttachContainerOptionsBuilder::default()
                    .stdin(false)
                    .stdout(true)
                    .stderr(true)
                    .stream(true)
                    .logs(true)
                    .build();

            let AttachContainerResults {
                mut output,
                input: _input,
            } = self
                .docker
                .attach_container(&name, Some(attach_options))
                .await
                .context("attach to installer container")?;

            self.docker
                .start_container(&name, None)
                .await
                .context("start installer container")?;

            let mut log = Vec::new();

            while let Some(item) = output.next().await {
                let item =
                    item.context("read installer output")?;

                log.extend_from_slice(&item.into_bytes());
            }

            let wait_options =
                WaitContainerOptionsBuilder::default()
                    .condition("not-running")
                    .build();

            let wait_result = self
                .docker
                .wait_container(&name, Some(wait_options))
                .next()
                .await
                .context("installer wait stream ended unexpectedly")?
                .context("wait for installer container")?;

            if !log.is_empty() {
                fs::write(
                    host.join(".agapornis-install.log"),
                    &log,
                )
                .await?;
            }

            if wait_result.status_code != 0 {
                let output = String::from_utf8_lossy(&log);

                bail!(
                    "installer container exited with status {}: {}",
                    wait_result.status_code,
                    output.trim()
                );
            }

            Ok(())
        }
        .await;

        let remove_options =
            RemoveContainerOptionsBuilder::default()
                .force(true)
                .v(true)
                .build();

        if let Err(err) = self
            .docker
            .remove_container(&name, Some(remove_options))
            .await
        {
            tracing::warn!(
                container = %name,
                "failed to remove installer container: {err}"
            );
        }

        if let Err(err) = fs::remove_file(&script_path).await
            && err.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(
                path = %script_path.display(),
                "failed to remove installer script: {err}"
            );
        }

        result
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

fn normalize_container_port(port: &str) -> Result<String> {
    let port = port.trim();

    if port.is_empty() {
        bail!("container port cannot be empty");
    }

    let (number, protocol) = match port.rsplit_once('/') {
        Some((number, protocol)) => (number, protocol),
        None => (port, "tcp"),
    };

    let number = number
        .parse::<u16>()
        .with_context(|| format!("invalid container port: {port}"))?;

    let protocol = protocol.to_ascii_lowercase();

    if !matches!(protocol.as_str(), "tcp" | "udp" | "sctp") {
        bail!("unsupported port protocol: {protocol}");
    }

    Ok(format!("{number}/{protocol}"))
}

fn add_port_mapping(
    exposed_ports: &mut Vec<String>,
    port_bindings: &mut HashMap<
        String,
        Option<Vec<PortBinding>>,
    >,
    internal_port: &str,
    host_port: Option<i32>,
) -> Result<()> {
    let port_key = normalize_container_port(internal_port)?;

    if !exposed_ports.contains(&port_key) {
        exposed_ports.push(port_key.clone());
    }

    let Some(host_port) = host_port else {
        return Ok(());
    };

    let host_port = u16::try_from(host_port)
        .context("mapped host port is outside the valid range")?;

    ensure_port(host_port)?;

    port_bindings
        .entry(port_key)
        .or_insert_with(|| Some(Vec::new()))
        .get_or_insert_with(Vec::new)
        .push(PortBinding {
            host_ip: Some("0.0.0.0".into()),
            host_port: Some(host_port.to_string()),
        });

    Ok(())
}