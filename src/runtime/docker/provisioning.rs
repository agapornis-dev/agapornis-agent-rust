use super::*;

impl DockerManager {
    pub fn new(protection: Arc<ProtectionState>) -> Self {
        Self {
            protection,
            disk_cache: Arc::new(Mutex::new(HashMap::new())),
            console_bindings: Arc::new(Mutex::new(HashMap::new())),
            reserved_ports: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub async fn create(&self, spec: CreateSpec) -> Result<i32> {
        paths::validate_id(&spec.server_id)?;
        if spec.image.trim().is_empty() {
            bail!("Docker image is required")
        }
        let host_port = if spec.expose_public_port {
            if spec.host_port > 0 {
                ensure_port(spec.host_port as u16)?;
                self.reserved_ports
                    .lock()
                    .await
                    .insert(spec.host_port as u16);
                spec.host_port
            } else {
                self.find_port().await? as i32
            }
        } else {
            0
        };
        let host = paths::server_dir(&spec.server_id)?;
        fs::create_dir_all(&host).await?;
        if spec.disk_limit_bytes > 0 {
            let metadata = paths::disk_limit_path(&spec.server_id)?;
            fs::create_dir_all(metadata.parent().unwrap()).await?;
            fs::write(metadata, spec.disk_limit_bytes.to_string()).await?;
        }
        if !spec.install_image.trim().is_empty() && !spec.install_script.trim().is_empty() {
            self.run_installer(&spec, &host).await?;
        }
        apply_config_files(&host, &spec.config_files_json).await?;
        validate_startup(&host, &spec.startup_command)?;
        process::docker(["pull", spec.image.as_str()]).await?;
        #[cfg(unix)]
        {
            let _ = process::run("chown", ["-R", "999:999", host.to_string_lossy().as_ref()]).await;
        }
        let network =
            std::env::var("AGAPORNIS_DOCKER_NETWORK").unwrap_or_else(|_| "agapornis_ntw".into());
        ensure_network(&network).await?;
        let data_path = paths::data_path(&spec.image, &spec.env);
        let bind = format!("{}:{data_path}", host.display());
        let mut args = vec![
            "create".into(),
            "--interactive".into(),
            "--tty".into(), // FIX: Enables TTY multiplexing for ordered console logs
            "--attach".into(),
            "stdin".into(),
            "--attach".into(),
            "stdout".into(),
            "--attach".into(),
            "stderr".into(),
            "--name".into(),
            spec.server_id.clone(),
            "--network".into(),
            network.clone(),
            "--network-alias".into(),
            spec.server_id.clone(),
            "--user".into(),
            "999:999".into(),
            "--workdir".into(),
            data_path.clone(),
            "--pids-limit".into(),
            "512".into(),
            "--restart".into(),
            "on-failure:2".into(),
            "--security-opt".into(),
            "no-new-privileges".into(),
            "--label".into(),
            format!("agapornis.server_id={}", spec.server_id),
            "--label".into(),
            format!("agapornis.disk_limit_bytes={}", spec.disk_limit_bytes),
            "--label".into(),
            format!("agapornis.cpu_cores={}", spec.cpu_cores),
            "--label".into(),
            format!(
                "agapornis.cpu_limit_percentage={}",
                spec.cpu_limit_percentage
            ),
            "--label".into(),
            format!("agapornis.data_path={data_path}"),
            "--label".into(),
            format!("agapornis.network={network}"),
        ];
        let _ = bind;
        let mut targets = vec![
            data_path.clone(),
            paths::HOME_CONTAINER_PATH.into(),
            paths::DATA_CONTAINER_PATH.into(),
        ];
        targets.sort();
        targets.dedup();
        for target in targets {
            args.extend([
                "--mount".into(),
                format!("type=bind,src={},dst={target}", host.display()),
            ]);
        }
        if spec.memory_bytes > 0 {
            args.extend([
                "--memory".into(),
                spec.memory_bytes.to_string(),
                "--memory-swap".into(),
                spec.memory_bytes.to_string(),
            ]);
        }
        let cpus = effective_cpus(spec.cpu_limit_percentage, spec.cpu_cores);
        if cpus > 0.0 {
            args.extend(["--cpus".into(), cpus.to_string()]);
        }
        if !spec.network_owner_id.trim().is_empty() {
            args.extend([
                "--label".into(),
                format!("agapornis.network_owner_id={}", spec.network_owner_id),
            ]);
        }
        if spec.expose_public_port && !spec.port_mappings.is_empty() {
            for (internal_port, mapped_host_port) in &spec.port_mappings {
                ensure_port(*mapped_host_port as u16)?;
                args.extend(port_arguments(internal_port, true, *mapped_host_port));
            }
        } else {
            let internal_port = effective_internal_port(&spec.internal_port, &spec.env)?;
            if let Some(internal_port) = internal_port {
                args.extend(port_arguments(&internal_port, spec.expose_public_port, host_port));
            }
        }
        if let Some(health_command) = database_health_command(&spec.env) {
            args.extend([
                "--health-cmd".into(),
                health_command,
                "--health-interval".into(),
                "10s".into(),
                "--health-timeout".into(),
                "5s".into(),
                "--health-start-period".into(),
                "30s".into(),
                "--health-retries".into(),
                "5".into(),
            ]);
        }
        for item in &spec.env {
            args.extend(["--env".into(), item.clone()]);
        }
        args.push(spec.image.clone());
        if let Some(db_port) = database_port(&spec.env) {
            args.push(format!("--port={db_port}"));
        } else if !spec.startup_command.trim().is_empty() {
            args.extend([
                "/bin/sh".into(),
                "-lc".into(),
                format!("exec {}", spec.startup_command),
            ]);
        }
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        process::docker(refs).await?;
        self.disk_cache.lock().await.remove(&spec.server_id);
        Ok(host_port)
    }

    async fn find_port(&self) -> Result<u16> {
        let mut reserved = self.reserved_ports.lock().await;
        let mut rng = rand::rng();

        for _ in 0..50 {
            let p = rng.random_range(25000..26000);
            if !reserved.contains(&p) && TcpListener::bind(("0.0.0.0", p)).is_ok() {
                reserved.insert(p);
                return Ok(p);
            }
        }
        bail!("No open ports found.")
    }

    async fn run_installer(&self, spec: &CreateSpec, host: &Path) -> Result<()> {
        process::docker(["pull", spec.install_image.as_str()]).await?;
        let name = format!("{}-install-{}", spec.server_id, Uuid::new_v4().simple());
        let script_path =
            std::env::temp_dir().join(format!("agapornis-install-{}.sh", Uuid::new_v4()));
        fs::write(&script_path, spec.install_script.replace("\r\n", "\n")).await?;
        let shell_parts: Vec<&str> = if spec.install_entrypoint.trim().is_empty() {
            vec!["/bin/sh"]
        } else {
            spec.install_entrypoint.split_whitespace().collect()
        };
        let mut args = vec![
            "create".to_owned(),
            "--name".to_owned(),
            name.clone(),
            "--workdir".to_owned(),
            "/mnt/server".to_owned(),
            "--mount".to_owned(),
            format!("type=bind,src={},dst=/mnt/server", host.display()),
            "--mount".to_owned(),
            format!(
                "type=bind,src={},dst=/tmp/agapornis-install.sh,readonly",
                script_path.display()
            ),
            "--entrypoint".to_owned(),
            "".to_owned(),
            "--env".to_owned(),
            "SERVER_DIR=/mnt/server".to_owned(),
        ];
        for item in &spec.env {
            args.extend(["--env".into(), item.clone()]);
        }
        args.push(spec.install_image.clone());
        args.extend(shell_parts.into_iter().map(str::to_owned));
        args.push("/tmp/agapornis-install.sh".into());
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        let result = async {
            process::docker(refs).await?;
            let output = process::docker(["start", "--attach", name.as_str()]).await;
            if let Ok(ref text) = output
                && !text.trim().is_empty()
            {
                fs::write(host.join(".agapornis-install.log"), text).await?;
            }
            output.map(|_| ())
        }
        .await;
        let _ = process::docker(["rm", "--force", name.as_str()]).await;
        let _ = fs::remove_file(script_path).await;
        result
    }
}
