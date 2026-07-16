use super::*;

use bollard::{
    models::{ContainerCreateBody, EndpointSettings, MountType, NetworkingConfig, PortBinding},
    query_parameters::{
        CreateContainerOptionsBuilder, RemoveContainerOptionsBuilder, RenameContainerOptionsBuilder,
    },
};

impl DockerManager {
    pub async fn update_configuration(
        &self,
        id: &str,
        env: Vec<String>,
        startup_command: &str,
        stop_command: &str,
        startup_done: &str,
        config_files_json: &str,
    ) -> Result<()> {
        paths::validate_id(id)?;
        let host = paths::server_dir(id)?;
        validate_startup(&host, startup_command)?;

        let inspect = self
            .docker
            .inspect_container(id, None)
            .await
            .with_context(|| format!("inspect Docker container {id}"))?;
        let mut config = inspect
            .config
            .context("Docker container has no reusable configuration")?;
        let labels = config
            .labels
            .as_mut()
            .context("refusing to update configuration without Agapornis ownership labels")?;
        if labels.get("agapornis.server_id").map(String::as_str) != Some(id) {
            bail!(
                "refusing to update configuration on a container not owned by this Agapornis server"
            );
        }

        let desired_command = (!startup_command.trim().is_empty()).then(|| {
            vec![
                "/bin/sh".into(),
                "-lc".into(),
                format!("exec {startup_command}"),
            ]
        });
        let merged_env = merged_environment(config.env.as_deref(), env);
        let configuration_changed = normalized_environment(config.env.as_deref())
            != normalized_environment(Some(&merged_env))
            || config.cmd != desired_command
            || labels
                .get("agapornis.stop_command")
                .map(String::as_str)
                .unwrap_or("")
                != stop_command
            || labels
                .get("agapornis.startup_done")
                .map(String::as_str)
                .unwrap_or("")
                != startup_done;

        let metadata = paths::config_files_path(id)?;
        if let Some(parent) = metadata.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::write(&metadata, config_files_json).await?;
        if !configuration_changed {
            return Ok(());
        }

        let was_running = inspect
            .state
            .as_ref()
            .and_then(|state| state.running)
            .unwrap_or(false);
        config.env = (!merged_env.is_empty()).then_some(merged_env);
        config.cmd = desired_command;
        labels.insert("agapornis.stop_command".into(), stop_command.into());
        labels.insert("agapornis.startup_done".into(), startup_done.into());
        let host_config = inspect
            .host_config
            .context("Docker container has no reusable host configuration")?;
        let networking_config = reusable_networks(inspect.network_settings, id);

        if was_running {
            self.stop(id).await?;
        }
        self.replace_stale_container(id, create_body(config, host_config, networking_config)?)
            .await?;
        self.forget_runtime_state(id).await;
        if was_running {
            self.start(id).await?;
        }
        Ok(())
    }

    pub async fn update_ports(&self, id: &str, mappings: Vec<(String, i32)>) -> Result<()> {
        paths::validate_id(id)?;
        let (exposed_ports, port_bindings) = validated_port_bindings(mappings)?;
        let inspect = self
            .docker
            .inspect_container(id, None)
            .await
            .with_context(|| format!("inspect Docker container {id}"))?;
        let mut config = inspect
            .config
            .context("Docker container has no reusable configuration")?;
        let labels = config
            .labels
            .as_ref()
            .context("refusing to update ports without Agapornis ownership labels")?;
        if labels.get("agapornis.server_id").map(String::as_str) != Some(id) {
            bail!("refusing to update ports on a container not owned by this Agapornis server");
        }
        let was_running = inspect
            .state
            .as_ref()
            .and_then(|state| state.running)
            .unwrap_or(false);
        let mut host_config = inspect
            .host_config
            .context("Docker container has no reusable host configuration")?;
        let current_host_ports = bound_host_ports(host_config.port_bindings.as_ref());
        self.ensure_requested_ports_available(&current_host_ports, &port_bindings)
            .await?;
        config.exposed_ports = Some(exposed_ports);
        host_config.port_bindings = Some(port_bindings);
        let networking_config = reusable_networks(inspect.network_settings, id);

        if was_running {
            self.stop(id).await?;
        }
        self.replace_stale_container(id, create_body(config, host_config, networking_config)?)
            .await?;
        self.forget_runtime_state(id).await;
        if was_running {
            self.start(id).await?;
        }
        Ok(())
    }

    async fn ensure_requested_ports_available(
        &self,
        current_host_ports: &HashSet<u16>,
        requested: &HashMap<String, Option<Vec<PortBinding>>>,
    ) -> Result<()> {
        let reserved = self.reserved_ports.lock().await;
        for bindings in requested.values().flatten() {
            for binding in bindings {
                let port = binding
                    .host_port
                    .as_deref()
                    .context("requested host port is missing")?
                    .parse::<u16>()
                    .context("requested host port is invalid")?;
                if current_host_ports.contains(&port) {
                    continue;
                }
                if reserved.contains(&port)
                    || tokio::net::TcpListener::bind(("0.0.0.0", port))
                        .await
                        .is_err()
                    || tokio::net::UdpSocket::bind(("0.0.0.0", port))
                        .await
                        .is_err()
                {
                    bail!("requested host port {port} is already in use");
                }
            }
        }
        Ok(())
    }

    pub(super) async fn recreate_with_fresh_bind_mounts(&self, id: &str) -> Result<()> {
        let inspect = self
            .docker
            .inspect_container(id, None)
            .await
            .with_context(|| format!("inspect stale Docker container {id}"))?;
        let config = inspect
            .config
            .context("stale Docker container has no reusable configuration")?;
        let labels = config
            .labels
            .as_ref()
            .context("refusing to recreate a container without Agapornis ownership labels")?;
        if labels.get("agapornis.server_id").map(String::as_str) != Some(id) {
            bail!("refusing to recreate a container not owned by this Agapornis server");
        }

        let data_path = labels
            .get("agapornis.data_path")
            .map(String::as_str)
            .unwrap_or(paths::HOME_CONTAINER_PATH);
        let managed_targets = [
            data_path,
            paths::HOME_CONTAINER_PATH,
            paths::DATA_CONTAINER_PATH,
        ];
        let host_source = paths::server_dir(id)?.to_string_lossy().into_owned();
        let mut host_config = inspect
            .host_config
            .context("stale Docker container has no reusable host configuration")?;
        let repaired_mounts =
            repair_managed_mounts(&mut host_config, &managed_targets, &host_source);
        if repaired_mounts == 0 {
            bail!("stale Docker container has no managed bind mounts to repair");
        }

        let networking_config = reusable_networks(inspect.network_settings, id);
        let create_body = create_body(config, host_config, networking_config)?;
        self.replace_stale_container(id, create_body).await
    }

    async fn replace_stale_container(
        &self,
        id: &str,
        create_body: ContainerCreateBody,
    ) -> Result<()> {
        let stale_name = format!("{id}-stale-{}", Uuid::new_v4().simple());
        self.docker
            .rename_container(
                id,
                RenameContainerOptionsBuilder::default()
                    .name(&stale_name)
                    .build(),
            )
            .await
            .with_context(|| format!("rename stale Docker container {id}"))?;

        let create_options = CreateContainerOptionsBuilder::default().name(id).build();
        if let Err(create_error) = self
            .docker
            .create_container(Some(create_options), create_body)
            .await
        {
            let rollback = self
                .docker
                .rename_container(
                    &stale_name,
                    RenameContainerOptionsBuilder::default().name(id).build(),
                )
                .await;
            if let Err(rollback_error) = rollback {
                return Err(create_error).context(format!(
                    "recreate Docker container {id}; restoring its old name also failed: {rollback_error}"
                ));
            }
            return Err(create_error).with_context(|| format!("recreate Docker container {id}"));
        }

        let remove_options = RemoveContainerOptionsBuilder::default()
            .force(true)
            .v(true)
            .build();
        if let Err(error) = self
            .docker
            .remove_container(&stale_name, Some(remove_options))
            .await
        {
            tracing::warn!(
                container_id = %stale_name,
                "repaired container is ready, but the stale container could not be removed: {error}"
            );
        }
        Ok(())
    }
}

fn normalized_environment(values: Option<&[String]>) -> Vec<&str> {
    let mut normalized = values
        .unwrap_or_default()
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    normalized.sort_unstable();
    normalized
}

fn merged_environment(current: Option<&[String]>, desired: Vec<String>) -> Vec<String> {
    let desired_keys = desired
        .iter()
        .filter_map(|entry| entry.split_once('=').map(|(key, _)| key.to_owned()))
        .collect::<HashSet<_>>();
    current
        .unwrap_or_default()
        .iter()
        .filter(|entry| {
            entry
                .split_once('=')
                .is_none_or(|(key, _)| !desired_keys.contains(key))
        })
        .cloned()
        .chain(desired)
        .collect()
}

fn bound_host_ports(bindings: Option<&HashMap<String, Option<Vec<PortBinding>>>>) -> HashSet<u16> {
    bindings
        .into_iter()
        .flat_map(HashMap::values)
        .flatten()
        .flatten()
        .filter_map(|binding| binding.host_port.as_deref()?.parse().ok())
        .collect()
}

fn validated_port_bindings(
    mappings: Vec<(String, i32)>,
) -> Result<(Vec<String>, HashMap<String, Option<Vec<PortBinding>>>)> {
    if mappings.is_empty() || mappings.len() > 32 {
        bail!("a server must have between 1 and 32 port mappings");
    }
    let mut exposed = Vec::with_capacity(mappings.len());
    let mut bindings = HashMap::new();
    let mut host_ports = HashSet::new();
    for (internal, host_port) in mappings {
        if !(1..=65535).contains(&host_port) || !host_ports.insert(host_port) {
            bail!("host ports must be unique numbers between 1 and 65535");
        }
        let normalized = if internal.contains('/') {
            internal
        } else {
            format!("{internal}/tcp")
        };
        let (port, protocol) = normalized
            .split_once('/')
            .context("internal port must use the form port/protocol")?;
        let port = port
            .parse::<u16>()
            .context("internal port is not a valid number")?;
        if port == 0 || !matches!(protocol, "tcp" | "udp") {
            bail!("internal ports must be between 1 and 65535 and use tcp or udp");
        }
        let key = format!("{port}/{protocol}");
        if bindings.contains_key(&key) {
            bail!("internal port mappings must be unique");
        }
        exposed.push(key.clone());
        bindings.insert(
            key,
            Some(vec![PortBinding {
                host_ip: Some("0.0.0.0".into()),
                host_port: Some(host_port.to_string()),
            }]),
        );
    }
    Ok((exposed, bindings))
}

fn repair_managed_mounts(
    host_config: &mut bollard::models::HostConfig,
    managed_targets: &[&str],
    host_source: &str,
) -> usize {
    let mut repaired = 0;
    for mount in host_config.mounts.get_or_insert_default() {
        if mount.typ == Some(MountType::BIND)
            && mount
                .target
                .as_deref()
                .is_some_and(|target| managed_targets.contains(&target))
        {
            mount.source = Some(host_source.to_owned());
            repaired += 1;
        }
    }
    repaired
}

fn reusable_networks(
    settings: Option<bollard::models::NetworkSettings>,
    id: &str,
) -> NetworkingConfig {
    let endpoints_config = settings
        .and_then(|settings| settings.networks)
        .map(|networks| {
            networks
                .into_keys()
                .map(|network| {
                    (
                        network,
                        EndpointSettings {
                            aliases: Some(vec![id.to_owned()]),
                            ..Default::default()
                        },
                    )
                })
                .collect()
        });
    NetworkingConfig { endpoints_config }
}

fn create_body(
    config: bollard::models::ContainerConfig,
    host_config: bollard::models::HostConfig,
    networking_config: NetworkingConfig,
) -> Result<ContainerCreateBody> {
    let mut value =
        serde_json::to_value(config).context("serialize stale container configuration")?;
    let object = value
        .as_object_mut()
        .context("stale container configuration was not an object")?;
    object.insert(
        "HostConfig".into(),
        serde_json::to_value(host_config).context("serialize repaired host configuration")?,
    );
    object.insert(
        "NetworkingConfig".into(),
        serde_json::to_value(networking_config)
            .context("serialize repaired network configuration")?,
    );
    serde_json::from_value(value).context("build repaired container configuration")
}

#[cfg(test)]
mod tests {
    use super::{merged_environment, validated_port_bindings};

    #[test]
    fn runtime_environment_updates_managed_values_and_keeps_image_defaults() {
        let current = vec!["PATH=/usr/bin".into(), "MAX_PLAYERS=8".into()];
        let merged = merged_environment(
            Some(&current),
            vec!["MAX_PLAYERS=16".into(), "SERVER_PORT=8766".into()],
        );
        assert_eq!(
            merged,
            ["PATH=/usr/bin", "MAX_PLAYERS=16", "SERVER_PORT=8766"]
        );
    }

    #[test]
    fn validates_multiple_port_bindings() {
        let (exposed, bindings) = validated_port_bindings(vec![
            ("25565/tcp".into(), 30000),
            ("19132/udp".into(), 30001),
        ])
        .expect("valid mappings");
        assert_eq!(exposed, vec!["25565/tcp", "19132/udp"]);
        assert_eq!(bindings.len(), 2);
    }

    #[test]
    fn rejects_duplicate_host_ports() {
        assert!(
            validated_port_bindings(vec![
                ("25565/tcp".into(), 30000),
                ("19132/udp".into(), 30000),
            ])
            .is_err()
        );
    }
}
