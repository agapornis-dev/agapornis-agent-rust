use super::*;

use bollard::{
    models::{ContainerCreateBody, EndpointSettings, MountType, NetworkingConfig},
    query_parameters::{
        CreateContainerOptionsBuilder, RemoveContainerOptionsBuilder, RenameContainerOptionsBuilder,
    },
};

impl DockerManager {
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
