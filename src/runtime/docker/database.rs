use super::*;

use bollard::{
    errors::Error as BollardError,
    models::{ContainerCreateBody, HostConfig},
    query_parameters::{
        CreateContainerOptionsBuilder,
        LogsOptionsBuilder,
        RemoveContainerOptionsBuilder,
        WaitContainerOptionsBuilder,
    },
};
use futures_util::StreamExt;

impl DockerManager {
    pub async fn test_database_connection(
        &self,
        spec: DatabaseConnectionSpec<'_>,
    ) -> Result<i64> {
        let DatabaseConnectionSpec {
            server_id,
            database_type,
            host,
            port,
            database_name,
            username,
            password,
            docker_image,
        } = spec;

        paths::validate_id(server_id)?;
        paths::validate_id(host)?;

        if !(1..=65535).contains(&port) {
            bail!("database port is invalid");
        }

        if database_name.trim().is_empty()
            || username.trim().is_empty()
            || password.is_empty()
        {
            bail!("database credentials are incomplete");
        }

        if docker_image.trim().is_empty() {
            bail!("database test image is required");
        }

        let source = self.inspect(server_id).await?;

        if !source
            .pointer("/State/Running")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            bail!(
                "server container must be running to test its \
                 database connection"
            );
        }

        let database = self.inspect(host).await?;

        if !database
            .pointer("/State/Running")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            bail!("database container is not running");
        }

        let port = port.to_string();

        let (password_key, entrypoint, command) = match database_type {
            "mysql" => (
                "MYSQL_PWD",
                "mysql",
                vec![
                    "--protocol=TCP".into(),
                    "--host".into(),
                    host.into(),
                    "--port".into(),
                    port.clone(),
                    "--user".into(),
                    username.into(),
                    database_name.into(),
                    "--execute".into(),
                    "SELECT 1".into(),
                ],
            ),

            "mariadb" => (
                "MYSQL_PWD",
                "mariadb",
                vec![
                    "--protocol=TCP".into(),
                    "--host".into(),
                    host.into(),
                    "--port".into(),
                    port,
                    "--user".into(),
                    username.into(),
                    database_name.into(),
                    "--execute".into(),
                    "SELECT 1".into(),
                ],
            ),

            "postgres" => (
                "PGPASSWORD",
                "psql",
                vec![
                    "--host".into(),
                    host.into(),
                    "--port".into(),
                    port,
                    "--username".into(),
                    username.into(),
                    "--dbname".into(),
                    database_name.into(),
                    "--command".into(),
                    "SELECT 1".into(),
                ],
            ),

            _ => bail!("unsupported database type"),
        };

        let probe_name = format!(
            "{}-dbtest-{}",
            server_id,
            Uuid::new_v4().simple()
        );

        let started = Instant::now();

        let probe_result = tokio::time::timeout(
            Duration::from_secs(20),
            async {
                /*
                 * `docker run` implicitly pulled a missing image. Since the
                 * Engine create API does not do that, pull it explicitly.
                 */
                self.pull_image(docker_image).await?;

                let config = ContainerCreateBody {
                    image: Some(docker_image.to_owned()),

                    /*
                     * Pass the actual value. The old CLI invocation used:
                     *
                     *     --env MYSQL_PWD
                     *
                     * while setting MYSQL_PWD in the Docker CLI process'
                     * environment. The Engine API needs KEY=VALUE directly.
                     */
                    env: Some(vec![
                        format!("{password_key}={password}"),
                    ]),

                    entrypoint: Some(vec![
                        entrypoint.to_owned(),
                    ]),

                    cmd: Some(command),

                    attach_stdout: Some(true),
                    attach_stderr: Some(true),

                    tty: Some(false),

                    host_config: Some(HostConfig {
                        /*
                         * Equivalent to:
                         *
                         *     --network container:<server_id>
                         *
                         * The probe shares the server container's network
                         * namespace and therefore tests from the same network
                         * context as the server.
                         */
                        network_mode: Some(format!(
                            "container:{server_id}"
                        )),

                        /*
                         * Do not auto-remove because failure logs must remain
                         * available after the process exits. Cleanup happens
                         * explicitly below.
                         */
                        auto_remove: Some(false),

                        ..Default::default()
                    }),

                    ..Default::default()
                };

                let options =
                    CreateContainerOptionsBuilder::default()
                        .name(&probe_name)
                        .build();

                self.docker
                    .create_container(Some(options), config)
                    .await
                    .context(
                        "create database connection test container",
                    )?;

                self.docker
                    .start_container(&probe_name, None)
                    .await
                    .context(
                        "start database connection test container",
                    )?;

                let wait_options =
                    WaitContainerOptionsBuilder::default()
                        .condition("not-running")
                        .build();

                let wait = self
                    .docker
                    .wait_container(
                        &probe_name,
                        Some(wait_options),
                    )
                    .next()
                    .await
                    .context(
                        "database test wait stream ended unexpectedly",
                    )?
                    .context(
                        "wait for database test container",
                    )?;

                if wait.status_code != 0 {
                    let logs = self
                        .read_database_probe_logs(&probe_name)
                        .await;

                    let detail = if !logs.trim().is_empty() {
                        logs.trim().to_owned()
                    } else if let Some(error) = wait.error {
                        format!("{error:?}")
                    } else {
                        "database client returned no output".into()
                    };

                    bail!(
                        "database connection test exited with status {}: {}",
                        wait.status_code,
                        detail
                    );
                }

                Ok(())
            },
        )
        .await;

        /*
         * Always remove the probe, including after timeout or cancellation.
         * The future inside timeout is dropped when the deadline expires, but
         * Docker containers continue running unless explicitly removed.
         */
        let cleanup_result = self
            .remove_database_probe(&probe_name)
            .await;

        match probe_result {
            Err(_) => {
                if let Err(error) = cleanup_result {
                    tracing::warn!(
                        container = %probe_name,
                        "failed to remove timed-out database probe: {error}"
                    );
                }

                bail!("database connection test timed out");
            }

            Ok(Err(error)) => {
                if let Err(cleanup_error) = cleanup_result {
                    tracing::warn!(
                        container = %probe_name,
                        "failed to remove database probe after error: \
                         {cleanup_error}"
                    );
                }

                Err(error)
            }

            Ok(Ok(())) => {
                cleanup_result.context(
                    "remove successful database test container",
                )?;

                Ok(
                    started
                        .elapsed()
                        .as_millis()
                        .min(i64::MAX as u128)
                        as i64,
                )
            }
        }
    }

    async fn read_database_probe_logs(
        &self,
        container: &str,
    ) -> String {
        const MAX_LOG_BYTES: usize = 32 * 1024;

        let options = LogsOptionsBuilder::default()
            .stdout(true)
            .stderr(true)
            .follow(false)
            .build();

        let mut stream =
            self.docker.logs(container, Some(options));

        let mut output = Vec::new();

        while let Some(item) = stream.next().await {
            let item = match item {
                Ok(item) => item,

                Err(error) => {
                    tracing::debug!(
                        container,
                        "failed reading database probe logs: {error}"
                    );
                    break;
                }
            };

            let bytes = item.into_bytes();

            let remaining =
                MAX_LOG_BYTES.saturating_sub(output.len());

            if remaining == 0 {
                break;
            }

            output.extend_from_slice(
                &bytes[..bytes.len().min(remaining)],
            );
        }

        String::from_utf8_lossy(&output).into_owned()
    }

    async fn remove_database_probe(
        &self,
        container: &str,
    ) -> Result<()> {
        let options =
            RemoveContainerOptionsBuilder::default()
                .force(true)
                .v(true)
                .build();

        match self
            .docker
            .remove_container(container, Some(options))
            .await
        {
            Ok(()) => Ok(()),

            /*
             * The timeout may occur before create_container completes, or the
             * container may already have disappeared.
             */
            Err(BollardError::DockerResponseServerError {
                status_code: 404,
                ..
            }) => Ok(()),

            Err(error) => Err(error).with_context(|| {
                format!(
                    "remove database probe container {container}"
                )
            }),
        }
    }
}

pub(super) fn database_port(
    env: &[String],
) -> Option<u16> {
    env.iter().find_map(|entry| {
        entry
            .strip_prefix("AGAPORNIS_DATABASE_PORT=")?
            .parse()
            .ok()
    })
}

pub(super) fn database_health_command(
    env: &[String],
) -> Option<String> {
    let has = |prefix: &str| {
        env.iter()
            .any(|value| value.starts_with(prefix))
    };

    if has("POSTGRES_PASSWORD=") {
        return Some(
            "pg_isready \
             --host=127.0.0.1 \
             --port=\"$AGAPORNIS_DATABASE_PORT\" \
             --username=\"$POSTGRES_USER\" \
             --dbname=\"$POSTGRES_DB\""
                .into(),
        );
    }

    if has("MYSQL_ROOT_PASSWORD=") {
        return Some(
            "mysqladmin ping \
             --protocol=tcp \
             --host=127.0.0.1 \
             --port=\"$AGAPORNIS_DATABASE_PORT\" \
             --user=root \
             --password=\"$MYSQL_ROOT_PASSWORD\" \
             --silent"
                .into(),
        );
    }

    if has("MARIADB_ROOT_PASSWORD=") {
        return Some(
            "mariadb-admin ping \
             --protocol=tcp \
             --host=127.0.0.1 \
             --port=\"$AGAPORNIS_DATABASE_PORT\" \
             --user=root \
             --password=\"$MARIADB_ROOT_PASSWORD\" \
             --silent"
                .into(),
        );
    }

    None
}

pub(super) fn effective_internal_port(
    internal_port: &str,
    env: &[String],
) -> Result<Option<String>> {
    if let Some(port) = database_port(env) {
        return Ok(Some(format!("{port}/tcp")));
    }

    let value = internal_port.trim();

    if value.is_empty() {
        return Ok(None);
    }

    let (raw_port, protocol) =
        value.split_once('/').unwrap_or((value, "tcp"));

    let port: u16 = raw_port.parse().with_context(|| {
        format!("Invalid internal port '{internal_port}'.")
    })?;

    if !matches!(protocol, "tcp" | "udp" | "sctp") {
        bail!(
            "Invalid internal port protocol '{protocol}'."
        );
    }

    Ok(Some(format!("{port}/{protocol}")))
}