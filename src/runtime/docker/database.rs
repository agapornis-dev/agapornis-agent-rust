use super::*;

use bollard::{
    errors::Error as BollardError,
    models::{ContainerCreateBody, HostConfig},
    query_parameters::{
        CreateContainerOptionsBuilder, LogsOptionsBuilder, RemoveContainerOptionsBuilder,
        WaitContainerOptionsBuilder,
    },
};
use futures_util::StreamExt;

struct DatabaseProbe<'a> {
    server_id: &'a str,
    password: &'a str,
    docker_image: &'a str,
    password_key: &'static str,
    entrypoint: &'static str,
    command: Vec<String>,
}

impl<'a> DatabaseProbe<'a> {
    fn from_spec(spec: DatabaseConnectionSpec<'a>) -> Result<Self> {
        let port = spec.port.to_string();
        let (password_key, entrypoint, command) = match spec.database_type {
            "mysql" => (
                "MYSQL_PWD",
                "mysql",
                vec![
                    "--protocol=TCP".into(),
                    "--host".into(),
                    spec.host.into(),
                    "--port".into(),
                    port,
                    "--user".into(),
                    spec.username.into(),
                    spec.database_name.into(),
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
                    spec.host.into(),
                    "--port".into(),
                    port,
                    "--user".into(),
                    spec.username.into(),
                    spec.database_name.into(),
                    "--execute".into(),
                    "SELECT 1".into(),
                ],
            ),
            "postgres" => (
                "PGPASSWORD",
                "psql",
                vec![
                    "--host".into(),
                    spec.host.into(),
                    "--port".into(),
                    port,
                    "--username".into(),
                    spec.username.into(),
                    "--dbname".into(),
                    spec.database_name.into(),
                    "--command".into(),
                    "SELECT 1".into(),
                ],
            ),
            _ => bail!("unsupported database type"),
        };
        Ok(Self {
            server_id: spec.server_id,
            password: spec.password,
            docker_image: spec.docker_image,
            password_key,
            entrypoint,
            command,
        })
    }
}

fn validate_database_spec(spec: &DatabaseConnectionSpec<'_>) -> Result<()> {
    paths::validate_id(spec.server_id)?;
    paths::validate_id(spec.host)?;
    if !(1..=65535).contains(&spec.port) {
        bail!("database port is invalid");
    }
    if spec.database_name.trim().is_empty()
        || spec.username.trim().is_empty()
        || spec.password.is_empty()
    {
        bail!("database credentials are incomplete");
    }
    if spec.docker_image.trim().is_empty() {
        bail!("database test image is required");
    }
    Ok(())
}

impl DockerManager {
    pub async fn test_database_connection(&self, spec: DatabaseConnectionSpec<'_>) -> Result<i64> {
        validate_database_spec(&spec)?;
        self.ensure_database_context(spec.server_id, spec.host)
            .await?;
        let probe = DatabaseProbe::from_spec(spec)?;
        let probe_name = format!("{}-dbtest-{}", probe.server_id, Uuid::new_v4().simple());

        let started = Instant::now();

        let probe_result = tokio::time::timeout(
            Duration::from_secs(20),
            self.run_database_probe(&probe_name, &probe),
        )
        .await;

        /*
         * Always remove the probe, including after timeout or cancellation.
         * The future inside timeout is dropped when the deadline expires, but
         * Docker containers continue running unless explicitly removed.
         */
        let cleanup_result = self.remove_database_probe(&probe_name).await;

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
                cleanup_result.context("remove successful database test container")?;

                Ok(started.elapsed().as_millis().min(i64::MAX as u128) as i64)
            }
        }
    }

    async fn ensure_database_context(&self, server_id: &str, database_id: &str) -> Result<()> {
        let source = self.inspect(server_id).await?;
        if !source
            .pointer("/State/Running")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            bail!("server container must be running to test its database connection");
        }

        let database = self.inspect(database_id).await?;
        if !database
            .pointer("/State/Running")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            bail!("database container is not running");
        }
        Ok(())
    }

    async fn run_database_probe(&self, name: &str, probe: &DatabaseProbe<'_>) -> Result<()> {
        // A database container already has this exact image locally. Avoid a
        // network pull on every test; it made a simple SELECT 1 wait for the
        // registry even when the database was healthy.
        self.ensure_local_image(probe.docker_image).await?;

        let config = ContainerCreateBody {
            image: Some(probe.docker_image.to_owned()),
            // Engine API environment entries must include the actual value.
            env: Some(vec![format!("{}={}", probe.password_key, probe.password)]),
            entrypoint: Some(vec![probe.entrypoint.to_owned()]),
            cmd: Some(probe.command.clone()),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            tty: Some(false),
            host_config: Some(HostConfig {
                // Share the server's Linux network namespace so the probe
                // observes exactly the same routes and DNS context.
                network_mode: Some(format!("container:{}", probe.server_id)),
                // Keep the failed container long enough to collect its logs.
                auto_remove: Some(false),
                ..Default::default()
            }),
            ..Default::default()
        };
        let options = CreateContainerOptionsBuilder::default().name(name).build();
        self.docker
            .create_container(Some(options), config)
            .await
            .context("create database connection test container")?;
        self.docker
            .start_container(name, None)
            .await
            .context("start database connection test container")?;

        let wait_options = WaitContainerOptionsBuilder::default()
            .condition("not-running")
            .build();
        let wait = self
            .docker
            .wait_container(name, Some(wait_options))
            .next()
            .await
            .context("database test wait stream ended unexpectedly")?
            .context("wait for database test container")?;
        if wait.status_code == 0 {
            return Ok(());
        }

        let logs = self.read_database_probe_logs(name).await;
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
        )
    }

    async fn read_database_probe_logs(&self, container: &str) -> String {
        const MAX_LOG_BYTES: usize = 32 * 1024;

        let options = LogsOptionsBuilder::default()
            .stdout(true)
            .stderr(true)
            .follow(false)
            .build();

        let mut stream = self.docker.logs(container, Some(options));

        let mut output = Vec::new();

        while let Some(item) = stream.next().await {
            let item = match item {
                Ok(item) => item,

                Err(error) => {
                    tracing::debug!(container, "failed reading database probe logs: {error}");
                    break;
                }
            };

            let bytes = item.into_bytes();

            let remaining = MAX_LOG_BYTES.saturating_sub(output.len());

            if remaining == 0 {
                break;
            }

            output.extend_from_slice(&bytes[..bytes.len().min(remaining)]);
        }

        String::from_utf8_lossy(&output).into_owned()
    }

    async fn remove_database_probe(&self, container: &str) -> Result<()> {
        let options = RemoveContainerOptionsBuilder::default()
            .force(true)
            .v(true)
            .build();

        match self.docker.remove_container(container, Some(options)).await {
            Ok(()) => Ok(()),

            /*
             * The timeout may occur before create_container completes, or the
             * container may already have disappeared.
             */
            Err(BollardError::DockerResponseServerError {
                status_code: 404, ..
            }) => Ok(()),

            Err(error) => {
                Err(error).with_context(|| format!("remove database probe container {container}"))
            }
        }
    }

    async fn ensure_local_image(&self, image: &str) -> Result<()> {
        match self.docker.inspect_image(image).await {
            Ok(_) => Ok(()),
            Err(BollardError::DockerResponseServerError {
                status_code: 404, ..
            }) => self.pull_image(image).await,
            Err(error) => {
                Err(error).with_context(|| format!("inspect database test image {image}"))
            }
        }
    }
}

pub(super) fn database_port(env: &[String]) -> Option<u16> {
    env.iter()
        .find_map(|entry| entry.strip_prefix("AGAPORNIS_DATABASE_PORT=")?.parse().ok())
}

pub(super) fn database_health_command(env: &[String]) -> Option<String> {
    let has = |prefix: &str| env.iter().any(|value| value.starts_with(prefix));

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

    let (raw_port, protocol) = value.split_once('/').unwrap_or((value, "tcp"));

    let port: u16 = raw_port
        .parse()
        .with_context(|| format!("Invalid internal port '{internal_port}'."))?;

    if !matches!(protocol, "tcp" | "udp" | "sctp") {
        bail!("Invalid internal port protocol '{protocol}'.");
    }

    Ok(Some(format!("{port}/{protocol}")))
}
