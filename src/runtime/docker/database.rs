use super::*;

impl DockerManager {
    pub async fn test_database_connection(&self, spec: DatabaseConnectionSpec<'_>) -> Result<i64> {
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
            bail!("database port is invalid")
        }
        if database_name.trim().is_empty() || username.trim().is_empty() || password.is_empty() {
            bail!("database credentials are incomplete")
        }
        let source = self.inspect(server_id).await?;
        if !source
            .pointer("/State/Running")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            bail!("server container must be running to test its database connection")
        }
        let database = self.inspect(host).await?;
        if !database
            .pointer("/State/Running")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            bail!("database container is not running")
        }

        let network = format!("container:{server_id}");
        let port = port.to_string();
        let started = Instant::now();
        let (password_key, args): (&str, Vec<String>) = match database_type {
            "mysql" => (
                "MYSQL_PWD",
                vec![
                    "run".into(),
                    "--rm".into(),
                    "--network".into(),
                    network,
                    "--env".into(),
                    "MYSQL_PWD".into(),
                    "--entrypoint".into(),
                    "mysql".into(),
                    docker_image.into(),
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
            "mariadb" => (
                "MYSQL_PWD",
                vec![
                    "run".into(),
                    "--rm".into(),
                    "--network".into(),
                    network,
                    "--env".into(),
                    "MYSQL_PWD".into(),
                    "--entrypoint".into(),
                    "mariadb".into(),
                    docker_image.into(),
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
                vec![
                    "run".into(),
                    "--rm".into(),
                    "--network".into(),
                    network,
                    "--env".into(),
                    "PGPASSWORD".into(),
                    "--entrypoint".into(),
                    "psql".into(),
                    docker_image.into(),
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
        tokio::time::timeout(
            Duration::from_secs(20),
            process::docker_with_env(args, password_key, password),
        )
        .await
        .context("database connection test timed out")??;
        Ok(started.elapsed().as_millis().min(i64::MAX as u128) as i64)
    }
}

pub(super) fn database_port(env: &[String]) -> Option<u16> {
    env.iter()
        .find_map(|e| e.strip_prefix("AGAPORNIS_DATABASE_PORT=")?.parse().ok())
}

pub(super) fn database_health_command(env: &[String]) -> Option<String> {
    let has = |prefix: &str| env.iter().any(|value| value.starts_with(prefix));
    if has("POSTGRES_PASSWORD=") {
        return Some("pg_isready --host=127.0.0.1 --port=\"$AGAPORNIS_DATABASE_PORT\" --username=\"$POSTGRES_USER\" --dbname=\"$POSTGRES_DB\"".into());
    }
    if has("MYSQL_ROOT_PASSWORD=") {
        return Some("mysqladmin ping --protocol=tcp --host=127.0.0.1 --port=\"$AGAPORNIS_DATABASE_PORT\" --user=root --password=\"$MYSQL_ROOT_PASSWORD\" --silent".into());
    }
    if has("MARIADB_ROOT_PASSWORD=") {
        return Some("mariadb-admin ping --protocol=tcp --host=127.0.0.1 --port=\"$AGAPORNIS_DATABASE_PORT\" --user=root --password=\"$MARIADB_ROOT_PASSWORD\" --silent".into());
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
        bail!("Invalid internal port protocol '{protocol}'.")
    }
    Ok(Some(format!("{port}/{protocol}")))
}

pub(super) fn port_arguments(internal_port: &str, publish: bool, host_port: i32) -> Vec<String> {
    let mut args = vec!["--expose".into(), internal_port.into()];
    if publish {
        args.extend([
            "--publish".into(),
            format!("0.0.0.0:{host_port}:{internal_port}"),
        ]);
    }
    args
}
