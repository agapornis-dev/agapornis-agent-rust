use super::*;

use bollard::{
    errors::Error as BollardError,
    models::{HostConfig, NetworkCreateRequest, RestartPolicy, RestartPolicyNameEnum},
};
#[cfg(test)]
use serde_json::Map;

mod line;
mod structured;
mod xml;

#[cfg(test)]
mod tests;

use line::{apply_file_parser, apply_ini_parser, apply_properties_parser};
#[cfg(test)]
use structured::apply_structured_replacements;
use structured::{apply_json_parser, apply_yaml_parser};
use xml::apply_xml_parser;

const MAX_CONFIG_FILE_SIZE: u64 = 8 * 1024 * 1024;
const STARTUP_TARGET_CHECK_ATTEMPTS: usize = 10;
const XDG_RUNTIME_DIR_KEY: &str = "XDG_RUNTIME_DIR";
const MANAGED_XDG_RUNTIME_DIR: &str = "/tmp/agapornis-runtime";
const MANAGED_XDG_RUNTIME_TMPFS_OPTIONS: &str =
    "rw,nosuid,nodev,noexec,size=16m,mode=0700,uid=999,gid=999";
const RUNTIME_LAUNCHER: &str = r#"case "${XVFB:-0}" in
    1|true|TRUE|yes|YES)
        if command -v xvfb-run >/dev/null 2>&1; then
            exec xvfb-run --auto-servernum \
                --server-args="-screen 0 ${DISPLAY_WIDTH:-1024}x${DISPLAY_HEIGHT:-768}x${DISPLAY_DEPTH:-16} -nolisten tcp" \
                /bin/sh -lc "$1"
        fi
        ;;
esac
exec /bin/sh -lc "$1""#;
#[cfg(not(test))]
const STARTUP_TARGET_CHECK_INTERVAL: Duration = Duration::from_millis(200);
#[cfg(test)]
const STARTUP_TARGET_CHECK_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Debug)]
pub(super) struct MissingStartupTarget {
    pub(super) target: PathBuf,
    pub(super) resolved: PathBuf,
}

impl std::fmt::Display for MissingStartupTarget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "Startup target '{}' was not found at '{}'.",
            self.target.display(),
            self.resolved.display()
        )
    }
}

impl std::error::Error for MissingStartupTarget {}

pub(super) fn runtime_environment(values: &[String]) -> Vec<String> {
    let configured = values
        .iter()
        .rev()
        .find_map(|entry| {
            entry
                .split_once('=')
                .filter(|(key, _)| *key == XDG_RUNTIME_DIR_KEY)
        })
        .map(|(_, value)| value.trim())
        .filter(|value| !value.is_empty())
        .unwrap_or(MANAGED_XDG_RUNTIME_DIR);

    values
        .iter()
        .filter(|entry| {
            entry
                .split_once('=')
                .is_none_or(|(key, _)| key != XDG_RUNTIME_DIR_KEY)
        })
        .cloned()
        .chain([format!("{XDG_RUNTIME_DIR_KEY}={configured}")])
        .collect()
}

/// Build the managed server command without interpolating the configured
/// startup string into the launcher shell. Wine images that opt into XVFB get
/// a ready virtual display before the workload begins; all other images keep
/// the ordinary shell startup behavior.
pub(super) fn runtime_server_command(startup: &str) -> Option<Vec<String>> {
    (!startup.trim().is_empty()).then(|| {
        vec![
            "/bin/sh".into(),
            "-lc".into(),
            RUNTIME_LAUNCHER.into(),
            "agapornis-runtime".into(),
            format!("exec {startup}"),
        ]
    })
}

pub(super) fn legacy_runtime_command(command: &[String]) -> Option<&str> {
    match command {
        [shell, option, startup] if shell == "/bin/sh" && option == "-lc" => startup
            .strip_prefix("exec ")
            .filter(|startup| !startup.trim().is_empty()),
        _ => None,
    }
}

pub(super) fn runtime_launcher_repair_needed(inspect: &Value) -> bool {
    let Some(command) = inspect
        .pointer("/Config/Cmd")
        .and_then(Value::as_array)
        .map(|command| {
            command
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
    else {
        return false;
    };

    legacy_runtime_command(&command).is_some()
}

pub(super) fn repair_legacy_runtime_command(command: &mut Option<Vec<String>>) -> bool {
    let Some(startup) = command
        .as_deref()
        .and_then(legacy_runtime_command)
        .map(str::to_owned)
    else {
        return false;
    };

    *command = runtime_server_command(&startup);
    true
}

pub(super) fn manual_restart_policy() -> RestartPolicy {
    RestartPolicy {
        name: Some(RestartPolicyNameEnum::NO),
        maximum_retry_count: Some(0),
    }
}

pub(super) fn ensure_runtime_tmpfs(host_config: &mut HostConfig, environment: &[String]) {
    if runtime_environment_value(environment) != Some(MANAGED_XDG_RUNTIME_DIR) {
        return;
    }

    host_config.tmpfs.get_or_insert_default().insert(
        MANAGED_XDG_RUNTIME_DIR.into(),
        MANAGED_XDG_RUNTIME_TMPFS_OPTIONS.into(),
    );
}

pub(super) fn runtime_tmpfs_ready(
    host_config: Option<&HostConfig>,
    environment: &[String],
) -> bool {
    if runtime_environment_value(environment) != Some(MANAGED_XDG_RUNTIME_DIR) {
        return true;
    }

    host_config
        .and_then(|config| config.tmpfs.as_ref())
        .and_then(|tmpfs| tmpfs.get(MANAGED_XDG_RUNTIME_DIR))
        .is_some_and(|options| options == MANAGED_XDG_RUNTIME_TMPFS_OPTIONS)
}

pub(super) fn runtime_configuration_ready(inspect: &Value) -> bool {
    let environment = inspect
        .pointer("/Config/Env")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect::<Vec<_>>();

    match runtime_environment_value(&environment) {
        Some(value) if !value.trim().is_empty() && value != MANAGED_XDG_RUNTIME_DIR => true,
        Some(MANAGED_XDG_RUNTIME_DIR) => inspect
            .pointer("/HostConfig/Tmpfs")
            .and_then(Value::as_object)
            .and_then(|tmpfs| tmpfs.get(MANAGED_XDG_RUNTIME_DIR))
            .and_then(Value::as_str)
            .is_some_and(|options| options == MANAGED_XDG_RUNTIME_TMPFS_OPTIONS),
        _ => false,
    }
}

fn runtime_environment_value(values: &[String]) -> Option<&str> {
    values.iter().rev().find_map(|entry| {
        entry
            .split_once('=')
            .filter(|(key, _)| *key == XDG_RUNTIME_DIR_KEY)
            .map(|(_, value)| value)
    })
}

impl DockerManager {
    pub(super) async fn ensure_network(&self, name: &str) -> Result<()> {
        match self.docker.inspect_network(name, None).await {
            Ok(_) => Ok(()),

            Err(BollardError::DockerResponseServerError {
                status_code: 404, ..
            }) => {
                let labels = HashMap::from([
                    ("agapornis.managed".to_owned(), "true".to_owned()),
                    ("agapornis.network_type".to_owned(), "node".to_owned()),
                ]);

                let request = NetworkCreateRequest {
                    name: name.to_owned(),
                    driver: Some("bridge".to_owned()),
                    labels: Some(labels),
                    ..Default::default()
                };

                match self.docker.create_network(request).await {
                    Ok(_) => Ok(()),

                    // Another create operation may have created the network
                    // between inspect_network() and create_network().
                    Err(BollardError::DockerResponseServerError {
                        status_code: 409, ..
                    }) => Ok(()),

                    Err(error) => {
                        Err(error).with_context(|| format!("create Docker network {name}"))
                    }
                }
            }

            Err(error) => Err(error).with_context(|| format!("inspect Docker network {name}")),
        }
    }
}

pub(super) fn ensure_port(port: u16) -> Result<()> {
    TcpListener::bind(("0.0.0.0", port))
        .with_context(|| format!("Requested host port {port} is already in use."))?;

    Ok(())
}

pub(super) fn effective_cpus(percent: i32, _legacy_cores: f64) -> f64 {
    if percent > 0 {
        percent as f64 / 100.0
    } else {
        0.0
    }
}

pub(super) fn pinned_cpu_set(value: &str) -> Result<Option<String>> {
    let value = value.split_whitespace().collect::<String>();
    if value.is_empty() {
        return Ok(None);
    }
    let available = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1);
    let mut selected = HashSet::new();
    for segment in value.split(',') {
        let (start, end) = match segment.split_once('-') {
            Some((start, end)) => (start.parse::<usize>()?, end.parse::<usize>()?),
            None => {
                let thread = segment.parse::<usize>()?;
                (thread, thread)
            }
        };
        if end < start {
            bail!("invalid pinned CPU thread range '{segment}'");
        }
        if end >= available {
            bail!(
                "pinned CPU thread {end} does not exist; this node has threads 0-{}",
                available - 1
            );
        }
        for thread in start..=end {
            if !selected.insert(thread) {
                bail!("pinned CPU thread {thread} is listed more than once");
            }
        }
    }
    if selected.is_empty() {
        return Ok(None);
    }
    Ok(Some(value))
}

pub(super) fn effective_disk_limit(disk: i64, swap: i64, storage: &str) -> Result<i64> {
    if swap < 0 {
        bail!("swap memory cannot be negative");
    }
    if storage == "server" && swap > 0 {
        if disk <= swap {
            bail!("server storage must be larger than swap memory");
        }
        Ok(disk - swap)
    } else {
        Ok(disk)
    }
}

pub(super) async fn validate_startup(root: &Path, command: &str) -> Result<()> {
    if let Some(missing) = missing_startup_target(root, command).await? {
        return Err(missing.into());
    }

    Ok(())
}

pub(super) async fn missing_startup_target(
    root: &Path,
    command: &str,
) -> Result<Option<MissingStartupTarget>> {
    let Some(target) = startup_target(command) else {
        return Ok(None);
    };
    let resolved = root.join(&target);

    for attempt in 1..=STARTUP_TARGET_CHECK_ATTEMPTS {
        match fs::metadata(&resolved).await {
            Ok(metadata) if metadata.is_file() => return Ok(None),
            Ok(_) => {
                bail!(
                    "Startup target '{}' exists at '{}', but is not a regular file.",
                    target.display(),
                    resolved.display()
                );
            }
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound
                    && attempt < STARTUP_TARGET_CHECK_ATTEMPTS =>
            {
                tokio::time::sleep(STARTUP_TARGET_CHECK_INTERVAL).await;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Some(MissingStartupTarget { target, resolved }));
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "inspect startup target '{}' at '{}'",
                        target.display(),
                        resolved.display()
                    )
                });
            }
        }
    }

    unreachable!("startup target check loop always returns")
}

pub(super) fn startup_target(command: &str) -> Option<PathBuf> {
    command
        .split_whitespace()
        .map(|value| value.trim_matches(|character| character == '\'' || character == '"'))
        .find(|value| value.ends_with(".jar") || value.starts_with("./"))
        .map(|value| PathBuf::from(value.trim_start_matches("./")))
}

pub(super) async fn apply_config_files(
    root: &Path,
    json: &str,
    docker_interface: &str,
) -> Result<()> {
    if json.trim().is_empty() || json.trim() == "{}" {
        return Ok(());
    }

    let mut descriptor: Value =
        serde_json::from_str(json).context("parse configuration-files descriptor")?;
    resolve_daemon_placeholders(&mut descriptor, docker_interface);
    let map: HashMap<String, Value> = serde_json::from_value(descriptor)?;
    for (name, config) in map {
        let target = root.join(paths::relative(&name)?);
        apply_config_file(&target, &config)
            .await
            .with_context(|| format!("apply configuration descriptor to {}", target.display()))?;
    }

    Ok(())
}

pub(super) async fn docker_network_interface(docker: &Docker, network: &str) -> Result<String> {
    let inspect = docker
        .inspect_network(network, None)
        .await
        .with_context(|| format!("inspect Docker network {network}"))?;
    let document = serde_json::to_value(inspect)?;

    find_named_string(&document, "Gateway")
        .or_else(|| find_named_string(&document, "gateway"))
        .context("Docker network does not define an IPAM gateway")
}

fn find_named_string(value: &Value, name: &str) -> Option<String> {
    match value {
        Value::Object(object) => {
            if let Some(value) = object.get(name).and_then(Value::as_str)
                && !value.is_empty()
            {
                return Some(value.to_owned());
            }
            object
                .values()
                .find_map(|value| find_named_string(value, name))
        }
        Value::Array(array) => array
            .iter()
            .find_map(|value| find_named_string(value, name)),
        _ => None,
    }
}

fn resolve_daemon_placeholders(value: &mut Value, docker_interface: &str) {
    match value {
        Value::String(text) => {
            *text = text
                .replace("{{config.docker.interface}}", docker_interface)
                .replace("{{config.docker.network.interface}}", docker_interface);
        }
        Value::Array(array) => {
            for value in array {
                resolve_daemon_placeholders(value, docker_interface);
            }
        }
        Value::Object(object) => {
            for value in object.values_mut() {
                resolve_daemon_placeholders(value, docker_interface);
            }
        }
        _ => {}
    }
}

async fn apply_config_file(target: &Path, config: &Value) -> Result<()> {
    let metadata = match fs::metadata(target).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };

    if metadata.len() > MAX_CONFIG_FILE_SIZE {
        bail!(
            "configuration file is {} bytes; maximum is {} bytes",
            metadata.len(),
            MAX_CONFIG_FILE_SIZE
        );
    }

    let parser = config.get("parser").and_then(Value::as_str).unwrap_or("");

    let Some(find) = config.get("find").and_then(Value::as_object) else {
        return Ok(());
    };

    let bytes = fs::read(target).await?;
    let output = match parser.to_ascii_lowercase().as_str() {
        "file" => apply_file_parser(&bytes, find),
        "properties" => apply_properties_parser(&bytes, find),
        "ini" => apply_ini_parser(&bytes, find),
        "json" => apply_json_parser(&bytes, find)?,
        "yaml" | "yml" => apply_yaml_parser(&bytes, find)?,
        "xml" => apply_xml_parser(&bytes, find)?,
        "" => bail!("configuration parser is required"),
        other => bail!("unsupported configuration parser '{other}'"),
    };

    fs::write(target, output).await?;
    Ok(())
}

fn replacement_for_current(replacement: &Value, current: &str) -> Option<Value> {
    match replacement {
        Value::Object(options) => options.get(current).cloned(),
        value => Some(value.clone()),
    }
}

fn scalar_text(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        value => value.to_string(),
    }
}
