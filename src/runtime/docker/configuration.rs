use super::*;

use bollard::{
    errors::Error as BollardError,
    models::NetworkCreateRequest,
};

impl DockerManager {
    pub(super) async fn ensure_network(
        &self,
        name: &str,
    ) -> Result<()> {
        match self.docker.inspect_network(name, None).await {
            Ok(_) => Ok(()),

            Err(BollardError::DockerResponseServerError {
                status_code: 404,
                ..
            }) => {
                let labels = HashMap::from([
                    (
                        "agapornis.managed".to_owned(),
                        "true".to_owned(),
                    ),
                    (
                        "agapornis.network_type".to_owned(),
                        "node".to_owned(),
                    ),
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
                    Err(
                        BollardError::DockerResponseServerError {
                            status_code: 409,
                            ..
                        },
                    ) => Ok(()),

                    Err(error) => Err(error).with_context(|| {
                        format!(
                            "create Docker network {name}"
                        )
                    }),
                }
            }

            Err(error) => Err(error).with_context(|| {
                format!("inspect Docker network {name}")
            }),
        }
    }
}

pub(super) fn ensure_port(port: u16) -> Result<()> {
    TcpListener::bind(("0.0.0.0", port))
        .with_context(|| {
            format!(
                "Requested host port {port} is already in use."
            )
        })?;

    Ok(())
}

pub(super) fn effective_cpus(
    percent: i32,
    cores: f64,
) -> f64 {
    if cores > 0.0 {
        cores
    } else if percent > 0 {
        percent as f64 / 100.0
    } else {
        0.0
    }
}

pub(super) fn validate_startup(
    root: &Path,
    command: &str,
) -> Result<()> {
    if let Some(target) = startup_target(command)
        && !root.join(&target).exists()
    {
        bail!(
            "Startup target '{}' was not found after the install \
             script completed.",
            target.display()
        );
    }

    Ok(())
}

pub(super) fn startup_target(
    command: &str,
) -> Option<PathBuf> {
    command
        .split_whitespace()
        .map(|value| {
            value.trim_matches(|character| {
                character == '\'' || character == '"'
            })
        })
        .find(|value| {
            value.ends_with(".jar")
                || value.starts_with("./")
        })
        .map(|value| {
            PathBuf::from(value.trim_start_matches("./"))
        })
}

pub(super) async fn apply_config_files(
    root: &Path,
    json: &str,
) -> Result<()> {
    if json.trim().is_empty() || json.trim() == "{}" {
        return Ok(());
    }

    let map: HashMap<String, Value> =
        serde_json::from_str(json)?;

    for (name, config) in map {
        let target =
            root.join(paths::relative(&name)?);

        if !target.exists() {
            continue;
        }

        let parser = config
            .get("parser")
            .and_then(Value::as_str)
            .unwrap_or("");

        let Some(find) = config
            .get("find")
            .and_then(Value::as_object)
        else {
            continue;
        };

        if parser.eq_ignore_ascii_case("file") {
            let mut text = fs::read_to_string(&target)
                .await?
                .replace("\r\n", "\n");

            for (needle, value) in find {
                let replacement = value
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| value.to_string());

                let mut replaced = false;

                let lines = text
                    .lines()
                    .map(|line| {
                        if line.contains(needle) {
                            replaced = true;
                            replacement.clone()
                        } else {
                            line.to_owned()
                        }
                    })
                    .collect::<Vec<_>>();

                text = lines.join("\n");

                if !replaced {
                    if !text.is_empty()
                        && !text.ends_with('\n')
                    {
                        text.push('\n');
                    }

                    text.push_str(&replacement);
                }
            }

            fs::write(&target, text).await?;
        } else if parser.eq_ignore_ascii_case("json") {
            let mut document: Value =
                serde_json::from_slice(
                    &fs::read(&target).await?,
                )?;

            for (key, value) in find {
                set_json_path(
                    &mut document,
                    key,
                    value.clone(),
                );
            }

            fs::write(
                &target,
                serde_json::to_vec_pretty(&document)?,
            )
            .await?;
        }
    }

    Ok(())
}

pub(super) fn set_json_path(
    root: &mut Value,
    path: &str,
    value: Value,
) {
    let parts = path
        .split('.')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();

    if parts.is_empty() {
        return;
    }

    let mut node = root;

    for part in &parts[..parts.len() - 1] {
        if !node.is_object() {
            *node = Value::Object(Default::default());
        }

        node = node
            .as_object_mut()
            .expect("node was converted to an object")
            .entry((*part).to_owned())
            .or_insert_with(|| {
                Value::Object(Default::default())
            });
    }

    if !node.is_object() {
        *node = Value::Object(Default::default());
    }

    node.as_object_mut()
        .expect("node was converted to an object")
        .insert(
            parts[parts.len() - 1].to_owned(),
            value,
        );
}