use super::*;

pub(super) async fn ensure_network(name: &str) -> Result<()> {
    if process::docker(["network", "inspect", name]).await.is_ok() {
        return Ok(());
    }
    process::docker([
        "network",
        "create",
        "--driver",
        "bridge",
        "--label",
        "agapornis.managed=true",
        "--label",
        "agapornis.network_type=node",
        name,
    ])
    .await
    .map(|_| ())
}

pub(super) fn ensure_port(port: u16) -> Result<()> {
    TcpListener::bind(("0.0.0.0", port))
        .with_context(|| format!("Requested host port {port} is already in use."))?;
    Ok(())
}

pub(super) fn effective_cpus(percent: i32, cores: f64) -> f64 {
    if cores > 0.0 {
        cores
    } else if percent > 0 {
        percent as f64 / 100.0
    } else {
        0.0
    }
}

pub(super) fn validate_startup(root: &Path, command: &str) -> Result<()> {
    if let Some(target) = startup_target(command)
        && !root.join(&target).exists()
    {
        bail!(
            "Startup target '{}' was not found after the install script completed.",
            target.display()
        )
    }
    Ok(())
}

pub(super) fn startup_target(command: &str) -> Option<PathBuf> {
    command
        .split_whitespace()
        .map(|v| v.trim_matches(|c| c == '\'' || c == '\"'))
        .find(|v| v.ends_with(".jar") || v.starts_with("./"))
        .map(|v| PathBuf::from(v.trim_start_matches("./")))
}

pub(super) async fn apply_config_files(root: &Path, json: &str) -> Result<()> {
    if json.trim().is_empty() || json.trim() == "{}" {
        return Ok(());
    }
    let map: HashMap<String, Value> = serde_json::from_str(json)?;
    for (name, cfg) in map {
        let target = root.join(paths::relative(&name)?);
        if !target.exists() {
            continue;
        }
        let parser = cfg.get("parser").and_then(Value::as_str).unwrap_or("");
        let Some(find) = cfg.get("find").and_then(Value::as_object) else {
            continue;
        };
        if parser.eq_ignore_ascii_case("file") {
            let mut text = fs::read_to_string(&target).await?.replace("\r\n", "\n");
            for (needle, value) in find {
                let replacement = value
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| value.to_string());
                let mut replaced = false;
                let lines: Vec<String> = text
                    .lines()
                    .map(|line| {
                        if line.contains(needle) {
                            replaced = true;
                            replacement.clone()
                        } else {
                            line.into()
                        }
                    })
                    .collect();
                text = lines.join("\n");
                if !replaced {
                    text.push('\n');
                    text.push_str(&replacement)
                }
            }
            fs::write(target, text).await?
        } else if parser.eq_ignore_ascii_case("json") {
            let mut doc: Value = serde_json::from_slice(&fs::read(&target).await?)?;
            for (key, value) in find {
                set_json_path(&mut doc, key, value.clone())
            }
            fs::write(target, serde_json::to_vec_pretty(&doc)?).await?
        }
    }
    Ok(())
}

pub(super) fn set_json_path(root: &mut Value, path: &str, value: Value) {
    let parts: Vec<&str> = path.split('.').filter(|p| !p.is_empty()).collect();
    if parts.is_empty() {
        return;
    }
    let mut node = root;
    for part in &parts[..parts.len() - 1] {
        if !node.is_object() {
            *node = Value::Object(Default::default())
        }
        node = node
            .as_object_mut()
            .unwrap()
            .entry((*part).to_owned())
            .or_insert_with(|| Value::Object(Default::default()));
    }
    if !node.is_object() {
        *node = Value::Object(Default::default())
    }
    node.as_object_mut()
        .unwrap()
        .insert(parts[parts.len() - 1].into(), value);
}
