//! Safe path resolution for server-owned data.

use anyhow::{Result, bail};
use std::path::{Component, Path, PathBuf};

pub const HOME_CONTAINER_PATH: &str = "/home/container";
pub const DATA_CONTAINER_PATH: &str = "/data";

pub fn base_servers_dir() -> PathBuf {
    std::env::var_os("AGAPORNIS_SERVERS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            if cfg!(windows) {
                std::env::var_os("ProgramData")
                    .map(PathBuf::from)
                    .unwrap_or_else(|| PathBuf::from("C:\\ProgramData"))
                    .join("agapornis")
                    .join("servers")
            } else {
                PathBuf::from("/var/lib/agapornis/servers")
            }
        })
}

pub fn validate_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id == "."
        || id == ".."
        || id.contains('/')
        || id.contains('\\')
        || id.contains('\0')
    {
        bail!("invalid server id")
    }
    Ok(())
}
pub fn server_dir(id: &str) -> Result<PathBuf> {
    validate_id(id)?;
    Ok(base_servers_dir().join(id))
}
pub fn disk_limit_path(id: &str) -> Result<PathBuf> {
    validate_id(id)?;
    Ok(base_servers_dir()
        .join(".metadata")
        .join(format!("{id}.disk-limit")))
}
pub fn backup_dir(id: &str) -> Result<PathBuf> {
    Ok(server_dir(id)?.join(".agapornis-backups"))
}

pub fn relative(requested: &str) -> Result<PathBuf> {
    let normalized = requested.replace('\\', "/");
    let mut output = PathBuf::new();
    for (index, part) in Path::new(&normalized).components().enumerate() {
        match part {
            Component::Normal(value) => output.push(value),
            Component::CurDir => {}
            Component::RootDir if index == 0 => {}
            _ => bail!("invalid file path"),
        }
    }
    Ok(output)
}
pub fn safe_host_path(root: &Path, requested: &str) -> Result<PathBuf> {
    let rel = relative(requested)?;
    Ok(root.join(rel))
}
pub fn container_path(root: &str, requested: &str) -> Result<String> {
    let rel = relative(requested)?.to_string_lossy().replace('\\', "/");
    Ok(if rel.is_empty() {
        root.to_owned()
    } else {
        format!("{}/{rel}", root.trim_end_matches('/'))
    })
}
pub fn data_path(image: &str, env: &[String]) -> String {
    for entry in env {
        if let Some((key, value)) = entry.split_once('=')
            && [
                "AGAPORNIS_DATA_DIR",
                "SERVER_WORKDIR",
                "SERVER_DATA_DIR",
                "DATA_DIR",
            ]
            .contains(&key)
            && value.starts_with('/')
            && !value.contains("..")
        {
            return value.to_owned();
        }
    }
    if image.to_ascii_lowercase().contains("itzg/")
        || image.to_ascii_lowercase().contains("minecraft-server")
    {
        DATA_CONTAINER_PATH.to_owned()
    } else {
        HOME_CONTAINER_PATH.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_path_traversal() {
        assert!(relative("../secret").is_err());
        assert!(relative("safe/config.yml").is_ok());
        assert_eq!(
            relative("/safe/config.yml").unwrap(),
            PathBuf::from("safe/config.yml")
        );
        assert!(relative("/safe/../../secret").is_err());
    }
    #[test]
    fn rejects_unsafe_server_ids() {
        assert!(server_dir("../../etc").is_err());
        assert!(server_dir("server-123").is_ok());
    }
    #[test]
    fn detects_data_images() {
        assert_eq!(data_path("itzg/minecraft-server", &[]), "/data");
        assert_eq!(data_path("debian", &[]), "/home/container");
    }
}
