//! Confined server file operations and ownership repair.

use crate::{docker::DockerManager, paths, process};
use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use serde::Deserialize;
use sha2::{Digest, Sha512};
use std::{
    collections::HashMap,
    io::{Read, Write},
    path::{Component, Path, PathBuf},
    sync::Arc,
};
use tokio::{fs, io::AsyncWriteExt};

const MAX_MODPACK_FILE_BYTES: u64 = 512 * 1024 * 1024;
const MAX_MODPACK_TOTAL_BYTES: u64 = 2 * 1024 * 1024 * 1024;

#[derive(Debug)]
pub struct Item {
    pub name: String,
    pub directory: bool,
    pub size: i64,
    pub modified: String,
}
#[derive(Clone)]
pub struct Files {
    docker: Arc<DockerManager>,
}
impl Files {
    pub fn new(docker: Arc<DockerManager>) -> Self {
        Self { docker }
    }

    async fn root(&self, id: &str) -> Result<Root> {
        let (host, container, running, exact) = self.docker.root(id).await?;
        Ok(Root {
            host,
            container,
            use_host: !running || exact,
        })
    }

    pub async fn list(&self, id: &str, path: &str) -> Result<Vec<Item>> {
        let mut clean_path = path.trim_start_matches(['/', '\\']);
        if clean_path.is_empty() {
            clean_path = "."; // Safely target the root directory
        }

        let root = self.root(id).await?;

        if root.use_host {
            fs::create_dir_all(&root.host).await?;
            let target = confined_host_path(&root.host, clean_path, true).await?;
            let mut read = match fs::read_dir(target).await {
                Ok(v) => v,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
                Err(e) => return Err(e.into()),
            };
            let mut out = vec![];
            while let Some(entry) = read.next_entry().await? {
                let meta = fs::symlink_metadata(entry.path()).await?;
                if meta.file_type().is_symlink() {
                    continue;
                }
                let modified = meta
                    .modified()
                    .ok()
                    .map(DateTime::<Utc>::from)
                    .map(|v| v.to_rfc3339())
                    .unwrap_or_default();
                out.push(Item {
                    name: entry.file_name().to_string_lossy().into(),
                    directory: meta.is_dir(),
                    size: if meta.is_file() { meta.len() as i64 } else { 0 },
                    modified,
                });
            }
            out.sort_by(|a, b| a.name.cmp(&b.name));
            Ok(out)
        } else {
            let target = paths::container_path(&root.container, clean_path)?.replace('\\', "/");
            let command = format!(
                r#"target={}; [ -d "$target" ] || exit 44; for f in "$target"/* "$target"/.[!.]* "$target"/..?*; do [ -e "$f" ] || continue; name=${{f##*/}}; if [ -d "$f" ]; then type=d; size=0; else type=f; size=$(wc -c < "$f" 2>/dev/null || echo 0); fi; mod=$(date -r "$f" -u +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || echo ""); printf '%s\t%s\t%s\t%s\n' "$name" "$type" "$size" "$mod"; done"#,
                process::shell_quote(&target)
            );
            let raw = self.docker.exec(id, &command).await?;
            Ok(raw
                .lines()
                .filter_map(|line| {
                    let p: Vec<&str> = line.split('\t').collect();
                    if p.len() < 4 {
                        return None;
                    }
                    Some(Item {
                        name: p[0].into(),
                        directory: p[1] == "d",
                        size: p[2].parse().unwrap_or(0),
                        modified: p[3].into(),
                    })
                })
                .collect())
        }
    }

    pub async fn read(&self, id: &str, path: &str) -> Result<Vec<u8>> {
        let mut clean_path = path.trim_start_matches(['/', '\\']);
        if clean_path.is_empty() {
            clean_path = ".";
        }

        let root = self.root(id).await?;

        if root.use_host {
            Ok(fs::read(confined_host_path(&root.host, clean_path, true).await?).await?)
        } else {
            let target = paths::container_path(&root.container, clean_path)?.replace('\\', "/");
            let raw = self
                .docker
                .exec(id, &format!("base64 < {}", process::shell_quote(&target)))
                .await?;
            Ok(base64::Engine::decode(
                &base64::engine::general_purpose::STANDARD,
                raw.split_whitespace().collect::<String>(),
            )?)
        }
    }

    pub async fn write(&self, id: &str, path: &str, bytes: &[u8]) -> Result<()> {
        let clean_path = path.trim_start_matches(['/', '\\']);
        if clean_path.is_empty() {
            bail!("A file path is required.")
        }

        let root = self.root(id).await?;
        if root.use_host {
            fs::create_dir_all(&root.host).await?;
            let target = confined_host_path(&root.host, clean_path, false).await?;
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent).await?;
            }
            fs::write(target, bytes).await?;
        } else {
            let target = paths::container_path(&root.container, clean_path)?.replace('\\', "/");
            let parent = target
                .rsplit_once('/')
                .map(|value| value.0)
                .unwrap_or(&root.container);
            let command = format!(
                "mkdir -p -- {} && cat > {}",
                process::shell_quote(parent),
                process::shell_quote(&target)
            );
            process::docker_with_input(["exec", "-i", id, "/bin/sh", "-lc", &command], bytes)
                .await?;
        }
        Ok(())
    }

    pub async fn delete(&self, id: &str, path: &str) -> Result<()> {
        let clean_path = path.trim_start_matches(['/', '\\']);

        // Block deletion if they try to target the root directory explicitly or by omitting the path
        if clean_path.is_empty() || clean_path == "." {
            bail!("Deleting the server root is not allowed.")
        }

        let root = self.root(id).await?;
        if root.use_host {
            let target = confined_host_path(&root.host, clean_path, true).await?;
            let meta = fs::symlink_metadata(&target).await?;
            if meta.is_dir() {
                fs::remove_dir_all(target).await?
            } else {
                fs::remove_file(target).await?
            }
        } else {
            let target = paths::container_path(&root.container, clean_path)?.replace('\\', "/");
            self.docker
                .exec(id, &format!("rm -rf -- {}", process::shell_quote(&target)))
                .await?;
        }
        Ok(())
    }

    pub async fn rename(&self, id: &str, path: &str, new_name: &str) -> Result<()> {
        let clean_path = path.trim_start_matches(['/', '\\']);
        validate_file_name(new_name)?;
        if clean_path.is_empty() || clean_path == "." {
            bail!("Renaming the server root is not allowed.")
        }
        let root = self.root(id).await?;
        if root.use_host {
            let source = confined_host_path(&root.host, clean_path, true).await?;
            let destination = source
                .parent()
                .ok_or_else(|| anyhow::anyhow!("invalid source path"))?
                .join(new_name);
            if fs::try_exists(&destination).await? {
                bail!("A file or directory with that name already exists.")
            }
            fs::rename(source, destination).await?;
        } else {
            let source = paths::container_path(&root.container, clean_path)?.replace('\\', "/");
            let parent = source
                .rsplit_once('/')
                .map(|value| value.0)
                .unwrap_or(&root.container);
            let destination = format!("{parent}/{new_name}");
            let command = format!(
                "[ ! -e {destination} ] || exit 45; mv -- {source} {destination}",
                destination = process::shell_quote(&destination),
                source = process::shell_quote(&source),
            );
            self.docker.exec(id, &command).await?;
        }
        Ok(())
    }

    pub async fn extract(
        &self,
        id: &str,
        archive_path: &str,
        destination_path: &str,
    ) -> Result<()> {
        let clean_archive = archive_path.trim_start_matches(['/', '\\']);
        if clean_archive.is_empty() {
            bail!("An archive path is required.")
        }
        let normalized = clean_archive.to_ascii_lowercase();
        let command = if normalized.ends_with(".tar.gz") || normalized.ends_with(".tgz") {
            "tar --no-same-owner --no-same-permissions -xzf /archive -C /extract"
        } else if normalized.ends_with(".tar") {
            "tar --no-same-owner --no-same-permissions -xf /archive -C /extract"
        } else if normalized.ends_with(".zip") {
            "unzip -o /archive -d /extract"
        } else {
            bail!("Only .zip, .tar, .tar.gz, and .tgz archives can be extracted.")
        };

        let (host_root, _, _, _) = self.docker.root(id).await?;
        let archive = confined_host_path(&host_root, clean_archive, true).await?;
        if !fs::metadata(&archive).await?.is_file() {
            bail!("Archive path is not a file.")
        }
        let destination = confined_host_path(&host_root, destination_path, false).await?;
        fs::create_dir_all(&destination).await?;
        let destination = confined_host_path(&host_root, destination_path, true).await?;
        let inspect = self.docker.inspect(id).await?;
        let image = inspect
            .pointer("/Config/Image")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| anyhow::anyhow!("server image is unavailable"))?;
        let archive_mount = format!("type=bind,src={},dst=/archive,readonly", archive.display());
        let destination_mount = format!("type=bind,src={},dst=/extract", destination.display());
        process::docker([
            "run",
            "--rm",
            "--pull",
            "never",
            "--network",
            "none",
            "--read-only",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--pids-limit",
            "128",
            "--memory",
            "256m",
            "--cpus",
            "0.5",
            "--user",
            "999:999",
            "--mount",
            &archive_mount,
            "--mount",
            &destination_mount,
            "--entrypoint",
            "/bin/sh",
            image,
            "-lc",
            command,
        ])
        .await?;
        Ok(())
    }

    pub async fn install_mrpack(&self, id: &str, archive_path: &str) -> Result<()> {
        let root = self.root(id).await?;
        if !root.use_host {
            bail!("Stop the server before installing a Modrinth modpack.")
        }
        let clean_archive = archive_path.trim_start_matches(['/', '\\']);
        if !clean_archive.to_ascii_lowercase().ends_with(".mrpack") {
            bail!("A .mrpack archive is required.")
        }
        let archive = confined_host_path(&root.host, clean_archive, true).await?;
        let manifest_archive = archive.clone();
        let manifest_text = tokio::task::spawn_blocking(move || -> Result<String> {
            let file = std::fs::File::open(manifest_archive)?;
            let mut zip = zip::ZipArchive::new(file)?;
            let mut entry = zip
                .by_name("modrinth.index.json")
                .map_err(|_| anyhow::anyhow!("modrinth.index.json is missing from this pack"))?;
            if entry.size() > 4 * 1024 * 1024 {
                bail!("Modrinth pack manifest is too large.")
            }
            let mut text = String::new();
            entry.read_to_string(&mut text)?;
            Ok(text)
        })
        .await??;
        let manifest: MrpackIndex = serde_json::from_str(&manifest_text)
            .map_err(|_| anyhow::anyhow!("Modrinth pack manifest is invalid"))?;
        if manifest.format_version != 1 {
            bail!("Unsupported Modrinth pack format version.")
        }

        let client = reqwest::Client::builder()
            .user_agent("Agapornis-Rust-Agent/1.0")
            .timeout(std::time::Duration::from_secs(120))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        let mut total_bytes = 0u64;
        for file in manifest.files {
            if file
                .env
                .as_ref()
                .and_then(|environment| environment.get("server"))
                .is_some_and(|value| value == "unsupported")
            {
                continue;
            }
            let relative = safe_pack_path(&file.path)?;
            let target = paths::safe_host_path(&root.host, relative.to_string_lossy().as_ref())?;
            let expected = file
                .hashes
                .get("sha512")
                .ok_or_else(|| anyhow::anyhow!("Pack file {} has no SHA-512 hash", file.path))?
                .to_ascii_lowercase();
            let url = file
                .downloads
                .first()
                .ok_or_else(|| anyhow::anyhow!("Pack file {} has no download URL", file.path))?;
            let parsed = reqwest::Url::parse(url)?;
            if parsed.scheme() != "https" || parsed.host_str() != Some("cdn.modrinth.com") {
                bail!("Pack file {} uses an untrusted download host.", file.path)
            }
            let response = client.get(parsed).send().await?.error_for_status()?;
            if response
                .content_length()
                .is_some_and(|length| length > MAX_MODPACK_FILE_BYTES)
            {
                bail!("Pack file {} exceeds the size limit.", file.path)
            }
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent).await?;
            }
            let temporary = target.with_extension(format!(
                "{}.agapornis-part",
                target.extension().and_then(|value| value.to_str()).unwrap_or("")
            ));
            let mut output = fs::File::create(&temporary).await?;
            let mut hasher = Sha512::new();
            let mut file_bytes = 0u64;
            let mut stream = response.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                file_bytes += chunk.len() as u64;
                total_bytes += chunk.len() as u64;
                if file_bytes > MAX_MODPACK_FILE_BYTES || total_bytes > MAX_MODPACK_TOTAL_BYTES {
                    let _ = fs::remove_file(&temporary).await;
                    bail!("Modrinth pack exceeds the extraction size limit.")
                }
                hasher.update(&chunk);
                output.write_all(&chunk).await?;
            }
            output.flush().await?;
            drop(output);
            if hex::encode(hasher.finalize()) != expected {
                let _ = fs::remove_file(&temporary).await;
                bail!("SHA-512 verification failed for {}.", file.path)
            }
            if fs::try_exists(&target).await? {
                fs::remove_file(&target).await?;
            }
            fs::rename(temporary, target).await?;
        }

        let overrides_archive = archive.clone();
        let output_root = root.host.clone();
        tokio::task::spawn_blocking(move || {
            extract_mrpack_overrides(&overrides_archive, &output_root)
        })
        .await??;
        Ok(())
    }
}

#[derive(Deserialize)]
struct MrpackIndex {
    #[serde(rename = "formatVersion")]
    format_version: u32,
    files: Vec<MrpackFile>,
}

#[derive(Deserialize)]
struct MrpackFile {
    path: String,
    hashes: HashMap<String, String>,
    downloads: Vec<String>,
    env: Option<HashMap<String, String>>,
}

fn safe_pack_path(value: &str) -> Result<PathBuf> {
    let path = Path::new(value);
    if value.is_empty()
        || value.contains('\\')
        || path.is_absolute()
        || path.components().any(|component| !matches!(component, Component::Normal(_)))
    {
        bail!("Modrinth pack contains an unsafe path.")
    }
    Ok(path.to_path_buf())
}

fn extract_mrpack_overrides(archive_path: &Path, root: &Path) -> Result<()> {
    let file = std::fs::File::open(archive_path)?;
    let mut archive = zip::ZipArchive::new(file)?;
    for prefix in ["overrides/", "server-overrides/"] {
        for index in 0..archive.len() {
            let mut entry = archive.by_index(index)?;
            let Some(relative_name) = entry.name().strip_prefix(prefix) else {
                continue;
            };
            if relative_name.is_empty() {
                continue;
            }
            if entry
                .unix_mode()
                .is_some_and(|mode| mode & 0o170000 == 0o120000)
            {
                bail!("Modrinth pack symbolic links are not allowed.")
            }
            let relative = safe_pack_path(relative_name)?;
            let target = root.join(relative);
            if entry.is_dir() {
                std::fs::create_dir_all(target)?;
                continue;
            }
            if let Some(parent) = target.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let temporary = target.with_extension(format!(
                "{}.agapornis-part",
                target.extension().and_then(|value| value.to_str()).unwrap_or("")
            ));
            let mut output = std::fs::File::create(&temporary)?;
            std::io::copy(&mut entry, &mut output)?;
            output.flush()?;
            if target.exists() {
                std::fs::remove_file(&target)?;
            }
            std::fs::rename(temporary, target)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::safe_pack_path;

    #[test]
    fn modpack_paths_stay_relative_and_confined() {
        assert!(safe_pack_path("mods/example.jar").is_ok());
        assert!(safe_pack_path("config/example.toml").is_ok());
        assert!(safe_pack_path("../server.jar").is_err());
        assert!(safe_pack_path("/etc/passwd").is_err());
        assert!(safe_pack_path("mods\\example.jar").is_err());
        assert!(safe_pack_path("./mods/example.jar").is_err());
    }
}

fn validate_file_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.contains('/')
        || name.contains('\\')
        || name.contains('\0')
        || name
            .chars()
            .any(|character| character.is_control() || "<>:\"|?*".contains(character))
    {
        bail!("New name must be a single file or directory name.")
    }
    Ok(())
}

async fn confined_host_path(
    root: &std::path::Path,
    requested: &str,
    must_exist: bool,
) -> Result<PathBuf> {
    let lexical = paths::safe_host_path(root, requested)?;
    let canonical_root = fs::canonicalize(root).await?;
    let canonical_target = match fs::canonicalize(&lexical).await {
        Ok(path) => path,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && !must_exist => {
            let mut ancestor = lexical.parent();
            loop {
                let Some(candidate) = ancestor else {
                    bail!("Path has no confined parent.")
                };
                match fs::canonicalize(candidate).await {
                    Ok(path) => break path,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        ancestor = candidate.parent()
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        }
        Err(error) => return Err(error.into()),
    };
    if !canonical_target.starts_with(&canonical_root) {
        bail!("Path escapes the server root.")
    }
    Ok(lexical)
}
struct Root {
    host: PathBuf,
    container: String,
    use_host: bool,
}
