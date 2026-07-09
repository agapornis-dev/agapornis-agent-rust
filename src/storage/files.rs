//! Confined server file operations and ownership repair.

use crate::{docker::DockerManager, paths, process};
use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{fs, io::AsyncWriteExt};

const MAX_IN_MEMORY_FILE_BYTES: usize = 8 * 1024 * 1024;
const MAX_DIRECTORY_ENTRIES: usize = 10_000;

#[path = "files/modpack.rs"]
mod modpack;

#[derive(Debug)]
pub struct Item {
    pub name: String,
    pub directory: bool,
    pub size: i64,
    pub modified: String,
}

pub enum ReadSource {
    Host(PathBuf),
    Container { id: String, path: String },
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
                if out.len() > MAX_DIRECTORY_ENTRIES {
                    bail!(
                        "Directory contains more than {} entries.",
                        MAX_DIRECTORY_ENTRIES
                    )
                }
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
            let items = raw
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
                .collect::<Vec<_>>();
            if items.len() > MAX_DIRECTORY_ENTRIES {
                bail!(
                    "Directory contains more than {} entries.",
                    MAX_DIRECTORY_ENTRIES
                )
            }
            Ok(items)
        }
    }

    pub async fn read(&self, id: &str, path: &str) -> Result<Vec<u8>> {
        self.read_limited(id, path, MAX_IN_MEMORY_FILE_BYTES).await
    }

    pub async fn read_limited(&self, id: &str, path: &str, maximum: usize) -> Result<Vec<u8>> {
        match self.read_source(id, path).await? {
            ReadSource::Host(path) => {
                if fs::metadata(&path).await?.len() > maximum as u64 {
                    bail!("File is too large to read into memory.")
                }
                Ok(fs::read(path).await?)
            }
            ReadSource::Container { id, path } => {
                let byte_limit = maximum.saturating_add(1);
                let raw = self
                    .docker
                    .exec(
                        &id,
                        &format!(
                            "head -c {} -- {} | base64",
                            byte_limit,
                            process::shell_quote(&path)
                        ),
                    )
                    .await?;
                let decoded = base64::Engine::decode(
                    &base64::engine::general_purpose::STANDARD,
                    raw.split_whitespace().collect::<String>(),
                )?;
                if decoded.len() > maximum {
                    bail!("File is too large to read into memory.")
                }
                Ok(decoded)
            }
        }
    }

    pub async fn read_source(&self, id: &str, path: &str) -> Result<ReadSource> {
        let mut clean_path = path.trim_start_matches(['/', '\\']);
        if clean_path.is_empty() {
            clean_path = ".";
        }

        let root = self.root(id).await?;

        if root.use_host {
            Ok(ReadSource::Host(
                confined_host_path(&root.host, clean_path, true).await?,
            ))
        } else {
            let target = paths::container_path(&root.container, clean_path)?.replace('\\', "/");
            Ok(ReadSource::Container {
                id: id.to_owned(),
                path: target,
            })
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

    pub async fn write_from_path(&self, id: &str, path: &str, source: &Path) -> Result<()> {
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
            fs::copy(source, target).await?;
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
            let mut child = tokio::process::Command::new("docker")
                .args(["exec", "-i", id, "/bin/sh", "-lc", &command])
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .kill_on_drop(true)
                .spawn()
                .context("start Docker upload stream")?;
            let mut input = child.stdin.take().context("open Docker upload stream")?;
            let mut file = fs::File::open(source).await?;
            tokio::io::copy(&mut file, &mut input).await?;
            input.shutdown().await?;
            drop(input);
            let status = child.wait().await?;
            if !status.success() {
                bail!(
                    "docker upload stream exited with status {}",
                    status.code().unwrap_or(-1)
                )
            }
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
