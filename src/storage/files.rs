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
            fs::write(&target, bytes).await?;
            normalize_host_ownership(&root.host, &target).await?;
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
            fs::copy(source, &target).await?;
            normalize_host_ownership(&root.host, &target).await?;
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

    pub async fn create_directory(&self, id: &str, path: &str) -> Result<()> {
        let relative = required_relative_path(path, "A directory path is required.")?;
        let root = self.root(id).await?;
        if root.use_host {
            fs::create_dir_all(&root.host).await?;
            let target = confined_host_path(&root.host, &relative.to_string_lossy(), false).await?;
            fs::create_dir(&target).await?;
            normalize_host_ownership(&root.host, &target).await?;
        } else {
            let target = paths::container_path(&root.container, &relative.to_string_lossy())?
                .replace('\\', "/");
            self.docker
                .exec(id, &format!("mkdir -- {}", process::shell_quote(&target)))
                .await?;
        }
        Ok(())
    }

    pub async fn move_files(
        &self,
        id: &str,
        source_paths: &[String],
        destination_path: &str,
    ) -> Result<()> {
        let sources = validated_sources(source_paths)?;
        let destination_relative = paths::relative(destination_path)?;
        let root = self.root(id).await?;

        if root.use_host {
            let destination =
                confined_host_path(&root.host, &destination_relative.to_string_lossy(), true)
                    .await?;
            if !fs::metadata(&destination).await?.is_dir() {
                bail!("Move destination must be a directory.")
            }

            let mut moves = Vec::with_capacity(sources.len());
            for relative in &sources {
                let source =
                    confined_host_path(&root.host, &relative.to_string_lossy(), true).await?;
                if fs::symlink_metadata(&source)
                    .await?
                    .file_type()
                    .is_symlink()
                {
                    bail!("Symbolic links cannot be moved.")
                }
                let target = destination.join(
                    source
                        .file_name()
                        .ok_or_else(|| anyhow::anyhow!("invalid source path"))?,
                );
                if source == target {
                    bail!("An item is already in the selected destination.")
                }
                if fs::try_exists(&target).await? {
                    bail!("A file or directory with that name already exists in the destination.")
                }
                if fs::metadata(&source).await?.is_dir() && destination.starts_with(&source) {
                    bail!("A directory cannot be moved inside itself.")
                }
                moves.push((source, target));
            }
            for (source, target) in moves {
                fs::rename(source, target).await?;
            }
        } else {
            let destination =
                paths::container_path(&root.container, &destination_relative.to_string_lossy())?
                    .replace('\\', "/");
            let mut checks = vec![format!(
                "[ -d {} ] || {{ echo 'Move destination must be a directory.' >&2; exit 44; }}",
                process::shell_quote(&destination),
            )];
            let mut moves = Vec::with_capacity(sources.len());
            for relative in &sources {
                let source = paths::container_path(&root.container, &relative.to_string_lossy())?
                    .replace('\\', "/");
                let name = relative
                    .file_name()
                    .ok_or_else(|| anyhow::anyhow!("invalid source path"))?
                    .to_string_lossy();
                let target = format!("{}/{}", destination.trim_end_matches('/'), name);
                checks.push(format!(
                    "[ -e {source} ] && [ ! -L {source} ] || {{ echo 'Move source is unavailable.' >&2; exit 45; }}; [ {source} != {target} ] || {{ echo 'An item is already in the selected destination.' >&2; exit 46; }}; [ ! -e {target} ] || {{ echo 'An item with that name already exists.' >&2; exit 47; }}; case {destination}/ in {source}/*) echo 'A directory cannot be moved inside itself.' >&2; exit 48;; esac",
                    source = process::shell_quote(&source),
                    target = process::shell_quote(&target),
                    destination = process::shell_quote(&destination),
                ));
                moves.push(format!(
                    "mv -- {} {}",
                    process::shell_quote(&source),
                    process::shell_quote(&target),
                ));
            }
            checks.extend(moves);
            self.docker.exec(id, &checks.join("; ")).await?;
        }
        Ok(())
    }

    pub async fn create_archive(
        &self,
        id: &str,
        source_paths: &[String],
        destination_path: &str,
    ) -> Result<()> {
        let sources = validated_sources(source_paths)?;
        let destination_relative =
            required_relative_path(destination_path, "An archive destination path is required.")?;
        if !destination_relative
            .to_string_lossy()
            .to_ascii_lowercase()
            .ends_with(".tar.gz")
        {
            bail!("Archive name must end with .tar.gz.")
        }
        let root = self.root(id).await?;

        if root.use_host {
            fs::create_dir_all(&root.host).await?;
            let destination =
                confined_host_path(&root.host, &destination_relative.to_string_lossy(), false)
                    .await?;
            if fs::try_exists(&destination).await? {
                bail!("A file or directory with the archive name already exists.")
            }
            let parent = destination
                .parent()
                .ok_or_else(|| anyhow::anyhow!("invalid archive path"))?;
            if !fs::metadata(parent).await?.is_dir() {
                bail!("Archive destination directory does not exist.")
            }
            for source_relative in &sources {
                let source =
                    confined_host_path(&root.host, &source_relative.to_string_lossy(), true)
                        .await?;
                let metadata = fs::symlink_metadata(&source).await?;
                if metadata.file_type().is_symlink() {
                    bail!("Symbolic links cannot be archived.")
                }
                if metadata.is_dir() && destination.starts_with(&source) {
                    bail!("An archive cannot be created inside a selected directory.")
                }
            }
            let mut arguments = vec![
                "-czf".to_owned(),
                destination.to_string_lossy().into_owned(),
                "-C".to_owned(),
                root.host.to_string_lossy().into_owned(),
                "--".to_owned(),
            ];
            arguments.extend(
                sources
                    .iter()
                    .map(|source| source.to_string_lossy().into_owned()),
            );
            if let Err(error) = process::run("tar", arguments).await {
                let _ = fs::remove_file(&destination).await;
                return Err(error);
            }
            normalize_host_ownership(&root.host, &destination).await?;
        } else {
            let destination =
                paths::container_path(&root.container, &destination_relative.to_string_lossy())?
                    .replace('\\', "/");
            let parent = destination
                .rsplit_once('/')
                .map(|value| value.0)
                .unwrap_or(&root.container);
            let temporary = format!("{}.agapornis-{}", destination, uuid::Uuid::new_v4());
            let mut checks = vec![
                format!(
                    "[ -d {} ] || {{ echo 'Archive destination directory does not exist.' >&2; exit 44; }}",
                    process::shell_quote(parent)
                ),
                format!(
                    "[ ! -e {} ] || {{ echo 'An item with the archive name already exists.' >&2; exit 45; }}",
                    process::shell_quote(&destination)
                ),
            ];
            let mut archive_sources = Vec::with_capacity(sources.len());
            for relative in &sources {
                let source = paths::container_path(&root.container, &relative.to_string_lossy())?
                    .replace('\\', "/");
                checks.push(format!("[ -e {0} ] && [ ! -L {0} ] || {{ echo 'Archive source is unavailable.' >&2; exit 46; }}", process::shell_quote(&source)));
                checks.push(format!("case {}/ in {}/*) echo 'An archive cannot be created inside a selected directory.' >&2; exit 47;; esac", process::shell_quote(&destination), process::shell_quote(&source)));
                archive_sources.push(process::shell_quote(&relative.to_string_lossy()));
            }
            checks.push(format!(
                "trap 'rm -f -- {temporary}' EXIT; cd {root}; tar -czf {temporary} -- {sources}; mv -- {temporary} {destination}; trap - EXIT",
                temporary = process::shell_quote(&temporary),
                root = process::shell_quote(&root.container),
                sources = archive_sources.join(" "),
                destination = process::shell_quote(&destination),
            ));
            self.docker.exec(id, &checks.join("; ")).await?;
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
        normalize_host_ownership(&host_root, &destination).await?;
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
            paths::SERVER_RUNTIME_USER,
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

fn ownership_paths(root: &Path, target: &Path) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    let mut current = Some(target);
    while let Some(path) = current {
        if !path.starts_with(root) {
            break;
        }
        paths.push(path.to_path_buf());
        if path == root {
            break;
        }
        current = path.parent();
    }
    paths
}

async fn normalize_host_ownership(root: &Path, target: &Path) -> Result<()> {
    if !cfg!(unix) {
        return Ok(());
    }
    let mut arguments = vec![paths::SERVER_RUNTIME_USER.to_owned()];
    arguments.extend(
        ownership_paths(root, target)
            .into_iter()
            .map(|path| path.to_string_lossy().into_owned()),
    );
    process::run("chown", arguments)
        .await
        .context("assign file operation output to the server runtime user")?;
    Ok(())
}

fn required_relative_path(path: &str, message: &str) -> Result<PathBuf> {
    let relative = paths::relative(path)?;
    if relative.as_os_str().is_empty() {
        bail!(message.to_owned())
    }
    Ok(relative)
}

fn validated_sources(source_paths: &[String]) -> Result<Vec<PathBuf>> {
    if source_paths.is_empty() {
        bail!("At least one source path is required.")
    }
    if source_paths.len() > 100 {
        bail!("No more than 100 items can be processed at once.")
    }
    let mut sources = Vec::with_capacity(source_paths.len());
    for path in source_paths {
        let relative = required_relative_path(path, "The server root cannot be selected.")?;
        if sources.contains(&relative) {
            bail!("Duplicate source paths are not allowed.")
        }
        sources.push(relative);
    }
    Ok(sources)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selected_sources_are_confined_unique_and_bounded() {
        assert!(validated_sources(&[]).is_err());
        assert!(validated_sources(&["/safe/file.txt".into(), "/safe/file.txt".into()]).is_err());
        assert!(validated_sources(&["../../outside".into()]).is_err());
        assert!(validated_sources(&["/".into()]).is_err());
        assert_eq!(
            validated_sources(&["/safe/file.txt".into(), "/folder".into()]).unwrap(),
            vec![PathBuf::from("safe/file.txt"), PathBuf::from("folder")],
        );
    }

    #[test]
    fn ownership_normalization_includes_nested_parents_and_root() {
        let root = PathBuf::from("/servers/example");
        let target = root.join("plugins/config/settings.json");
        assert_eq!(
            ownership_paths(&root, &target),
            vec![
                target,
                root.join("plugins/config"),
                root.join("plugins"),
                root,
            ],
        );
    }
}
