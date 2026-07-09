use super::*;

use futures::StreamExt;
use serde::Deserialize;
use sha2::{Digest, Sha512};
use std::{
    collections::HashMap,
    io::{Read, Write},
    path::Component,
};

const MAX_FILE_BYTES: u64 = 512 * 1024 * 1024;
const MAX_TOTAL_BYTES: u64 = 2 * 1024 * 1024 * 1024;

impl Files {
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
        let manifest = read_manifest(archive.clone()).await?;
        if manifest.format_version != 1 {
            bail!("Unsupported Modrinth pack format version.")
        }

        let client = reqwest::Client::builder()
            .user_agent("Agapornis-Rust-Agent/1.0")
            .timeout(std::time::Duration::from_secs(120))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        download_files(&client, &root.host, manifest.files).await?;

        let output_root = root.host;
        tokio::task::spawn_blocking(move || extract_overrides(&archive, &output_root)).await??;
        Ok(())
    }
}

async fn read_manifest(archive: PathBuf) -> Result<MrpackIndex> {
    let text = tokio::task::spawn_blocking(move || -> Result<String> {
        let file = std::fs::File::open(archive)?;
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

    serde_json::from_str(&text).map_err(|_| anyhow::anyhow!("Modrinth pack manifest is invalid"))
}

async fn download_files(
    client: &reqwest::Client,
    root: &Path,
    files: Vec<MrpackFile>,
) -> Result<()> {
    let mut total_bytes = 0u64;
    for file in files {
        if file.is_unsupported_on_server() {
            continue;
        }
        download_file(client, root, file, &mut total_bytes).await?;
    }
    Ok(())
}

async fn download_file(
    client: &reqwest::Client,
    root: &Path,
    file: MrpackFile,
    total_bytes: &mut u64,
) -> Result<()> {
    let relative = safe_pack_path(&file.path)?;
    let target = paths::safe_host_path(root, relative.to_string_lossy().as_ref())?;
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
        .is_some_and(|length| length > MAX_FILE_BYTES)
    {
        bail!("Pack file {} exceeds the size limit.", file.path)
    }
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).await?;
    }

    let temporary = temporary_path(&target);
    let mut output = fs::File::create(&temporary).await?;
    let mut hasher = Sha512::new();
    let mut file_bytes = 0u64;
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file_bytes += chunk.len() as u64;
        *total_bytes += chunk.len() as u64;
        if file_bytes > MAX_FILE_BYTES || *total_bytes > MAX_TOTAL_BYTES {
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
    Ok(())
}

fn extract_overrides(archive_path: &Path, root: &Path) -> Result<()> {
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
            let target = root.join(safe_pack_path(relative_name)?);
            if entry.is_dir() {
                std::fs::create_dir_all(target)?;
                continue;
            }
            if let Some(parent) = target.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let temporary = temporary_path(&target);
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

fn temporary_path(target: &Path) -> PathBuf {
    target.with_extension(format!(
        "{}.agapornis-part",
        target
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or("")
    ))
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

impl MrpackFile {
    fn is_unsupported_on_server(&self) -> bool {
        self.env
            .as_ref()
            .and_then(|environment| environment.get("server"))
            .is_some_and(|value| value == "unsupported")
    }
}

fn safe_pack_path(value: &str) -> Result<PathBuf> {
    let path = Path::new(value);
    if value.is_empty()
        || value.contains('\\')
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        bail!("Modrinth pack contains an unsafe path.")
    }
    Ok(path.to_path_buf())
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
