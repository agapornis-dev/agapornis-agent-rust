use super::*;

pub(super) fn backups_base() -> PathBuf {
    std::env::var_os("AGAPORNIS_BACKUPS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            if cfg!(windows) {
                PathBuf::from(
                    std::env::var_os("ProgramData").unwrap_or_else(|| "C:\\ProgramData".into()),
                )
                .join("agapornis/backups")
            } else {
                PathBuf::from("/var/lib/agapornis/backups")
            }
        })
}
pub(super) fn backup_dir(id: &str) -> Result<PathBuf> {
    paths::validate_id(id)?;
    Ok(backups_base().join(id))
}
pub(super) fn archive_path(id: &str, bid: &str) -> Result<PathBuf> {
    validate_backup_id(bid)?;
    Ok(backup_dir(id)?.join(format!("{bid}.tar.gz")))
}
pub(super) fn metadata_path(id: &str, bid: &str) -> Result<PathBuf> {
    validate_backup_id(bid)?;
    Ok(backup_dir(id)?.join(format!("{bid}.json")))
}
pub(super) fn temp_dir() -> Result<PathBuf> {
    let p = std::env::temp_dir().join("agapornis-backups");
    std::fs::create_dir_all(&p)?;
    Ok(p)
}
pub(super) fn validate_backup_id(id: &str) -> Result<()> {
    if id.is_empty() || id.contains("..") || id.contains('/') || id.contains('\\') {
        bail!("Invalid backup id.")
    }
    Ok(())
}
pub(super) fn validate_storage(s: &str) -> Result<()> {
    if s != "local" && s != "s3" {
        bail!("Backup storage must be local or s3.")
    }
    Ok(())
}
pub(super) async fn write_metadata(id: &str, info: &BackupInfo) -> Result<()> {
    let path = metadata_path(id, &info.backup_id)?;
    fs::create_dir_all(path.parent().unwrap()).await?;
    fs::write(path, serde_json::to_vec_pretty(info)?).await?;
    Ok(())
}
pub(super) async fn read_metadata(id: &str, bid: &str) -> Result<Option<BackupInfo>> {
    match fs::read(metadata_path(id, bid)?).await {
        Ok(v) => Ok(serde_json::from_slice(&v).ok()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}
pub(super) async fn list_local(id: &str) -> Result<Vec<BackupInfo>> {
    let dir = backup_dir(id)?;
    let mut out = vec![];
    let mut entries = match fs::read_dir(&dir).await {
        Ok(v) => v,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(e.into()),
    };
    while let Some(e) = entries.next_entry().await? {
        let name = e.file_name().to_string_lossy().into_owned();
        if let Some(bid) = name.strip_suffix(".tar.gz") {
            if let Some(info) = read_metadata(id, bid).await? {
                out.push(info)
            } else {
                let m = e.metadata().await?;
                out.push(BackupInfo {
                    backup_id: bid.into(),
                    server_id: id.into(),
                    archive_name: name,
                    size_bytes: m.len() as i64,
                    created_at: DateTimeString::from_meta(&m),
                    checksum_sha256: "".into(),
                    checksum_type: CHECKSUM.into(),
                    storage: "local".into(),
                    encrypted: false,
                    last_verified_at: None,
                    expires_at: None,
                })
            }
        }
    }
    Ok(out)
}
struct DateTimeString;
impl DateTimeString {
    fn from_meta(m: &std::fs::Metadata) -> String {
        m.created()
            .or_else(|_| m.modified())
            .ok()
            .map(chrono::DateTime::<Utc>::from)
            .map(|v| v.to_rfc3339())
            .unwrap_or_default()
    }
}
pub(super) async fn tar_create(source: &Path, target: &Path) -> Result<()> {
    process::run(
        "tar",
        [
            "-czf",
            target.to_string_lossy().as_ref(),
            "-C",
            source.to_string_lossy().as_ref(),
            ".",
        ],
    )
    .await
    .map(|_| ())
}
pub(super) async fn tar_extract(source: &Path, target: &Path) -> Result<()> {
    process::run(
        "tar",
        [
            "-xzf",
            source.to_string_lossy().as_ref(),
            "-C",
            target.to_string_lossy().as_ref(),
        ],
    )
    .await
    .map(|_| ())
}
pub(super) async fn restore_transactionally(archive: &Path, server_id: &str) -> Result<()> {
    let target = paths::server_dir(server_id)?;
    let parent = target.parent().context("server directory has no parent")?;
    fs::create_dir_all(parent).await?;
    let suffix = Uuid::new_v4().simple().to_string();
    let staging = parent.join(format!(".{server_id}.restore-{suffix}"));
    let previous = parent.join(format!(".{server_id}.previous-{suffix}"));
    fs::create_dir_all(&staging).await?;
    if let Err(error) = tar_extract(archive, &staging).await {
        let _ = fs::remove_dir_all(&staging).await;
        return Err(error);
    }

    if !target.exists() {
        return fs::rename(&staging, &target)
            .await
            .context("activate restored server data");
    }

    /*
     * Keep the target directory itself in place. Docker Desktop creates an
     * internal WSL bind source for this directory and stores that source in
     * the container configuration. Replacing the directory root invalidates
     * the internal source, leaving the container unable to mount /data after
     * a restore. Moving the children preserves the bind mount while retaining
     * replacement (rather than overlay) restore semantics.
     */
    fs::create_dir_all(&previous).await?;
    if let Err(error) = move_directory_contents(&target, &previous).await {
        let rollback = move_directory_contents(&previous, &target).await;
        let _ = fs::remove_dir_all(&previous).await;
        let _ = fs::remove_dir_all(&staging).await;
        if let Err(rollback_error) = rollback {
            return Err(error).context(format!(
                "move current server data aside before restore; rollback also failed: {rollback_error:#}"
            ));
        }
        return Err(error).context("move current server data aside before restore");
    }

    if let Err(error) = move_directory_contents(&staging, &target).await {
        let clear_result = clear_directory_contents(&target).await;
        let rollback = move_directory_contents(&previous, &target).await;
        let _ = fs::remove_dir_all(&staging).await;
        let _ = fs::remove_dir_all(&previous).await;
        if let Err(clear_error) = clear_result {
            return Err(error).context(format!(
                "activate restored server data; clearing the partial restore failed: {clear_error:#}"
            ));
        }
        if let Err(rollback_error) = rollback {
            return Err(error).context(format!(
                "activate restored server data; rollback also failed: {rollback_error:#}"
            ));
        }
        return Err(error).context("activate restored server data");
    }

    let _ = fs::remove_dir_all(&staging).await;
    let _ = fs::remove_dir_all(&previous).await;
    Ok(())
}

async fn move_directory_contents(source: &Path, target: &Path) -> Result<()> {
    fs::create_dir_all(target).await?;
    let mut entries = fs::read_dir(source).await?;
    while let Some(entry) = entries.next_entry().await? {
        let destination = target.join(entry.file_name());
        fs::rename(entry.path(), &destination)
            .await
            .with_context(|| {
                format!(
                    "move restored entry {} to {}",
                    entry.path().display(),
                    destination.display()
                )
            })?;
    }
    Ok(())
}

async fn clear_directory_contents(directory: &Path) -> Result<()> {
    let mut entries = fs::read_dir(directory).await?;
    while let Some(entry) = entries.next_entry().await? {
        let file_type = entry.file_type().await?;
        if file_type.is_dir() && !file_type.is_symlink() {
            fs::remove_dir_all(entry.path()).await?;
        } else {
            fs::remove_file(entry.path()).await?;
        }
    }
    Ok(())
}
pub(super) async fn validate_archive(path: &Path) -> Result<()> {
    let mut child = tokio::process::Command::new("tar")
        .args(["-tzf", path.to_string_lossy().as_ref()])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .context("start archive validation")?;
    let stdout = child
        .stdout
        .take()
        .context("open archive validation output")?;
    let mut lines = BufReader::new(stdout).lines();
    while let Some(raw) = lines.next_line().await? {
        let entry = raw.trim().replace('\\', "/");
        if entry.starts_with('/') || entry.split('/').any(|v| v == "..") {
            bail!("Backup contains an unsafe archive path.")
        }
    }
    let status = child.wait().await?;
    if !status.success() {
        bail!(
            "archive validation exited with status {}",
            status.code().unwrap_or(-1)
        )
    }
    Ok(())
}
pub(super) async fn sha256(path: &Path) -> Result<String> {
    let mut f = fs::File::open(path).await?;
    let mut h = Sha256::new();
    let mut b = vec![0; 1024 * 1024];
    loop {
        let n = f.read(&mut b).await?;
        if n == 0 {
            break;
        }
        h.update(&b[..n]);
    }
    Ok(hex::encode(h.finalize()))
}
pub(super) async fn verify_integrity(
    path: &Path,
    info: &BackupInfo,
    expected: Option<&str>,
) -> Result<()> {
    let actual = sha256(path).await?;
    if let Some(expected) = expected
        && !hash_eq(&actual, expected)
    {
        bail!("Backup checksum did not match the requested restore.")
    }
    if !info.checksum_sha256.is_empty() && !hash_eq(&actual, &info.checksum_sha256) {
        bail!("Backup checksum did not match its metadata.")
    }
    if fs::metadata(path).await?.len() as i64 != info.size_bytes {
        bail!("Backup size did not match its metadata.")
    }
    Ok(())
}
pub(super) fn hash_eq(a: &str, b: &str) -> bool {
    let (Ok(a), Ok(b)) = (hex::decode(a), hex::decode(b)) else {
        return false;
    };
    a.len() == b.len() && bool::from(a.ct_eq(&b))
}
