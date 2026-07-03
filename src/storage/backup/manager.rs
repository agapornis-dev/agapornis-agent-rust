use super::*;

impl Backups {
    pub async fn new() -> Self {
        Self {
            s3: Arc::new(S3Store::new().await),
        }
    }
    pub async fn create(
        &self,
        id: &str,
        storage: &str,
        retention: i32,
        encrypt: bool,
    ) -> Result<BackupInfo> {
        validate_storage(storage)?;
        paths::validate_id(id)?;
        if storage == "s3" && !self.s3.configured() {
            bail!("S3 storage is not configured on this agent.")
        }
        if encrypt {
            encryption_key()?;
        }
        let source = paths::server_dir(id)?;
        if !source.exists() {
            bail!("Server volume not found: {id}")
        }
        let backup_id = format!(
            "{}-{:08x}",
            Utc::now().format("%Y%m%d-%H%M%S"),
            rand::random::<u32>()
        );
        let dir = if storage == "local" {
            backup_dir(id)?
        } else {
            temp_dir()?
        };
        fs::create_dir_all(&dir).await?;
        let plain = dir.join(format!("{backup_id}.tar.gz"));
        tar_create(&source, &plain).await?;
        let size = fs::metadata(&plain).await?.len() as i64;
        let checksum = sha256(&plain).await?;
        let mut info = BackupInfo {
            backup_id: backup_id.clone(),
            server_id: id.into(),
            archive_name: plain.file_name().unwrap().to_string_lossy().into(),
            size_bytes: size,
            created_at: Utc::now().to_rfc3339(),
            checksum_sha256: checksum,
            checksum_type: CHECKSUM.into(),
            storage: storage.into(),
            encrypted: encrypt,
            last_verified_at: None,
            expires_at: None,
        };
        if storage == "local" {
            write_metadata(id, &info).await?
        } else {
            let upload = if encrypt {
                let p = plain.with_extension("gz.agp");
                encrypt_file(&plain, &p).await?;
                p
            } else {
                plain.clone()
            };
            self.s3
                .upload(id, &backup_id, &upload, &info, encrypt)
                .await?;
            self.verify(id, &backup_id, "s3").await?;
            let _ = fs::remove_file(&upload).await;
            let _ = fs::remove_file(&plain).await;
            if retention > 0 {
                let mut list = self.s3.list(id).await?;
                list.sort_by(|a, b| b.created_at.cmp(&a.created_at));
                for old in list.into_iter().skip(retention.max(1) as usize) {
                    self.s3.delete(id, &old.backup_id, old.encrypted).await?
                }
            }
            info = self
                .find(id, &backup_id, "s3")
                .await?
                .context("new remote backup disappeared")?;
        }
        Ok(info)
    }
    pub async fn list(&self, id: &str, remote: bool) -> Result<Vec<BackupInfo>> {
        let mut out = list_local(id).await?;
        if remote && self.s3.configured() {
            out.extend(self.s3.list(id).await?)
        }
        out.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(out)
    }
    pub async fn delete(&self, id: &str, bid: &str, storage: &str) -> Result<()> {
        validate_backup_id(bid)?;
        if storage == "s3" {
            let info = self
                .find(id, bid, storage)
                .await?
                .context("Backup not found.")?;
            self.s3.delete(id, bid, info.encrypted).await
        } else {
            fs::remove_file(archive_path(id, bid)?).await?;
            let _ = fs::remove_file(metadata_path(id, bid)?).await;
            Ok(())
        }
    }
    pub async fn restore(
        &self,
        id: &str,
        bid: &str,
        storage: &str,
        expected: Option<&str>,
    ) -> Result<()> {
        let (path, info, temp) = self.prepare(id, bid, storage).await?;
        verify_integrity(&path, &info, expected).await?;
        validate_archive(&path).await?;
        restore_transactionally(&path, id).await?;
        if temp {
            let _ = fs::remove_file(path).await;
        }
        Ok(())
    }
    pub async fn verify(&self, id: &str, bid: &str, storage: &str) -> Result<()> {
        let (path, mut info, temp) = self.prepare(id, bid, storage).await?;
        verify_integrity(&path, &info, None).await?;
        validate_archive(&path).await?;
        let test = temp_dir()?.join(format!("verify-{}", Uuid::new_v4()));
        fs::create_dir_all(&test).await?;
        tar_extract(&path, &test).await?;
        let _ = fs::remove_dir_all(test).await;
        info.last_verified_at = Some(Utc::now().to_rfc3339());
        if storage == "s3" {
            self.s3.put_metadata(id, bid, &info).await?
        } else {
            write_metadata(id, &info).await?
        }
        if temp {
            let _ = fs::remove_file(path).await;
        }
        Ok(())
    }
    pub async fn prepare_download(
        &self,
        id: &str,
        bid: &str,
        storage: &str,
    ) -> Result<(PathBuf, BackupInfo, bool)> {
        let (p, i, t) = self.prepare(id, bid, storage).await?;
        verify_integrity(&p, &i, None).await?;
        Ok((p, i, t))
    }
    async fn prepare(
        &self,
        id: &str,
        bid: &str,
        storage: &str,
    ) -> Result<(PathBuf, BackupInfo, bool)> {
        validate_storage(storage)?;
        let info = self
            .find(id, bid, storage)
            .await?
            .context("Backup not found.")?;
        if storage == "local" {
            return Ok((archive_path(id, bid)?, info, false));
        }
        let stored = temp_dir()?.join(format!("{bid}-{}.stored", Uuid::new_v4()));
        self.s3.download(id, bid, info.encrypted, &stored).await?;
        if info.encrypted {
            let plain = stored.with_extension("tar.gz");
            decrypt_file(&stored, &plain).await?;
            let _ = fs::remove_file(stored).await;
            Ok((plain, info, true))
        } else {
            let plain = stored.with_extension("tar.gz");
            fs::rename(stored, &plain).await?;
            Ok((plain, info, true))
        }
    }
    async fn find(&self, id: &str, bid: &str, storage: &str) -> Result<Option<BackupInfo>> {
        validate_backup_id(bid)?;
        if storage == "s3" {
            Ok(self
                .s3
                .list(id)
                .await?
                .into_iter()
                .find(|v| v.backup_id == bid))
        } else {
            read_metadata(id, bid).await
        }
    }
    pub async fn temporary_server(&self, id: &str) -> Result<PathBuf> {
        let target = temp_dir()?.join(format!("{id}-{}.tar.gz", Utc::now().format("%Y%m%d%H%M%S")));
        tar_create(&paths::server_dir(id)?, &target).await?;
        Ok(target)
    }
    pub async fn temporary_backups(&self, id: &str) -> Result<Option<PathBuf>> {
        let source = backup_dir(id)?;
        if !source.exists() || std::fs::read_dir(&source)?.next().is_none() {
            return Ok(None);
        }
        let target = temp_dir()?.join(format!(
            "{id}-backups-{}.tar.gz",
            Utc::now().format("%Y%m%d%H%M%S")
        ));
        tar_create(&source, &target).await?;
        Ok(Some(target))
    }
    pub async fn extract_server(&self, path: &Path, id: &str) -> Result<()> {
        validate_archive(path).await?;
        let target = paths::server_dir(id)?;
        fs::create_dir_all(&target).await?;
        tar_extract(path, &target).await
    }
    pub async fn extract_backups(&self, path: &Path, id: &str) -> Result<()> {
        validate_archive(path).await?;
        let target = backup_dir(id)?;
        fs::create_dir_all(&target).await?;
        tar_extract(path, &target).await
    }
    pub async fn delete_local(&self, id: &str) -> Result<()> {
        let path = backup_dir(id)?;
        if path.exists() {
            fs::remove_dir_all(path).await?
        }
        Ok(())
    }
}
