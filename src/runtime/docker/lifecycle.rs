use super::*;

impl DockerManager {
    pub async fn start(&self, id: &str) -> Result<()> {
        self.ensure_disk(id).await?;
        self.protection.manual_recovery(id);
        let _ = process::docker(["update", "--restart", "on-failure:2", id]).await?;
        process::docker(["start", id]).await.map(|_| ())
    }

    pub async fn stop(&self, id: &str) -> Result<()> {
        self.detach_console(id).await;
        process::docker(["stop", "--time", "10", id])
            .await
            .map(|_| ())
    }

    pub async fn restart(&self, id: &str) -> Result<()> {
        self.detach_console(id).await;
        self.ensure_disk(id).await?;
        self.protection.manual_recovery(id);
        let _ = process::docker(["update", "--restart", "on-failure:2", id]).await?;
        process::docker(["restart", "--time", "10", id])
            .await
            .map(|_| ())
    }

    pub async fn delete(&self, id: &str) -> Result<()> {
        paths::validate_id(id)?;
        self.detach_console(id).await;
        let _ = process::docker(["rm", "--force", "--volumes", id]).await;
        let dir = paths::server_dir(id)?;
        if dir.exists() {
            fs::remove_dir_all(dir).await?;
        }
        let limit = paths::disk_limit_path(id)?;
        let _ = fs::remove_file(limit).await;
        self.disk_cache.lock().await.remove(id);
        self.protection.remove(id);
        Ok(())
    }

    pub async fn update_resources(
        &self,
        id: &str,
        memory: i64,
        percent: i32,
        cores: f64,
        disk: i64,
    ) -> Result<()> {
        let mut args = vec!["update".to_owned()];
        if memory > 0 {
            args.extend([
                "--memory".into(),
                memory.to_string(),
                "--memory-swap".into(),
                memory.to_string(),
            ]);
        }
        let cpus = effective_cpus(percent, cores);
        if cpus > 0.0 {
            args.extend(["--cpus".into(), cpus.to_string()]);
        }
        args.push(id.into());
        if args.len() > 2 {
            let refs: Vec<&str> = args.iter().map(String::as_str).collect();
            process::docker(refs).await?;
        }
        if disk > 0 {
            let p = paths::disk_limit_path(id)?;
            fs::create_dir_all(p.parent().unwrap()).await?;
            fs::write(p, disk.to_string()).await?;
        }
        self.disk_cache.lock().await.remove(id);
        Ok(())
    }

    async fn ensure_disk(&self, id: &str) -> Result<()> {
        let (usage, limit) = self.disk_force(id).await?;
        if limit > 0 && usage > limit {
            self.protection.mark(id, "disk-limit-exceeded");
            bail!(
                "Server disk limit exceeded ({usage} / {limit} bytes). Delete files before starting the server."
            )
        }
        self.protection.clear_disk(id);
        Ok(())
    }
}
