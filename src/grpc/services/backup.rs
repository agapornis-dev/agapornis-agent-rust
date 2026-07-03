use super::*;

#[derive(Clone)]
pub struct BackupService(pub AppState);
#[tonic::async_trait]
impl proto::backup_management_server::BackupManagement for BackupService {
    async fn create_backup(
        &self,
        r: Request<CreateBackupRequest>,
    ) -> Result<Response<CreateBackupResponse>, Status> {
        let r = r.into_inner();
        let storage = storage(&r.storage);
        Ok(Response::new(
            match self
                .0
                .backups
                .create(&r.server_id, storage, r.retention_count, r.encrypt)
                .await
            {
                Ok(v) => CreateBackupResponse {
                    success: true,
                    backup_id: v.backup_id,
                    error_message: "".into(),
                    size_bytes: v.size_bytes,
                    checksum_sha256: v.checksum_sha256,
                    checksum_type: v.checksum_type,
                    storage: v.storage,
                    encrypted: v.encrypted,
                },
                Err(e) => CreateBackupResponse {
                    success: false,
                    error_message: e.to_string(),
                    ..Default::default()
                },
            },
        ))
    }
    async fn list_backups(
        &self,
        r: Request<ListBackupsRequest>,
    ) -> Result<Response<ListBackupsResponse>, Status> {
        let r = r.into_inner();
        let list = self
            .0
            .backups
            .list(&r.server_id, r.include_remote)
            .await
            .map_err(internal)?;
        Ok(Response::new(ListBackupsResponse {
            backups: list.into_iter().map(backup_entry).collect(),
        }))
    }
    async fn delete_backup(
        &self,
        r: Request<DeleteBackupRequest>,
    ) -> Result<Response<BackupActionResponse>, Status> {
        let r = r.into_inner();
        Ok(Response::new(backup_action(
            self.0
                .backups
                .delete(&r.server_id, &r.backup_id, storage(&r.storage))
                .await,
        )))
    }
    async fn restore_backup(
        &self,
        r: Request<RestoreBackupRequest>,
    ) -> Result<Response<BackupActionResponse>, Status> {
        let r = r.into_inner();
        let expected =
            (!r.expected_checksum_sha256.is_empty()).then_some(r.expected_checksum_sha256.as_str());
        Ok(Response::new(backup_action(
            self.0
                .backups
                .restore(&r.server_id, &r.backup_id, storage(&r.storage), expected)
                .await,
        )))
    }
    type DownloadBackupStream = ResponseStream<DownloadBackupResponse>;
    async fn download_backup(
        &self,
        r: Request<DownloadBackupRequest>,
    ) -> Result<Response<Self::DownloadBackupStream>, Status> {
        let r = r.into_inner();
        let (path, info, temp) = self
            .0
            .backups
            .prepare_download(&r.server_id, &r.backup_id, storage(&r.storage))
            .await
            .map_err(internal)?;
        let stream = try_stream! {yield DownloadBackupResponse{chunk_data:vec![],backup_id:info.backup_id,size_bytes:info.size_bytes,checksum_sha256:info.checksum_sha256,checksum_type:info.checksum_type};let mut file=fs::File::open(&path).await.map_err(internal)?;let mut buffer=vec![0;1024*1024];loop{let n=file.read(&mut buffer).await.map_err(internal)?;if n==0{break}yield DownloadBackupResponse{chunk_data:buffer[..n].to_vec(),..Default::default()};}if temp{let _=fs::remove_file(path).await;}};
        Ok(Response::new(Box::pin(stream)))
    }
    async fn verify_backup(
        &self,
        r: Request<VerifyBackupRequest>,
    ) -> Result<Response<BackupActionResponse>, Status> {
        let r = r.into_inner();
        Ok(Response::new(backup_action(
            self.0
                .backups
                .verify(&r.server_id, &r.backup_id, storage(&r.storage))
                .await,
        )))
    }
}
