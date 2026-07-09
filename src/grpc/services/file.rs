use super::*;

struct TemporaryUpload {
    path: std::path::PathBuf,
}

impl TemporaryUpload {
    async fn create() -> Result<(Self, fs::File), Status> {
        let path = std::env::temp_dir().join(format!("agapornis-upload-{}", uuid::Uuid::new_v4()));
        let file = fs::File::create(&path).await.map_err(internal)?;
        Ok((Self { path }, file))
    }
}

impl Drop for TemporaryUpload {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[derive(Clone)]
pub struct FileService(pub AppState);
#[tonic::async_trait]
impl proto::file_management_server::FileManagement for FileService {
    async fn upload_file(
        &self,
        r: Request<tonic::Streaming<UploadFileRequest>>,
    ) -> Result<Response<FileActionResponse>, Status> {
        let mut stream = r.into_inner();
        let mut id = None;
        let mut path = None;
        let mut temporary = None;
        let mut output = None;
        let mut total = 0usize;
        while let Some(item) = stream.next().await {
            let item = item?;
            match item.data {
                Some(upload_file_request::Data::Metadata(m)) => {
                    if id.is_some() {
                        return Err(Status::invalid_argument("metadata may only be sent once"));
                    }
                    id = Some(m.server_id);
                    path = Some(m.target_path);
                    let (guard, file) = TemporaryUpload::create().await?;
                    temporary = Some(guard);
                    output = Some(file);
                }
                Some(upload_file_request::Data::ChunkData(bytes)) => {
                    if id.is_none() {
                        return Err(Status::invalid_argument(
                            "Metadata must be sent before chunk data.",
                        ));
                    }
                    total = total.saturating_add(bytes.len());
                    if total > 128 * 1024 * 1024 {
                        return Err(Status::resource_exhausted("Uploaded file is too large."));
                    }
                    output
                        .as_mut()
                        .ok_or_else(|| Status::internal("upload temporary file is unavailable"))?
                        .write_all(&bytes)
                        .await
                        .map_err(internal)?;
                }
                None => {}
            }
        }
        let Some(id) = id else {
            return Err(Status::invalid_argument("Upload metadata is required."));
        };
        let temporary =
            temporary.ok_or_else(|| Status::internal("upload temporary file is unavailable"))?;
        if let Some(mut output) = output {
            output.flush().await.map_err(internal)?;
        }
        let result = self
            .0
            .files
            .write_from_path(&id, &path.unwrap_or_default(), &temporary.path)
            .await;
        Ok(Response::new(file_action(result)))
    }
    async fn delete_file_or_directory(
        &self,
        r: Request<DeleteFileRequest>,
    ) -> Result<Response<FileActionResponse>, Status> {
        let r = r.into_inner();
        Ok(Response::new(file_action(
            self.0.files.delete(&r.server_id, &r.target_path).await,
        )))
    }
    type DownloadFileStream = ResponseStream<DownloadFileResponse>;
    async fn download_file(
        &self,
        r: Request<DownloadFileRequest>,
    ) -> Result<Response<Self::DownloadFileStream>, Status> {
        let r = r.into_inner();
        let source = self
            .0
            .files
            .read_source(&r.server_id, &r.target_path)
            .await
            .map_err(internal)?;
        let stream = try_stream! {
            match source {
                ReadSource::Host(path) => {
                    let mut file = fs::File::open(path).await.map_err(internal)?;
                    let mut buffer = vec![0; 81920];
                    loop {
                        let n = file.read(&mut buffer).await.map_err(internal)?;
                        if n == 0 {
                            break;
                        }
                        yield DownloadFileResponse { chunk_data: buffer[..n].to_vec() };
                    }
                }
                ReadSource::Container { id, path } => {
                    let mut child = Command::new("docker")
                        .args(["exec", &id, "cat", "--", &path])
                        .stdout(std::process::Stdio::piped())
                        .stderr(std::process::Stdio::null())
                        .kill_on_drop(true)
                        .spawn()
                        .map_err(internal)?;
                    let mut stdout = child.stdout.take().ok_or_else(|| Status::internal("open Docker download stream"))?;
                    let mut buffer = vec![0; 81920];
                    loop {
                        let n = stdout.read(&mut buffer).await.map_err(internal)?;
                        if n == 0 {
                            break;
                        }
                        yield DownloadFileResponse { chunk_data: buffer[..n].to_vec() };
                    }
                    let status = child.wait().await.map_err(internal)?;
                    if !status.success() {
                        Err(Status::internal(format!(
                            "Docker download stream exited with status {}",
                            status.code().unwrap_or(-1)
                        )))?;
                    }
                }
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }
    async fn list_directory(
        &self,
        r: Request<ListDirectoryRequest>,
    ) -> Result<Response<ListDirectoryResponse>, Status> {
        let r = r.into_inner();
        let items = self
            .0
            .files
            .list(&r.server_id, &r.target_path)
            .await
            .map_err(internal)?
            .into_iter()
            .map(|v| FileItem {
                name: v.name,
                is_directory: v.directory,
                size: v.size,
                last_modified: v.modified,
            })
            .collect();
        Ok(Response::new(ListDirectoryResponse { items }))
    }
    async fn read_file_content(
        &self,
        r: Request<ReadFileRequest>,
    ) -> Result<Response<ReadFileResponse>, Status> {
        let r = r.into_inner();
        let data = self
            .0
            .files
            .read_limited(&r.server_id, &r.target_path, 5 * 1024 * 1024)
            .await
            .map_err(internal)?;
        let content = String::from_utf8(data)
            .map_err(|_| Status::invalid_argument("File is not valid UTF-8"))?;
        Ok(Response::new(ReadFileResponse { content }))
    }
    async fn write_file_content(
        &self,
        r: Request<WriteFileRequest>,
    ) -> Result<Response<FileActionResponse>, Status> {
        let r = r.into_inner();
        Ok(Response::new(file_action(
            self.0
                .files
                .write(&r.server_id, &r.target_path, r.content.as_bytes())
                .await,
        )))
    }
    async fn rename_file_or_directory(
        &self,
        r: Request<RenameFileRequest>,
    ) -> Result<Response<FileActionResponse>, Status> {
        let r = r.into_inner();
        Ok(Response::new(file_action(
            self.0
                .files
                .rename(&r.server_id, &r.target_path, &r.new_name)
                .await,
        )))
    }

    async fn extract_archive(
        &self,
        r: Request<ExtractArchiveRequest>,
    ) -> Result<Response<FileActionResponse>, Status> {
        let r = r.into_inner();
        Ok(Response::new(file_action(
            self.0
                .files
                .extract(&r.server_id, &r.target_path, &r.destination_path)
                .await,
        )))
    }

    async fn install_modpack(
        &self,
        r: Request<InstallModpackRequest>,
    ) -> Result<Response<FileActionResponse>, Status> {
        let r = r.into_inner();
        Ok(Response::new(file_action(
            self.0
                .files
                .install_mrpack(&r.server_id, &r.target_path)
                .await,
        )))
    }
}
