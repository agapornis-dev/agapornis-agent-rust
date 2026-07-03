use super::*;

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
        let mut data = Vec::new();
        while let Some(item) = stream.next().await {
            let item = item?;
            match item.data {
                Some(upload_file_request::Data::Metadata(m)) => {
                    if id.is_some() {
                        return Err(Status::invalid_argument("metadata may only be sent once"));
                    }
                    id = Some(m.server_id);
                    path = Some(m.target_path)
                }
                Some(upload_file_request::Data::ChunkData(bytes)) => {
                    if id.is_none() {
                        return Err(Status::invalid_argument(
                            "Metadata must be sent before chunk data.",
                        ));
                    }
                    if data.len() + bytes.len() > 128 * 1024 * 1024 {
                        return Err(Status::resource_exhausted("Uploaded file is too large."));
                    }
                    data.extend(bytes)
                }
                None => {}
            }
        }
        let Some(id) = id else {
            return Err(Status::invalid_argument("Upload metadata is required."));
        };
        let result = self
            .0
            .files
            .write(&id, &path.unwrap_or_default(), &data)
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
        let data = self
            .0
            .files
            .read(&r.server_id, &r.target_path)
            .await
            .map_err(internal)?;
        let stream = try_stream! {for chunk in data.chunks(81920){yield DownloadFileResponse{chunk_data:chunk.to_vec()};}};
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
            .read(&r.server_id, &r.target_path)
            .await
            .map_err(internal)?;
        if data.len() > 5 * 1024 * 1024 {
            return Err(Status::resource_exhausted(
                "File is too large to edit directly.",
            ));
        }
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
            self.0.files.install_mrpack(&r.server_id, &r.target_path).await,
        )))
    }
}
