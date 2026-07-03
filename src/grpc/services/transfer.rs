use super::*;

#[derive(Clone)]
pub struct TransferService(pub AppState);
#[tonic::async_trait]
impl proto::node_transfer_server::NodeTransfer for TransferService {
    type ExportServerStream = ResponseStream<ExportServerResponse>;
    async fn export_server(
        &self,
        r: Request<ExportServerRequest>,
    ) -> Result<Response<Self::ExportServerStream>, Status> {
        let r = r.into_inner();
        if r.payload == TransferPayload::ServerData as i32 {
            self.0.docker.stop(&r.server_id).await.map_err(internal)?
        }
        let path = if r.payload == TransferPayload::LocalBackups as i32 {
            self.0
                .backups
                .temporary_backups(&r.server_id)
                .await
                .map_err(internal)?
        } else {
            Some(
                self.0
                    .backups
                    .temporary_server(&r.server_id)
                    .await
                    .map_err(internal)?,
            )
        };
        let size = match &path {
            Some(p) => fs::metadata(p).await.map_err(internal)?.len() as i64,
            None => 0,
        };
        let stream = try_stream! {yield ExportServerResponse{data:Some(export_server_response::Data::Metadata(ExportMetadata{server_id:r.server_id,archive_size_bytes:size,payload:r.payload}))};if let Some(path)=path{let mut file=fs::File::open(&path).await.map_err(internal)?;let mut buffer=vec![0;81920];loop{let n=file.read(&mut buffer).await.map_err(internal)?;if n==0{break}yield ExportServerResponse{data:Some(export_server_response::Data::ChunkData(buffer[..n].to_vec()))};}let _=fs::remove_file(path).await;}};
        Ok(Response::new(Box::pin(stream)))
    }
    async fn import_server(
        &self,
        r: Request<tonic::Streaming<ImportServerRequest>>,
    ) -> Result<Response<ImportServerResponse>, Status> {
        let mut input = r.into_inner();
        let path = std::env::temp_dir().join(format!(
            "agapornis-transfer-{}.tar.gz",
            uuid::Uuid::new_v4()
        ));
        let mut file = fs::File::create(&path).await.map_err(internal)?;
        let mut metadata = None;
        let mut total = 0i64;
        while let Some(frame) = input.next().await {
            match frame?.data {
                Some(import_server_request::Data::Metadata(v)) => {
                    if metadata.is_some() {
                        return Err(Status::invalid_argument(
                            "Import metadata may only be sent once.",
                        ));
                    }
                    metadata = Some(v)
                }
                Some(import_server_request::Data::ChunkData(v)) => {
                    if metadata.is_none() {
                        return Err(Status::invalid_argument(
                            "Metadata must be sent before chunk data.",
                        ));
                    }
                    total += v.len() as i64;
                    file.write_all(&v).await.map_err(internal)?
                }
                None => {}
            }
        }
        file.flush().await.map_err(internal)?;
        drop(file);
        let Some(m) = metadata else {
            return Err(Status::invalid_argument("Import metadata is required."));
        };
        if m.archive_size_bytes >= 0 && m.archive_size_bytes != total {
            return Err(Status::data_loss(format!(
                "Transfer size mismatch: expected {} bytes, received {total}.",
                m.archive_size_bytes
            )));
        }
        if m.payload == TransferPayload::ServerData as i32 && total == 0 {
            return Err(Status::data_loss("Server data transfer was empty."));
        }
        let result = if total == 0 {
            Ok(())
        } else if m.payload == TransferPayload::LocalBackups as i32 {
            self.0.backups.extract_backups(&path, &m.server_id).await
        } else {
            self.0.backups.extract_server(&path, &m.server_id).await
        };
        let _ = fs::remove_file(path).await;
        Ok(Response::new(match result {
            Ok(()) => ImportServerResponse {
                success: true,
                error_message: "".into(),
            },
            Err(e) => ImportServerResponse {
                success: false,
                error_message: e.to_string(),
            },
        }))
    }
    async fn delete_local_backups(
        &self,
        r: Request<ServerActionRequest>,
    ) -> Result<Response<ServerActionResponse>, Status> {
        Ok(Response::new(action(
            self.0.backups.delete_local(&r.into_inner().server_id).await,
        )))
    }
}
