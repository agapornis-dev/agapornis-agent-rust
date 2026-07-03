//! gRPC adapters that translate protobuf requests into domain operations.

use crate::{
    backup::{BackupInfo, Backups},
    certificate::CertificateManager,
    config::DaemonConfig,
    docker::{CreateSpec, DatabaseConnectionSpec, DockerManager},
    file_service::Files,
    node,
    protection::{LineDecision, ProtectionState},
    proto::{self, *},
    update::UpdateManager,
};
use async_stream::try_stream;
use futures::{Stream, StreamExt};
use std::{
    collections::{HashMap, VecDeque},
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use tokio::{
    fs,
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::Command,
    sync::{Mutex, broadcast},
};
use tokio_stream::wrappers::BroadcastStream;
use tonic::{Request, Response, Status};
use tracing::error;

type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Clone)]
pub struct AppState {
    pub docker: Arc<DockerManager>,
    pub protection: Arc<ProtectionState>,
    pub console: Arc<ConsoleHub>,
    pub files: Files,
    pub backups: Backups,
    pub update: UpdateManager,
    pub certificates: CertificateManager,
    pub config: DaemonConfig,
}
impl AppState {
    pub async fn new(config: DaemonConfig, certificates: CertificateManager) -> Self {
        let protection = Arc::new(ProtectionState::default());
        let docker = Arc::new(DockerManager::new(protection.clone()));
        let console = Arc::new(ConsoleHub::new(protection.clone()));
        Self {
            files: Files::new(docker.clone()),
            backups: Backups::new().await,
            update: UpdateManager,
            certificates,
            config,
            docker,
            protection,
            console,
        }
    }
}

mod authorization;
mod backup;
mod console;
mod file;
mod responses;
mod self_test;
mod server;
mod transfer;

pub use authorization::authorize_master;
pub use backup::BackupService;
pub use console::ConsoleHub;
pub use file::FileService;
use responses::*;
pub use self_test::self_test_console;
pub use server::ServerService;
pub use transfer::TransferService;
