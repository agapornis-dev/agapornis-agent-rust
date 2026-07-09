use agapornis_agent::{
    certificate::CertificateManager,
    config::{DaemonConfig, load_dotenv},
    proto::{
        backup_management_server::BackupManagementServer,
        file_management_server::FileManagementServer, node_transfer_server::NodeTransferServer,
        server_management_server::ServerManagementServer,
    },
    runtime,
    services::{
        AppState, BackupService, FileService, ServerService, TransferService, authorize_master,
    },
    update::UpdateManager,
};
use anyhow::{Context, Result};
use std::{net::SocketAddr, time::Duration};
use tokio::signal;
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls ring crypto provider");
    load_dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("agapornis_agent=info,info")),
        )
        .init();
    let args: Vec<String> = std::env::args().collect();
    let updates = UpdateManager;
    if args
        .iter()
        .any(|value| value == "--activate-pending-update")
    {
        println!("{:?}", updates.activate_pending()?);
        return Ok(());
    }
    if args.iter().any(|value| value == "--rollback-update") {
        println!("{:?}", updates.rollback()?);
        return Ok(());
    }
    if args.iter().any(|v| v == "--self-test-backups") {
        return self_test_backups().await;
    }
    if args.iter().any(|v| v == "--self-test-console") {
        agapornis_agent::services::self_test_console().await?;
        return Ok(());
    }
    if args.iter().any(|v| v == "--self-test-disk-cache") {
        agapornis_agent::docker::self_test_disk_cache().await?;
        return Ok(());
    }
    let config = DaemonConfig::load_or_setup().await?;
    info!(node=%config.node_id,"agapornis Rust agent starting");
    let certificates = CertificateManager::new(config.clone());
    let state = AppState::new(config, certificates.clone()).await?;
    updates.schedule_health_commit();
    runtime::spawn(
        state.docker.clone(),
        state.protection.clone(),
        state.console.clone(),
    );
    let address: SocketAddr = "[::]:5001".parse()?;
    let mut reload = certificates.subscribe();
    loop {
        let tls = certificates
            .tls()
            .await
            .context("load mTLS certificate bundle")?;
        let server = Server::builder()
            .tls_config(tls)?
            .http2_keepalive_interval(Some(Duration::from_secs(30)))
            .http2_keepalive_timeout(Some(Duration::from_secs(10)))
            .add_service(NodeTransferServer::with_interceptor(
                TransferService(state.clone()),
                authorize_master,
            ))
            .add_service(ServerManagementServer::with_interceptor(
                ServerService(state.clone()),
                authorize_master,
            ))
            .add_service(FileManagementServer::with_interceptor(
                FileService(state.clone()),
                authorize_master,
            ))
            .add_service(BackupManagementServer::with_interceptor(
                BackupService(state.clone()),
                authorize_master,
            ));
        info!(%address,"gRPC listening with mTLS");
        let current = *reload.borrow();
        let shutdown = async {
            tokio::select! {_=signal::ctrl_c()=>{},result=reload.changed()=>{if result.is_ok()&&*reload.borrow()!=current{info!("certificate changed; reloading TLS listener")}}}
        };
        server.serve_with_shutdown(address, shutdown).await?;
        if *reload.borrow() == current {
            break;
        }
    }
    Ok(())
}

async fn self_test_backups() -> Result<()> {
    let key = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [7u8; 32]);
    unsafe { std::env::set_var("AGAPORNIS_BACKUP_ENCRYPTION_KEY", key) };
    agapornis_agent::backup::self_test().await?;
    println!("backup encryption and archive self-test: PASS");
    Ok(())
}
