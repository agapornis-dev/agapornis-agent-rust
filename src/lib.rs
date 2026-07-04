#![allow(clippy::result_large_err)]

// Keep these public module names stable for downstream users while grouping the
// implementation by responsibility on disk.
#[path = "storage/backup/mod.rs"]
pub mod backup;

#[path = "security/certificate.rs"]
pub mod certificate;

#[path = "core/config.rs"]
pub mod config;

#[path = "runtime/docker/mod.rs"]
pub mod docker;

#[path = "storage/files.rs"]
pub mod file_service;

#[path = "runtime/node.rs"]
pub mod node;

#[path = "core/paths.rs"]
pub mod paths;

#[path = "core/process.rs"]
pub mod process;

#[path = "security/protection.rs"]
pub mod protection;

#[path = "runtime/supervisor.rs"]
pub mod runtime;

#[path = "grpc/services/mod.rs"]
pub mod services;

#[path = "runtime/update.rs"]
pub mod update;

pub mod proto {
    tonic::include_proto!("agapornis.v1");
}