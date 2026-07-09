//! Local and S3 backup creation, verification, encryption, and restore.

use crate::{paths, process};
use aes_gcm::{
    Aes256Gcm, KeyInit,
    aead::{Aead, Payload},
};
use anyhow::{Context, Result, bail};
use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_s3::{
    Client,
    primitives::ByteStream,
    types::{Delete, ObjectIdentifier, ServerSideEncryption},
};
use base64::Engine;
use chrono::Utc;
use rand::Rng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use subtle::ConstantTimeEq;
use tokio::{
    fs,
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    sync::{OwnedSemaphorePermit, Semaphore},
};
use uuid::Uuid;

const CHECKSUM: &str = "sha256";
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct BackupInfo {
    pub backup_id: String,
    pub server_id: String,
    pub archive_name: String,
    pub size_bytes: i64,
    pub created_at: String,
    pub checksum_sha256: String,
    pub checksum_type: String,
    pub storage: String,
    pub encrypted: bool,
    pub last_verified_at: Option<String>,
    pub expires_at: Option<String>,
}
#[derive(Clone)]
pub struct Backups {
    s3: Arc<S3Store>,
    operations: Arc<Semaphore>,
}

mod archive;
mod crypto;
mod manager;
mod s3;
mod self_test;

use archive::*;
use crypto::*;
use s3::S3Store;
pub use self_test::self_test;

#[cfg(test)]
mod tests;
