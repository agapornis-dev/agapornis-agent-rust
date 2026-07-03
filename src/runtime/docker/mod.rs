//! Docker lifecycle, resource accounting, stats, and console attachment.

use crate::{paths, process, protection::ProtectionState};
use anyhow::{Context, Result, bail};
use rand::Rng;
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    net::TcpListener,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    fs,
    io::AsyncWriteExt,
    process::{Child, ChildStdin, Command},
    sync::{Mutex, Notify},
};
use uuid::Uuid;

const DEFAULT_DISK_LIMIT: i64 = 10 * 1024 * 1024 * 1024;

enum CacheState {
    Ready(Instant, i64, i64),
    Calculating(Arc<Notify>),
}
type DiskCache = Arc<Mutex<HashMap<String, CacheState>>>;
type ConsoleBindings = Arc<Mutex<HashMap<String, Arc<Mutex<ConsoleBinding>>>>>;

struct ConsoleBinding {
    child: Child,
    stdin: ChildStdin,
}

#[derive(Clone)]
pub struct DockerManager {
    protection: Arc<ProtectionState>,
    disk_cache: DiskCache,
    console_bindings: ConsoleBindings,
    reserved_ports: Arc<Mutex<HashSet<u16>>>,
}

#[derive(Clone, Debug)]
pub struct CreateSpec {
    pub server_id: String,
    pub image: String,
    pub internal_port: String,
    pub env: Vec<String>,
    pub memory_bytes: i64,
    pub cpu_limit_percentage: i32,
    pub cpu_cores: f64,
    pub disk_limit_bytes: i64,
    pub startup_command: String,
    pub install_image: String,
    pub install_entrypoint: String,
    pub install_script: String,
    pub config_files_json: String,
    pub host_port: i32,
    pub network_owner_id: String,
    pub expose_public_port: bool,
}

pub struct DatabaseConnectionSpec<'a> {
    pub server_id: &'a str,
    pub database_type: &'a str,
    pub host: &'a str,
    pub port: i32,
    pub database_name: &'a str,
    pub username: &'a str,
    pub password: &'a str,
    pub docker_image: &'a str,
}

#[derive(Default, Debug)]
pub struct Metrics {
    pub memory_usage: i64,
    pub memory_limit: i64,
    pub cpu_percent: f64,
    pub network_read: i64,
    pub network_write: i64,
    pub disk_usage: i64,
    pub disk_limit: i64,
    pub status: String,
}

mod configuration;
mod console;
mod database;
mod inspection;
mod lifecycle;
mod provisioning;

use configuration::*;
use database::*;
pub use inspection::self_test_disk_cache;

#[cfg(test)]
mod tests;
