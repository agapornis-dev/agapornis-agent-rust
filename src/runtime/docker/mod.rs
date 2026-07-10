//! Docker lifecycle, resource accounting, stats, and console attachment.
//!
//! On Linux, Docker is the agent's isolation foundation. The agent describes
//! containers through the Engine API; the Docker daemon then creates Linux
//! namespaces for process/network isolation, cgroups for CPU and memory
//! accounting, and bind mounts for persistent server data. Agapornis keeps
//! that data on the host and recreates containers around it, so a container is
//! disposable while its server directory remains durable. The agent reads
//! Docker's cgroup-derived statistics rather than manipulating cgroups
//! directly.

use crate::{paths, protection::ProtectionState};
use anyhow::{Context, Result, bail};
use bollard::Docker;
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    net::TcpListener,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    fs,
    io::{AsyncWrite, AsyncWriteExt},
    sync::{Mutex, Notify, Semaphore},
    task::JoinHandle,
};
use uuid::Uuid;

const DEFAULT_DISK_LIMIT: i64 = 10 * 1024 * 1024 * 1024;
const DOCKER_CONNECT_RETRY_INTERVAL: Duration = Duration::from_secs(30);

enum CacheState {
    Ready(Instant, i64, i64),
    Calculating(Arc<Notify>),
}

type DiskCache = Arc<Mutex<HashMap<String, CacheState>>>;

type ConsoleBindings = Arc<Mutex<HashMap<String, Arc<Mutex<ConsoleBinding>>>>>;
type ProvisioningReporter = Arc<dyn Fn(&str, i32, &str) + Send + Sync>;

struct ConsoleBinding {
    stdin: Pin<Box<dyn AsyncWrite + Send>>,
    output_task: JoinHandle<()>,
}

impl Drop for ConsoleBinding {
    fn drop(&mut self) {
        self.output_task.abort();
    }
}

#[derive(Clone)]
pub struct DockerManager {
    docker: Docker,
    protection: Arc<ProtectionState>,
    disk_cache: DiskCache,
    console_bindings: ConsoleBindings,
    reserved_ports: Arc<Mutex<HashSet<u16>>>,
    startup_ready: Arc<Mutex<HashSet<String>>>,
    startup_checks: Arc<Mutex<HashMap<String, (Instant, i32)>>>,
    disk_scans: Arc<Semaphore>,
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
    pub stop_command: String,
    pub startup_done: String,
    pub install_image: String,
    pub install_entrypoint: String,
    pub install_script: String,
    pub config_files_json: String,
    pub host_port: i32,
    pub network_owner_id: String,
    pub expose_public_port: bool,
    pub port_mappings: Vec<(String, i32)>,
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
    pub uptime_seconds: i64,
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
