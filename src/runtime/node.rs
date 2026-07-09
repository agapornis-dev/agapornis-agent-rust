//! Host telemetry and optional CrowdSec observations.

use crate::{
    config::DaemonConfig,
    process,
    proto::{CrowdSecAlert, CrowdSecAlertsResponse},
};
use anyhow::Result;
use tokio::fs;

#[path = "node/crowdsec.rs"]
mod crowdsec;
#[path = "node/linux.rs"]
mod linux;

pub use crowdsec::crowdsec;
pub use linux::stats;

pub struct NodeStats {
    pub cpu: f64,
    pub memory_used: i64,
    pub memory_total: i64,
    pub disk_used: i64,
    pub disk_total: i64,
    pub uptime: i64,
    pub cpus: i32,
}
