use super::*;
use bollard::{errors::Error as BollardError, models::ContainerWaitResponse};

#[test]
fn docker_connection_retry_interval_is_thirty_seconds() {
    assert_eq!(DOCKER_CONNECT_RETRY_INTERVAL, Duration::from_secs(30));
}

#[test]
fn treats_nonzero_installer_wait_as_process_exit() {
    let (code, error) =
        provisioning::installer_exit_status(Err(BollardError::DockerContainerWaitError {
            error: "script failed".into(),
            code: 17,
        }))
        .unwrap();

    assert_eq!(code, 17);
    assert_eq!(error.as_deref(), Some("script failed"));
}

#[test]
fn accepts_successful_installer_wait() {
    let (code, error) = provisioning::installer_exit_status(Ok(ContainerWaitResponse {
        status_code: 0,
        error: None,
    }))
    .unwrap();

    assert_eq!(code, 0);
    assert_eq!(error, None);
}

#[test]
fn installer_log_keeps_only_the_tail() {
    let mut output = b"1234".to_vec();
    provisioning::append_tail(&mut output, b"567890", 6);
    assert_eq!(output, b"567890");
}

#[test]
fn recognizes_only_missing_docker_desktop_bind_mounts_for_self_repair() {
    let stale = anyhow::anyhow!(
        "error mounting /run/desktop/mnt/host/wsl/docker-desktop-bind-mounts/Ubuntu/hash to /data: no such file or directory"
    );
    assert!(super::lifecycle::stale_docker_desktop_bind_error(&stale));

    let unrelated = anyhow::anyhow!("start failed: no such file or directory");
    assert!(!super::lifecycle::stale_docker_desktop_bind_error(
        &unrelated
    ));
}

#[test]
fn exec_output_stops_at_its_capture_budget() {
    let mut output = Vec::new();
    assert!(console::append_exec_output(&mut output, b"abcdef", 4));
    assert_eq!(output, b"abcd");
}

#[tokio::test]
async fn temporary_port_reservation_releases_on_drop() {
    let ports = Arc::new(Mutex::new(HashSet::from([25565])));
    {
        let _reservation = provisioning::PortReservation::new(ports.clone(), Some(25565));
    }
    assert!(!ports.lock().await.contains(&25565));
}

#[test]
fn prefers_explicit_cpu_cores() {
    assert_eq!(configuration::effective_cpus(50, 2.0), 2.0);
    assert_eq!(configuration::effective_cpus(50, 0.0), 0.5);
}
#[test]
fn private_database_port_is_exposed_without_host_publish() {
    let port = database::effective_internal_port("", &["AGAPORNIS_DATABASE_PORT=33062".into()])
        .unwrap()
        .unwrap();
    assert_eq!(port, "33062/tcp");
    assert_eq!(
        database::database_port(&["AGAPORNIS_DATABASE_PORT=33062".into()]),
        Some(33062)
    );
}
#[test]
fn validates_and_normalizes_internal_ports() {
    assert_eq!(
        database::effective_internal_port("25565", &[]).unwrap(),
        Some("25565/tcp".into())
    );
    assert!(database::effective_internal_port("not-a-port", &[]).is_err());
}
