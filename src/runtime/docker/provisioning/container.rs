use super::*;

use bollard::models::{
    ContainerCreateBody, EndpointSettings, HealthConfig, HostConfig, Mount, MountType,
    NetworkingConfig, PortBinding, RestartPolicy, RestartPolicyNameEnum,
};

struct ServerPorts {
    exposed: Vec<String>,
    bindings: HashMap<String, Option<Vec<PortBinding>>>,
}

pub(super) fn build_container(
    spec: &CreateSpec,
    host: &Path,
    network: &str,
    data_path: String,
    host_port: i32,
) -> Result<ContainerCreateBody> {
    let mounts = server_mounts(host, &data_path);
    let labels = server_labels(spec, network, &data_path);
    let ports = server_ports(spec, host_port)?;
    let cpus = effective_cpus(spec.cpu_limit_percentage, spec.cpu_cores);

    let healthcheck = database_health_command(&spec.env).map(|command| HealthConfig {
        test: Some(vec!["CMD-SHELL".into(), command]),
        interval: Some(10_000_000_000),
        timeout: Some(5_000_000_000),
        start_period: Some(30_000_000_000),
        retries: Some(5),
        ..Default::default()
    });
    let host_config = HostConfig {
        mounts: Some(mounts),
        network_mode: Some(network.to_owned()),
        pids_limit: Some(512),
        restart_policy: Some(RestartPolicy {
            name: Some(RestartPolicyNameEnum::ON_FAILURE),
            maximum_retry_count: Some(2),
        }),
        security_opt: Some(vec!["no-new-privileges".into()]),
        memory: (spec.memory_bytes > 0).then_some(spec.memory_bytes),
        memory_swap: (spec.memory_bytes > 0).then_some(spec.memory_bytes),
        nano_cpus: (cpus > 0.0).then_some((cpus * 1_000_000_000.0).round() as i64),
        port_bindings: (!ports.bindings.is_empty()).then_some(ports.bindings),
        ..Default::default()
    };

    let mut endpoint_configs = HashMap::new();
    endpoint_configs.insert(
        network.to_owned(),
        EndpointSettings {
            aliases: Some(vec![spec.server_id.clone()]),
            ..Default::default()
        },
    );

    Ok(ContainerCreateBody {
        image: Some(spec.image.clone()),
        user: Some("999:999".into()),
        working_dir: Some(data_path),
        // Docker keeps the container's stdin open between agent attachments.
        open_stdin: Some(true),
        stdin_once: Some(false),
        tty: Some(true),
        attach_stdin: Some(true),
        attach_stdout: Some(true),
        attach_stderr: Some(true),
        env: (!spec.env.is_empty()).then_some(spec.env.clone()),
        cmd: server_command(spec),
        healthcheck,
        labels: Some(labels),
        exposed_ports: (!ports.exposed.is_empty()).then_some(ports.exposed),
        host_config: Some(host_config),
        networking_config: Some(NetworkingConfig {
            endpoints_config: Some(endpoint_configs),
        }),
        ..Default::default()
    })
}

fn server_mounts(host: &Path, data_path: &str) -> Vec<Mount> {
    let host_source = host.to_string_lossy().into_owned();
    let mut targets = vec![
        data_path.to_owned(),
        paths::HOME_CONTAINER_PATH.into(),
        paths::DATA_CONTAINER_PATH.into(),
    ];
    targets.sort();
    targets.dedup();
    targets
        .into_iter()
        .map(|target| Mount {
            target: Some(target),
            source: Some(host_source.clone()),
            typ: Some(MountType::BIND),
            read_only: Some(false),
            ..Default::default()
        })
        .collect()
}

fn server_labels(spec: &CreateSpec, network: &str, data_path: &str) -> HashMap<String, String> {
    let mut labels = HashMap::from([
        ("agapornis.server_id".into(), spec.server_id.clone()),
        (
            "agapornis.disk_limit_bytes".into(),
            spec.disk_limit_bytes.to_string(),
        ),
        ("agapornis.cpu_cores".into(), spec.cpu_cores.to_string()),
        (
            "agapornis.cpu_limit_percentage".into(),
            spec.cpu_limit_percentage.to_string(),
        ),
        ("agapornis.data_path".into(), data_path.to_owned()),
        ("agapornis.network".into(), network.to_owned()),
        ("agapornis.stop_command".into(), spec.stop_command.clone()),
        ("agapornis.startup_done".into(), spec.startup_done.clone()),
    ]);
    if !spec.network_owner_id.trim().is_empty() {
        labels.insert(
            "agapornis.network_owner_id".into(),
            spec.network_owner_id.clone(),
        );
    }
    labels
}

fn server_ports(spec: &CreateSpec, host_port: i32) -> Result<ServerPorts> {
    let mut exposed_ports = Vec::new();
    let mut port_bindings = HashMap::new();
    if spec.expose_public_port && !spec.port_mappings.is_empty() {
        for (internal_port, mapped_host_port) in &spec.port_mappings {
            add_port_mapping(
                &mut exposed_ports,
                &mut port_bindings,
                internal_port,
                Some(*mapped_host_port),
            )?;
        }
    } else if let Some(internal_port) = effective_internal_port(&spec.internal_port, &spec.env)? {
        add_port_mapping(
            &mut exposed_ports,
            &mut port_bindings,
            &internal_port,
            spec.expose_public_port.then_some(host_port),
        )?;
    }
    Ok(ServerPorts {
        exposed: exposed_ports,
        bindings: port_bindings,
    })
}

fn server_command(spec: &CreateSpec) -> Option<Vec<String>> {
    if let Some(db_port) = database_port(&spec.env) {
        Some(vec![format!("--port={db_port}")])
    } else if !spec.startup_command.trim().is_empty() {
        Some(vec![
            "/bin/sh".into(),
            "-lc".into(),
            format!("exec {}", spec.startup_command),
        ])
    } else {
        None
    }
}
