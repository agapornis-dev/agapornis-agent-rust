use super::*;

use bollard::models::PortBinding;
use rand::RngExt;

pub(crate) struct PortReservation {
    ports: Arc<Mutex<HashSet<u16>>>,
    port: Option<u16>,
}

impl PortReservation {
    pub(crate) fn new(ports: Arc<Mutex<HashSet<u16>>>, port: Option<u16>) -> Self {
        Self { ports, port }
    }
}

impl Drop for PortReservation {
    fn drop(&mut self) {
        let Some(port) = self.port.take() else {
            return;
        };
        if let Ok(mut ports) = self.ports.try_lock() {
            ports.remove(&port);
            return;
        }
        let ports = self.ports.clone();
        tokio::spawn(async move {
            ports.lock().await.remove(&port);
        });
    }
}

impl DockerManager {
    pub(super) async fn reserve_host_port(
        &self,
        spec: &CreateSpec,
    ) -> Result<(i32, PortReservation)> {
        let host_port = if !spec.expose_public_port {
            0
        } else if spec.host_port > 0 {
            let port =
                u16::try_from(spec.host_port).context("host port is outside the valid range")?;
            ensure_port(port)?;
            if !self.reserved_ports.lock().await.insert(port) {
                bail!("Requested host port {port} is already being allocated.")
            }
            spec.host_port
        } else {
            i32::from(self.find_port().await?)
        };

        let reservation = PortReservation::new(
            self.reserved_ports.clone(),
            u16::try_from(host_port).ok().filter(|port| *port > 0),
        );
        Ok((host_port, reservation))
    }

    async fn find_port(&self) -> Result<u16> {
        for _ in 0..50 {
            // ThreadRng is not Send, so keep it out of the state captured
            // across the asynchronous bind operation.
            let port = rand::rng().random_range(25000..26000);

            if tokio::net::TcpListener::bind(("0.0.0.0", port))
                .await
                .is_ok()
            {
                let mut reserved = self.reserved_ports.lock().await;
                if !reserved.contains(&port) {
                    reserved.insert(port);
                    return Ok(port);
                }
            }
        }

        bail!("No open ports found.")
    }
}

pub(super) fn add_port_mapping(
    exposed_ports: &mut Vec<String>,
    port_bindings: &mut HashMap<String, Option<Vec<PortBinding>>>,
    internal_port: &str,
    host_port: Option<i32>,
) -> Result<()> {
    let port_key = normalize_container_port(internal_port)?;
    if !exposed_ports.contains(&port_key) {
        exposed_ports.push(port_key.clone());
    }

    let Some(host_port) = host_port else {
        return Ok(());
    };
    let host_port =
        u16::try_from(host_port).context("mapped host port is outside the valid range")?;
    ensure_port(host_port)?;

    port_bindings
        .entry(port_key)
        .or_insert_with(|| Some(Vec::new()))
        .get_or_insert_with(Vec::new)
        .push(PortBinding {
            host_ip: Some("0.0.0.0".into()),
            host_port: Some(host_port.to_string()),
        });
    Ok(())
}

fn normalize_container_port(port: &str) -> Result<String> {
    let port = port.trim();
    if port.is_empty() {
        bail!("container port cannot be empty");
    }

    let (number, protocol) = match port.rsplit_once('/') {
        Some((number, protocol)) => (number, protocol),
        None => (port, "tcp"),
    };
    let number = number
        .parse::<u16>()
        .with_context(|| format!("invalid container port: {port}"))?;
    let protocol = protocol.to_ascii_lowercase();
    if !matches!(protocol.as_str(), "tcp" | "udp" | "sctp") {
        bail!("unsupported port protocol: {protocol}");
    }
    Ok(format!("{number}/{protocol}"))
}
