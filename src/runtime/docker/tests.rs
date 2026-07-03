use super::*;
#[test]
fn parses_docker_sizes() {
    assert_eq!(inspection::parse_size("1KiB"), 1024);
    assert_eq!(inspection::parse_size("1.5MiB"), 1_572_864);
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
    let args = database::port_arguments(&port, false, 0);
    assert_eq!(args, ["--expose", "33062/tcp"]);
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
