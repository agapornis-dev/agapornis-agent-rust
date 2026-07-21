use super::*;
use serde_json::json;

fn replacements(value: Value) -> Map<String, Value> {
    value.as_object().unwrap().clone()
}

#[test]
fn runtime_environment_gets_a_private_managed_xdg_directory() {
    let environment = runtime_environment(&["SERVER_PORT=25565".into(), "XDG_RUNTIME_DIR=".into()]);
    assert_eq!(
        environment,
        [
            "SERVER_PORT=25565",
            "XDG_RUNTIME_DIR=/tmp/agapornis-runtime"
        ]
    );

    let mut host_config = HostConfig::default();
    ensure_runtime_tmpfs(&mut host_config, &environment);
    assert!(runtime_tmpfs_ready(Some(&host_config), &environment));
    assert_eq!(
        host_config
            .tmpfs
            .as_ref()
            .and_then(|tmpfs| tmpfs.get("/tmp/agapornis-runtime"))
            .map(String::as_str),
        Some("rw,nosuid,nodev,noexec,size=16m,mode=0700,uid=999,gid=999")
    );
}

#[test]
fn explicit_xdg_runtime_directory_is_preserved() {
    let environment = runtime_environment(&["XDG_RUNTIME_DIR=/run/custom-runtime".into()]);
    assert_eq!(environment, ["XDG_RUNTIME_DIR=/run/custom-runtime"]);

    let mut host_config = HostConfig::default();
    ensure_runtime_tmpfs(&mut host_config, &environment);
    assert_eq!(host_config.tmpfs, None);
    assert!(runtime_tmpfs_ready(Some(&host_config), &environment));
}

#[test]
fn legacy_container_configuration_is_marked_for_runtime_repair() {
    let legacy = json!({
        "Config": {"Env": ["SERVER_PORT=25565"]},
        "HostConfig": {"Tmpfs": {}}
    });
    assert!(!runtime_configuration_ready(&legacy));

    let managed = json!({
        "Config": {"Env": ["XDG_RUNTIME_DIR=/tmp/agapornis-runtime"]},
        "HostConfig": {
            "Tmpfs": {
                "/tmp/agapornis-runtime":
                    "rw,nosuid,nodev,noexec,size=16m,mode=0700,uid=999,gid=999"
            }
        }
    });
    assert!(runtime_configuration_ready(&managed));
}

#[tokio::test]
async fn startup_validation_waits_for_a_delayed_installer_target() {
    let root = std::env::temp_dir().join(format!("agapornis-startup-test-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).await.unwrap();
    fs::create_dir_all(root.join("bin")).await.unwrap();
    let executable = root.join("bin/dedicated-server");
    let delayed_executable = executable.clone();

    let writer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        fs::write(delayed_executable, b"test executable")
            .await
            .unwrap();
    });

    validate_startup(&root, "compat-runtime ./bin/dedicated-server --port 25565")
        .await
        .unwrap();
    writer.await.unwrap();
    fs::remove_dir_all(root).await.unwrap();
}

#[test]
fn file_parser_matches_line_prefix_only() {
    let output = apply_file_parser(
        b"port=1\nother-port=1\n",
        &replacements(json!({"port": "port=25565"})),
    );
    assert_eq!(
        String::from_utf8(output).unwrap(),
        "port=25565\nother-port=1\n"
    );
}

#[test]
fn properties_and_ini_update_exact_keys() {
    let properties = apply_properties_parser(
        b"# generated\nserver-port = 1\nmotd=hello\n",
        &replacements(json!({
            "server-port": "25565",
            "enable-query": "true"
        })),
    );
    assert_eq!(
        String::from_utf8(properties).unwrap(),
        "# generated\nserver-port=25565\nmotd=hello\nenable-query=true\n"
    );

    let ini = apply_ini_parser(
        b"root=yes\n[network]\nport=1\n",
        &replacements(json!({
            "network.port": 25565,
            "network.host": "0.0.0.0"
        })),
    );
    let ini = String::from_utf8(ini).unwrap();
    assert!(ini.contains("[network]\nport=25565"));
    assert!(ini.contains("host=0.0.0.0"));
    assert_eq!(ini.matches("[network]").count(), 1);
}

#[test]
fn structured_paths_support_arrays_wildcards_and_value_maps() {
    let mut document = json!({
        "listeners": [{"query_enabled": false, "query_port": 1}],
        "servers": {
            "lobby": {"address": "127.0.0.1"},
            "game": {"address": "localhost"}
        }
    });
    apply_structured_replacements(
        &mut document,
        &replacements(json!({
            "listeners[0].query_enabled": true,
            "listeners[0].query_port": "25565",
            "servers.*.address": {
                "127.0.0.1": "172.18.0.1",
                "localhost": "172.18.0.1"
            }
        })),
    );

    assert_eq!(
        document.pointer("/listeners/0/query_enabled"),
        Some(&json!(true))
    );
    assert_eq!(
        document.pointer("/listeners/0/query_port"),
        Some(&json!(25565))
    );
    assert_eq!(
        document.pointer("/servers/lobby/address"),
        Some(&json!("172.18.0.1"))
    );
    assert_eq!(
        document.pointer("/servers/game/address"),
        Some(&json!("172.18.0.1"))
    );
}

#[test]
fn json_yaml_and_xml_parsers_write_valid_documents() {
    let find = replacements(json!({
        "listeners[0].port": "25565"
    }));
    let json_output = apply_json_parser(br#"{"listeners":[{"port":1}]}"#, &find).unwrap();
    let json_document: Value = serde_json::from_slice(&json_output).unwrap();
    assert_eq!(
        json_document.pointer("/listeners/0/port"),
        Some(&json!(25565))
    );

    let yaml_output = apply_yaml_parser(b"listeners:\n  - port: 1\n", &find).unwrap();
    let yaml_document: Value = serde_yaml::from_slice(&yaml_output).unwrap();
    assert_eq!(
        yaml_document.pointer("/listeners/0/port"),
        Some(&json!(25565))
    );

    let xml_output = apply_xml_parser(
        b"<configuration><server><port>1</port></server></configuration>",
        &replacements(json!({
            "configuration.server.port": "25565",
            "configuration.server.bind": "[address='0.0.0.0']"
        })),
    )
    .unwrap();
    let xml = String::from_utf8(xml_output).unwrap();
    assert!(xml.contains("<port>25565</port>"));
    assert!(xml.contains(r#"<bind address="0.0.0.0" />"#));
}

#[tokio::test]
async fn descriptor_applies_multiple_files() {
    let root = std::env::temp_dir().join(format!("agapornis-config-test-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).await.unwrap();
    fs::write(root.join("server.properties"), "server-port=1\n")
        .await
        .unwrap();
    fs::write(root.join("config.json"), r#"{"Server":{"ListenPort":1}}"#)
        .await
        .unwrap();

    let descriptor = json!({
        "server.properties": {
            "parser": "properties",
            "find": {"server-port": "25565"}
        },
        "config.json": {
            "parser": "json",
            "find": {
                "Server.ListenPort": "25565",
                "Server.Host": "{{config.docker.interface}}"
            }
        }
    });
    apply_config_files(&root, &descriptor.to_string(), "172.18.0.1")
        .await
        .unwrap();

    assert_eq!(
        fs::read_to_string(root.join("server.properties"))
            .await
            .unwrap(),
        "server-port=25565\n"
    );
    let json: Value =
        serde_json::from_slice(&fs::read(root.join("config.json")).await.unwrap()).unwrap();
    assert_eq!(json.pointer("/Server/ListenPort"), Some(&json!(25565)));
    assert_eq!(json.pointer("/Server/Host"), Some(&json!("172.18.0.1")));

    fs::remove_dir_all(root).await.unwrap();
}
