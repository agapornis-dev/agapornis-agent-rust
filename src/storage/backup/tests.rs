use super::*;
#[test]
fn backup_ids_reject_traversal() {
    assert!(validate_backup_id("../../etc").is_err());
    assert!(validate_backup_id("valid-2026").is_ok());
}

#[tokio::test]
async fn local_backup_verifies_and_restores_exact_snapshot() {
    let root =
        std::env::temp_dir().join(format!("agapornis-backup-integration-{}", Uuid::new_v4()));
    let servers = root.join("servers");
    let backups = root.join("backups");
    unsafe {
        std::env::set_var("AGAPORNIS_SERVERS_DIR", &servers);
        std::env::set_var("AGAPORNIS_BACKUPS_DIR", &backups);
    }
    let server = servers.join("server-one");
    fs::create_dir_all(&server).await.unwrap();
    fs::write(server.join("world.txt"), b"original")
        .await
        .unwrap();
    let manager = Backups::new().await;
    let created = manager
        .create("server-one", "local", 0, false)
        .await
        .unwrap();
    manager
        .verify("server-one", &created.backup_id, "local")
        .await
        .unwrap();
    fs::write(server.join("world.txt"), b"changed")
        .await
        .unwrap();
    fs::write(server.join("stale.txt"), b"must disappear")
        .await
        .unwrap();
    manager
        .restore(
            "server-one",
            &created.backup_id,
            "local",
            Some(&created.checksum_sha256),
        )
        .await
        .unwrap();
    assert_eq!(
        fs::read(server.join("world.txt")).await.unwrap(),
        b"original"
    );
    assert!(!server.join("stale.txt").exists());
    let listed = manager.list("server-one", false).await.unwrap();
    assert!(listed[0].last_verified_at.is_some());
    let _ = fs::remove_dir_all(root).await;
}
