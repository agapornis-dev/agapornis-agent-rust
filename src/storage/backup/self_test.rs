use super::*;

pub async fn self_test() -> Result<()> {
    let root = std::env::temp_dir().join(format!("agapornis-backup-selftest-{}", Uuid::new_v4()));
    let source = root.join("source");
    let restored = root.join("restored");
    fs::create_dir_all(source.join("nested")).await?;
    fs::create_dir_all(&restored).await?;
    fs::write(source.join("nested/test.txt"), b"agapornis backup parity").await?;
    let archive = root.join("test.tar.gz");
    let encrypted = root.join("test.tar.gz.agp");
    let decrypted = root.join("decrypted.tar.gz");
    tar_create(&source, &archive).await?;
    validate_archive(&archive).await?;
    encrypt_file(&archive, &encrypted).await?;
    decrypt_file(&encrypted, &decrypted).await?;
    if !hash_eq(&sha256(&archive).await?, &sha256(&decrypted).await?) {
        bail!("backup encryption round trip changed the archive")
    }
    tar_extract(&decrypted, &restored).await?;
    if fs::read(restored.join("nested/test.txt")).await? != b"agapornis backup parity" {
        bail!("backup archive round trip changed file content")
    }
    let _ = fs::remove_dir_all(root).await;
    Ok(())
}
