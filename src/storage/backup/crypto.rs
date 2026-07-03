use super::*;

pub(super) fn encryption_key() -> Result<[u8; 32]> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(std::env::var("AGAPORNIS_BACKUP_ENCRYPTION_KEY").unwrap_or_default())
        .context("AGAPORNIS_BACKUP_ENCRYPTION_KEY must be a base64 encoded 32-byte key.")?;
    bytes.try_into().map_err(|_| {
        anyhow::anyhow!("AGAPORNIS_BACKUP_ENCRYPTION_KEY must be a base64 encoded 32-byte key.")
    })
}
pub(super) async fn encrypt_file(input: &Path, output: &Path) -> Result<()> {
    let key = encryption_key()?;
    let cipher = Aes256Gcm::new_from_slice(&key).unwrap();
    let mut prefix = [0u8; 8];
    rand::rng().fill_bytes(&mut prefix);
    let mut src = fs::File::open(input).await?;
    let mut dst = fs::File::create(output).await?;
    dst.write_all(b"AGPBK01\0").await?;
    dst.write_all(&prefix).await?;
    let mut counter = 0u32;
    let mut buf = vec![0; 4 * 1024 * 1024];
    loop {
        let n = src.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        let mut nonce = [0u8; 12];
        nonce[..8].copy_from_slice(&prefix);
        nonce[8..].copy_from_slice(&counter.to_be_bytes());
        let encrypted = cipher
            .encrypt(
                (&nonce).into(),
                Payload {
                    msg: &buf[..n],
                    aad: &counter.to_be_bytes(),
                },
            )
            .map_err(|_| anyhow::anyhow!("backup encryption failed"))?;
        dst.write_all(&(n as u32).to_be_bytes()).await?;
        dst.write_all(&encrypted).await?;
        counter += 1;
    }
    dst.write_all(&0u32.to_be_bytes()).await?;
    Ok(())
}
pub(super) async fn decrypt_file(input: &Path, output: &Path) -> Result<()> {
    let key = encryption_key()?;
    let cipher = Aes256Gcm::new_from_slice(&key).unwrap();
    let mut src = fs::File::open(input).await?;
    let mut magic = [0u8; 8];
    src.read_exact(&mut magic).await?;
    if &magic != b"AGPBK01\0" {
        bail!("Encrypted backup header is invalid.")
    }
    let mut prefix = [0u8; 8];
    src.read_exact(&mut prefix).await?;
    let mut dst = fs::File::create(output).await?;
    let mut counter = 0u32;
    loop {
        let mut length = [0u8; 4];
        src.read_exact(&mut length).await?;
        let n = u32::from_be_bytes(length) as usize;
        if n == 0 {
            break;
        }
        if n > 4 * 1024 * 1024 {
            bail!("Encrypted backup chunk is invalid.")
        }
        let mut data = vec![0; n + 16];
        src.read_exact(&mut data).await?;
        let mut nonce = [0u8; 12];
        nonce[..8].copy_from_slice(&prefix);
        nonce[8..].copy_from_slice(&counter.to_be_bytes());
        let plain = cipher
            .decrypt(
                (&nonce).into(),
                Payload {
                    msg: &data,
                    aad: &counter.to_be_bytes(),
                },
            )
            .map_err(|_| anyhow::anyhow!("encrypted backup authentication failed"))?;
        dst.write_all(&plain).await?;
        counter += 1;
    }
    Ok(())
}
