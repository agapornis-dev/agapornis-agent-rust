use super::*;

pub(super) struct S3Store {
    client: Option<Client>,
    bucket: String,
    prefix: String,
}
impl S3Store {
    pub(super) async fn new() -> Self {
        let bucket = std::env::var("S3_BUCKET").unwrap_or_default();
        let access = std::env::var("S3_ACCESS_KEY_ID").unwrap_or_default();
        let secret = std::env::var("S3_SECRET_ACCESS_KEY").unwrap_or_default();
        if bucket.is_empty() || access.is_empty() || secret.is_empty() {
            return Self {
                client: None,
                bucket,
                prefix: "agapornis".into(),
            };
        }
        let region = std::env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".into());
        let credentials = Credentials::new(access, secret, None, None, "agapornis-static");
        let mut loader = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(region))
            .credentials_provider(credentials);
        if let Ok(endpoint) = std::env::var("S3_ENDPOINT")
            && !endpoint.trim().is_empty()
        {
            loader = loader.endpoint_url(endpoint)
        }
        let shared = loader.load().await;
        let conf = aws_sdk_s3::config::Builder::from(&shared)
            .force_path_style(
                std::env::var("S3_FORCE_PATH_STYLE")
                    .map(|v| v != "false")
                    .unwrap_or(true),
            )
            .build();
        Self {
            client: Some(Client::from_conf(conf)),
            bucket,
            prefix: std::env::var("S3_PREFIX")
                .unwrap_or_else(|_| "agapornis".into())
                .trim_matches('/')
                .into(),
        }
    }
    pub(super) fn configured(&self) -> bool {
        self.client.is_some() && !self.bucket.is_empty()
    }
    pub(super) fn prefix(&self, id: &str) -> String {
        format!("{}/servers/{id}/backups/", self.prefix)
    }
    pub(super) fn archive(&self, id: &str, bid: &str, encrypted: bool) -> String {
        format!(
            "{}{bid}.tar.gz{}",
            self.prefix(id),
            if encrypted { ".agp" } else { "" }
        )
    }
    pub(super) fn metadata(&self, id: &str, bid: &str) -> String {
        format!("{}{bid}.metadata.json", self.prefix(id))
    }
    pub(super) fn client(&self) -> Result<&Client> {
        self.client
            .as_ref()
            .context("S3 storage is not configured on this agent.")
    }
    pub(super) async fn upload(
        &self,
        id: &str,
        bid: &str,
        path: &Path,
        info: &BackupInfo,
        encrypted: bool,
    ) -> Result<()> {
        self.client()?
            .put_object()
            .bucket(&self.bucket)
            .key(self.archive(id, bid, encrypted))
            .body(ByteStream::from_path(path).await?)
            .server_side_encryption(ServerSideEncryption::Aes256)
            .send()
            .await?;
        self.put_metadata(id, bid, info).await
    }
    pub(super) async fn put_metadata(&self, id: &str, bid: &str, info: &BackupInfo) -> Result<()> {
        self.client()?
            .put_object()
            .bucket(&self.bucket)
            .key(self.metadata(id, bid))
            .body(ByteStream::from(serde_json::to_vec(info)?))
            .content_type("application/json")
            .server_side_encryption(ServerSideEncryption::Aes256)
            .send()
            .await?;
        Ok(())
    }
    pub(super) async fn list(&self, id: &str) -> Result<Vec<BackupInfo>> {
        let mut out = vec![];
        let result = self
            .client()?
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(self.prefix(id))
            .send()
            .await?;
        for obj in result.contents() {
            if let Some(key) = obj.key()
                && key.ends_with(".metadata.json")
            {
                let data = self
                    .client()?
                    .get_object()
                    .bucket(&self.bucket)
                    .key(key)
                    .send()
                    .await?
                    .body
                    .collect()
                    .await?
                    .into_bytes();
                if let Ok(info) = serde_json::from_slice(&data) {
                    out.push(info)
                }
            }
        }
        Ok(out)
    }
    pub(super) async fn download(
        &self,
        id: &str,
        bid: &str,
        encrypted: bool,
        path: &Path,
    ) -> Result<()> {
        let data = self
            .client()?
            .get_object()
            .bucket(&self.bucket)
            .key(self.archive(id, bid, encrypted))
            .send()
            .await?
            .body
            .collect()
            .await?
            .into_bytes();
        fs::write(path, data).await?;
        Ok(())
    }
    pub(super) async fn delete(&self, id: &str, bid: &str, encrypted: bool) -> Result<()> {
        let objects = [self.archive(id, bid, encrypted), self.metadata(id, bid)]
            .into_iter()
            .map(|key| ObjectIdentifier::builder().key(key).build())
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let delete = Delete::builder().set_objects(Some(objects)).build()?;
        self.client()?
            .delete_objects()
            .bucket(&self.bucket)
            .delete(delete)
            .send()
            .await?;
        Ok(())
    }
}
