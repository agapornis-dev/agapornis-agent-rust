use super::*;

pub(super) fn action(result: anyhow::Result<()>) -> ServerActionResponse {
    match result {
        Ok(()) => ServerActionResponse {
            success: true,
            error_message: "".into(),
        },
        Err(e) => ServerActionResponse {
            success: false,
            error_message: e.to_string(),
        },
    }
}
pub(super) fn failure(message: &str) -> ServerActionResponse {
    ServerActionResponse {
        success: false,
        error_message: message.into(),
    }
}
pub(super) fn file_action(result: anyhow::Result<()>) -> FileActionResponse {
    match result {
        Ok(()) => FileActionResponse {
            success: true,
            error_message: "".into(),
        },
        Err(e) => FileActionResponse {
            success: false,
            error_message: e.to_string(),
        },
    }
}
pub(super) fn backup_action(result: anyhow::Result<()>) -> BackupActionResponse {
    match result {
        Ok(()) => BackupActionResponse {
            success: true,
            error_message: "".into(),
        },
        Err(e) => BackupActionResponse {
            success: false,
            error_message: e.to_string(),
        },
    }
}
pub(super) fn internal(e: impl std::fmt::Display) -> Status {
    Status::internal(e.to_string())
}
pub(super) fn storage(value: &str) -> &str {
    if value.eq_ignore_ascii_case("s3") {
        "s3"
    } else {
        "local"
    }
}
pub(super) fn backup_entry(v: BackupInfo) -> BackupEntry {
    BackupEntry {
        backup_id: v.backup_id,
        size_bytes: v.size_bytes,
        created_at: v.created_at,
        checksum_sha256: v.checksum_sha256,
        checksum_type: v.checksum_type,
        storage: v.storage,
        encrypted: v.encrypted,
        last_verified_at: v.last_verified_at.unwrap_or_default(),
        expires_at: v.expires_at.unwrap_or_default(),
    }
}
