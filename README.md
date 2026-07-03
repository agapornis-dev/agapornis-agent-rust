# Agapornis Agent (Rust)

Rust replacement for the Agapornis node agent. It implements the existing `agapornis.v1` gRPC contract and manages Docker servers, files, live consoles, backups, node transfers, telemetry, certificate rotation, and staged agent updates.

## Codebase guide

Implementation is grouped by responsibility:

- `src/core/` — configuration, paths, and process execution
- `src/security/` — certificates and runtime protection
- `src/runtime/` — Docker, telemetry, supervision, and updates; Docker operations are split under `runtime/docker/`
- `src/storage/` — files and backups; archive, crypto, S3, and orchestration are split under `storage/backup/`
- `src/grpc/` — protobuf adapters and shared application state; each service lives under `grpc/services/`

See [Architecture](docs/ARCHITECTURE.md) for request flow and invariants, and [Contributing](CONTRIBUTING.md) for the development checklist.

## Compatibility

The protobuf package, four services, RPC names, message fields, and field numbers match the original agent:

- `ServerManagement`
- `FileManagement`
- `BackupManagement`
- `NodeTransfer`

The master therefore does not need a protocol migration when switching to this binary.

## Build

The build uses a vendored `protoc`; a system protobuf compiler is not required.

```bash
cargo build --release
```

On Linux the binary is `target/release/agapornis-agent`.

## First run

```bash
./agapornis-agent
```

When `config.json` does not exist, the setup wizard asks for the master URL, node ID, and one-time bootstrap token. Remote provisioning is HTTPS-only; loopback HTTP is accepted for local development. Certificates are written under `certs/`, and private material is mode `0600` on Unix.

The gRPC server listens on `0.0.0.0:5001` using HTTP/2 and mutual TLS. The client certificate must chain to the configured CA, use the exact common name `agapornis-master`, and contain the `clientAuth` extended key usage.

## Runtime dependencies

- Docker Engine and the `docker` CLI
- `tar`
- `df`, `chown`, and `/proc` on Linux
- `cscli` only when optional CrowdSec telemetry is enabled

## Environment

See `.env.example`. Important settings include:

- `AGAPORNIS_SERVERS_DIR`
- `AGAPORNIS_BACKUPS_DIR`
- `AGAPORNIS_DOCKER_NETWORK`
- `AGAPORNIS_BACKUP_ENCRYPTION_KEY` (base64-encoded 32-byte key)
- `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_BUCKET`, `S3_REGION`, `S3_ENDPOINT`, `S3_PREFIX`, `S3_FORCE_PATH_STYLE`
- `AGAPORNIS_CROWDSEC_ENABLED`, `AGAPORNIS_CROWDSEC_CLI_PATH`, `AGAPORNIS_CROWDSEC_MAX_ALERTS`
- `AGAPORNIS_DOCKER_IMAGE_CLEANUP_ENABLED` (default `true`)
- `AGAPORNIS_DOCKER_IMAGE_CLEANUP_INTERVAL_SECONDS` (default `21600`, every 6 hours)
- `AGAPORNIS_DOCKER_IMAGE_CLEANUP_MIN_AGE_HOURS` (default `24`)

The cleanup task runs `docker image prune` with both `dangling=true` and an age filter. Docker preserves images referenced by any container, including stopped containers; tagged and recently replaced images are not broadly pruned.

## Verification

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo run -- --self-test-backups
```

The backup self-test performs an authenticated encryption round trip and a real tar archive/extraction round trip.

## Binary updates

The API stages the SHA-256-verified artifact matching the agent runtime (`linux-x86_64` or `linux-aarch64`). Production activation uses the supplied `deploy/agapornis-agent.service`:

- `ExecStartPre --activate-pending-update` atomically swaps the staged binary into place while the service is stopped.
- The previous executable remains in `updates/previous-agent` during the health window.
- A successful 30-second run commits the update and deletes the previous executable.
- If the service exits sooner, the next `ExecStartPre` restores the previous executable.
- `--rollback-update` provides a manual rollback command while the activation is still pending health confirmation.

Set `AGAPORNIS_UPDATE_AUTO_RESTART=true` when running under the supplied systemd unit so a successful staging RPC schedules service activation automatically.
