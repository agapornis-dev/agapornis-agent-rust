![Logo Image](agapornis-agent.png)

# Agapornis Agent (Rust)

> **Beta software:** Agapornis is under active development and may introduce breaking changes. Back up node data and test upgrades before using a new release.

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

Tagged GitHub releases provide prebuilt binaries for `linux-x86_64` and `linux-aarch64`; local compilation is not required on supported hosts.

## First run

```bash
./agapornis-agent
```

When `config.json` does not exist, the setup wizard asks for the master URL, node ID, and one-time bootstrap token. Remote provisioning is HTTPS-only; loopback HTTP is accepted for local development. Certificates are written under `certs/`, and private material is mode `0600` on Unix.

The gRPC server listens on `0.0.0.0:5001` using HTTP/2 and mutual TLS. The client certificate must chain to the configured CA, use the exact common name `agapornis-master`, and contain the `clientAuth` extended key usage.

## Native systemd installation

Install the binary, run the interactive bootstrap once, and then enable the supplied unit as root:

```bash
install -m 0755 agapornis-agent-linux-x86_64 /usr/local/bin/agapornis-agent
mkdir -p /opt/agapornis/agent /etc/agapornis
cd /opt/agapornis/agent
/usr/local/bin/agapornis-agent
install -m 0644 deploy/agapornis-agent.service /etc/systemd/system/agapornis-agent.service
systemctl daemon-reload
systemctl enable --now agapornis-agent.service
```

Use `agapornis-agent-linux-aarch64` on an ARM64 host. Place the generated `config.json` and `certs/` directory in `/opt/agapornis/agent`. Optional environment settings belong in `/etc/agapornis/agent.env`.

## Runtime dependencies

- Docker Engine and the `docker` CLI
- `tar`
- `df`, `chown`, and `/proc` on Linux
- `cscli` only when optional CrowdSec telemetry is enabled. The default `cscli` value uses PATH lookup and then tries common Linux install paths such as `/usr/bin/cscli`, `/usr/local/bin/cscli`, and `/snap/bin/cscli`.

## Environment

See `.env.example`. Important settings include:

- `AGAPORNIS_SERVERS_DIR`
- `AGAPORNIS_BACKUPS_DIR`
- `AGAPORNIS_DOCKER_NETWORK`
- `AGAPORNIS_BACKUP_ENCRYPTION_KEY` (base64-encoded 32-byte key)
- `AGAPORNIS_BACKUP_CONCURRENCY` (default `1`; keep at `1` for the lowest CPU usage)
- `AGAPORNIS_PROTECTION_SCAN_SECONDS` (default `10`)
- `AGAPORNIS_DISK_CHECK_SECONDS` (default `150`)
- `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_BUCKET`, `S3_REGION`, `S3_ENDPOINT`, `S3_PREFIX`, `S3_FORCE_PATH_STYLE`
- `AGAPORNIS_CROWDSEC_ENABLED`, `AGAPORNIS_CROWDSEC_CLI_PATH`, `AGAPORNIS_CROWDSEC_MAX_ALERTS`

CrowdSec telemetry is Linux-only and read-only. Leave `AGAPORNIS_CROWDSEC_CLI_PATH=cscli` unless `cscli` is installed in a custom location; the agent will pass `alerts list -o json --limit <max>` safely as arguments and report successful reads as `active`.
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

The repository's release workflow publishes a manifest containing a URL, size, and SHA-256 hash for each supported runtime. The API reads that manifest directly from `agapornis-dev/agapornis-agent-rust`, asks the node which runtime it uses, and sends only the matching artifact to the agent.

The agent downloads over HTTPS, enforces the size limit, verifies SHA-256 before making the file executable, and stages it beside the installed binary. Production activation uses `deploy/agapornis-agent.service`:

- `ExecStartPre --activate-pending-update` atomically swaps the staged binary into place while the service is stopped.
- The previous executable remains in `updates/previous-agent` during the health window.
- A successful 30-second run commits the update and deletes the previous executable.
- If the service exits sooner, the next `ExecStartPre` restores the previous executable.
- `--rollback-update` provides a manual rollback command while the activation is still pending health confirmation.

Set `AGAPORNIS_UPDATE_AUTO_RESTART=true` when running under the supplied systemd unit so a successful staging RPC schedules service activation automatically.

The unit also sets `AGAPORNIS_UPDATE_SYSTEMD_SERVICE=agapornis-agent.service`. `AGAPORNIS_UPDATE_HEALTH_SECONDS` changes the default 30-second health window. To inspect an update, use `journalctl -u agapornis-agent.service`; to force a rollback before the health commit, stop the service and run `/usr/local/bin/agapornis-agent --rollback-update`.

## Publishing a release

Update `Cargo.toml` and `Cargo.lock`, commit the change, and push a matching tag:

```bash
git tag v0.2.0
git push origin main v0.2.0
```

`.github/workflows/release.yml` runs Clippy and tests, cross-compiles both Linux targets, publishes binary checksums, and generates `release-manifest.json`. The version reported by each binary is embedded at build time. The workflow rejects a tag that does not equal `v<Cargo package version>`.
