# Rust agent architecture

The Rust agent is a single binary with four gRPC services. The folders under `src/` group code by responsibility; the public Rust module names remain stable through path declarations in `src/lib.rs`.

## Request flow

1. `main.rs` loads configuration and the mTLS identity.
2. `grpc/services/` authenticates the master certificate and translates protobuf requests.
3. Runtime or storage modules perform the requested operation.
4. Shared state in `AppState` connects Docker, files, backups, updates, protection, and live consoles.

The protobuf contract in `proto/server.proto` is the compatibility boundary with the API and the C# reference agent. Do not rename RPCs, fields, or field numbers without coordinating a protocol migration.

## Source map

| Folder | Responsibility | Start here when changing |
| --- | --- | --- |
| `src/core/` | Configuration, safe paths, process execution | Setup, environment, command execution, path confinement |
| `src/security/` | mTLS lifecycle and runtime protections | Certificates, authorization assumptions, rate limits |
| `src/runtime/` | Docker, node telemetry, supervision, updates | Containers, stats, disk enforcement, CrowdSec, agent updates |
| `src/storage/` | Files and backups | Uploads, archives, S3, restore, ownership |
| `src/grpc/` | Protobuf-facing adapters and shared application state | RPC behavior and request/response mapping |
| `proto/` | Wire contract | API-agent compatibility |

### Docker runtime

`src/runtime/docker/` keeps container concerns deliberately separate:

| File | Owns |
| --- | --- |
| `provisioning.rs` | Container creation, port allocation, and egg installers |
| `lifecycle.rs` | Start, stop, restart, delete, limits, and disk start-gating |
| `console.rs` | Persistent Docker attach streams and console commands |
| `database.rs` | Attached-database connectivity and database port metadata |
| `inspection.rs` | Inspect data, metrics, disk cache, and size parsing |
| `configuration.rs` | Docker networks, startup validation, and generated config files |

### Backup storage

`src/storage/backup/` separates orchestration from storage mechanics:

| File | Owns |
| --- | --- |
| `manager.rs` | Public backup operations and local/S3 routing |
| `s3.rs` | S3 client configuration and object operations |
| `archive.rs` | Metadata, tar operations, integrity checks, and transactional restore |
| `crypto.rs` | Authenticated backup encryption and decryption |
| `self_test.rs` / `tests.rs` | Runtime self-test and regression tests |

### gRPC adapters

`src/grpc/services/` has one adapter per protocol service:

| File | Owns |
| --- | --- |
| `server.rs` | Server lifecycle, stats, console, updates, and certificates |
| `file.rs` | File RPC translation |
| `backup.rs` | Backup RPC translation |
| `transfer.rs` | Node transfer streaming |
| `console.rs` | Shared console fan-out and history |
| `authorization.rs` | Master certificate authorization |
| `responses.rs` | Common protobuf response conversion |

## Important invariants

- Every gRPC request requires a valid client certificate for the exact common name `agapornis-master` with `clientAuth` usage.
- Server file paths are confined below the configured servers directory. API paths may begin with `/`, but traversal with `..` must remain rejected.
- Backup restore is replacement-based. Restoring through a staging directory must remove stale files rather than overlaying them.
- Database containers expose their internal database port even when the port is private and not published on the host.
- Console input uses one reusable Docker attach stream per server.
- Update artifacts require HTTPS and a matching SHA-256 checksum.

## Background work

`runtime/supervisor.rs` periodically observes containers, enforces disk limits, and forwards console output into `ConsoleHub`. Keep periodic work bounded: avoid a Docker scan per viewer or per gRPC stream.

## Adding behavior

Put protocol translation in `src/grpc/`, reusable operations in the responsible runtime/storage module, and generic safety helpers in `src/core/` or `src/security/`. Add tests beside the smallest module that owns the invariant. If behavior exists in the C# agent, read that implementation as the compatibility reference before changing semantics.
