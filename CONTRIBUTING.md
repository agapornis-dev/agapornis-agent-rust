# Contributing to the Rust agent

## Before editing

Read `docs/ARCHITECTURE.md`, then locate the owning folder from its source map. Treat `proto/server.proto` as a stable external contract and the C# `agapornis-agent` as the runtime behavior reference.

## Local workflow

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

On Linux, also exercise the behavior-focused self-tests when touching their areas:

```bash
cargo run -- --self-test-backups
cargo run -- --self-test-console
cargo run -- --self-test-disk-cache
```

Docker-backed behavior should be tested against a disposable container or node. Never point development tests at production server directories.

## Change checklist

- Keep RPC declarations and protobuf field numbers compatible.
- Preserve path confinement and mTLS authorization on every new entry point.
- Avoid blocking work on Tokio executor threads; use the existing process helpers.
- Do not log tokens, private keys, backup keys, database passwords, or file contents.
- Add a focused regression test for fixed bugs when practical.
- Run formatting, Clippy with warnings denied, and the full test suite before handing off.

## Review guide

Review security boundaries first: certificate identity, path confinement, shell quoting, Docker arguments, and secret handling. Then check compatibility with the C# agent and the API request shape. Finally check cancellation and resource behavior for streams, background loops, and container processes.
