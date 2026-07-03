# Security Policy

Only the latest release is supported with security updates.

Do not report vulnerabilities in a public issue. Send reports to [abubukhari@proton.me](mailto:abubukhari@proton.me) with reproduction steps, impact, and any suggested fix.

The agent treats all gRPC calls as privileged. It requires mTLS, validates the master CA chain at the TLS layer, and additionally enforces the `agapornis-master` certificate identity and `clientAuth` usage before dispatching an RPC.

