# Security Policy

## Supported Versions

Invar is under active, fast-moving development. Only the **latest released version** receives security fixes — there's no long-term-support branch at this stage. If you're reporting an issue, please confirm it reproduces on the latest release first; it may already be fixed.

| Version | Supported |
|---|---|
| Latest release | ✅ |
| Anything older | ❌ |

## Reporting a Vulnerability

Please **do not** open a public GitHub issue for a security vulnerability.

- **Preferred:** use GitHub's private vulnerability reporting ("Report a vulnerability" under this repo's **Security** tab).
- **Alternative:** email `security@hardpoint.dev`.

We aim to acknowledge reports promptly. Invar is currently maintained by a small team without a dedicated security function or a formal SLA — reports are read and triaged personally rather than routed through a process, so response time will vary with availability. If you haven't heard back and it's been a while, a follow-up nudge is always fine.

If confirmed, we'll credit the reporter (unless you'd prefer to stay anonymous) once a fix ships and coordinate on disclosure timing.

## Known, By-Design Characteristics

These are deliberate scope decisions, documented here so you don't spend time writing up something we already know about. Full detail on all of these lives in the [Usage guide](https://docs.hardpoint.dev/guides/invar/usage).

- **No authentication or authorization.** `AUTH`, ACL, and RBAC aren't implemented. Anyone who can open a TCP connection to Invar's RESP port has full access to everything on that instance. Run Invar on a private network, behind your own access controls — the same way you wouldn't expose a bare Postgres or an unauthenticated Redis instance directly to the internet. If you need per-tenant access control, that's what [Hardpoint's managed offerings](https://hardpoint.dev) are for.
- **No built-in transport encryption.** Connections between RESP clients and Invar aren't encrypted by Invar itself. If you need encryption in transit, terminate TLS via a sidecar or proxy, or rely on your network's own isolation.
- **Object storage credentials are your responsibility to secure.** Invar doesn't manage, rotate, or protect whatever AWS/S3-compatible credentials you provide via environment variables — treat them with the same care you'd give any credential with write access to your bucket. Workload-identity-based credentials (e.g., IAM Roles for Service Accounts) are preferable to long-lived static keys wherever your platform supports them.
- **Lua scripting is not an additional privilege boundary.** `EVAL`/`EVALSHA` run via an embedded interpreter ([Piccolo](https://github.com/kyren/piccolo)) with access to `redis.call()`. As with real Redis, anyone able to execute a script against your instance already has the same access as any other client command — there's no sandboxing designed to contain a client that already has RESP access.
- **No per-client resource limiting.** A client with network access can issue commands at whatever rate it wants; there's no built-in throttling or multi-tenant fairness enforcement at this layer today.

## Out of Scope

- Vulnerabilities in the underlying object store (S3, or any S3-compatible provider), the OS, or the container runtime you deploy on — report those to the relevant upstream project.
- Vulnerabilities in [SlateDB](https://github.com/slatedb/slatedb) itself, if the issue is in SlateDB's own code rather than how Invar uses it — please also report those upstream so they can be fixed at the source, but let us know too, since we'll need to track the dependency bump either way.
- General correctness/data-integrity behavior that's already documented (e.g., isolation-level characteristics) — see the [Usage guide](https://docs.hardpoint.dev/guides/invar/usage) rather than filing these as security reports.