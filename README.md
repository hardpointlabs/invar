# Invar — A Diskless, Redis-Compatible Document Database

Durable storage lives in S3, not on disks you have to provision or manage.

Invar gives you Redis-compatible storage without the tradeoff between "cheap" and "durable." There are no attached volumes to size, replicate, or run out of — source of truth lives in object storage, so cost scales with what you store, not what you provision. A single binary can idle at a few MB of RAM, making it practical to run thousands of isolated instances on modest hardware.

[![Apache 2.0 License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE) ![GitHub Release](https://img.shields.io/github/v/release/hardpointlabs/invar) ![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/hardpointlabs/invar/release.yml) ![Discord](https://img.shields.io/discord/1481682538291400758)

---

## Why Invar

- **Redis wire protocol compatibility:** Point your existing Redis clients at Invar. Compatibility is tested continuously against real-world libraries, including [BullMQ](https://github.com/taskforcesh/bullmq), not just the raw command spec. See [COMPATIBILITY.md](COMPATIBILITY.md) for the full command matrix
- **Diskless by design:** No state replicate, no capacity planning. Durable data lives in S3; cost tracks what you store, not what you provision. A small hot working set stays fast via optional local caching (see [How it works](#how-it-works))
- **Defined durability and transactional guarantees:** Invar targets snapshot isolation, with merge support incubating
- **Single binary, lightweight:** One process, no cluster coordination
- **Real local dev experience:** Can persist to disk for simplified local dev & CI
- **Apache 2.0:** Fully open source

## Quickstart

### Local FS

This boots up an instance persisting data to `/tmp/invar`:

```bash
docker run -v /tmp/invar:/tmp/invar -p 6379:6379 -it ghcr.io/hardpointlabs/invar:latest --backend fjall --path /tmp/invar --redis
```

#### S3

Pass the usual `AWS_...` variables to configure Invar to run backed by object storage:

```bash
docker run -v /tmp/invar:/tmp/invar -e AWS_REGION=... -e AWS_ACCESS_KEY_ID=...\
  -e AWS_SECRET_ACCESS_KEY=... -p 6379:6379 -it ghcr.io/hardpointlabs/invar:latest \
  --backend slate --bucket <my-bucket-name> --redis
```

## How it works

Invar is built on [SlateDB](https://slatedb.io), an LSM-tree storage engine designed to sit directly on object storage, with [Fjall](https://github.com/fjall-rs/fjall) as the equivalent local-disk engine for development and cases where talking to S3 isn't practical. Each Invar instance is single-writer by design — it's built to run well as one-database-per-dataset at fleet scale, not as a single large shared cluster.

**"Diskless" refers to the source of truth, not literal I/O.** Invar uses local NVMe as a best-effort read cache to keep hot data fast and mitigate S3 latency on read-heavy workloads. That cache is disposable — Invar's durability guarantees never depend on it, and it can be lost or rebuilt without data loss. Durable state lives in S3 (or Fjall locally); nothing on disk needs to be provisioned, sized, or replicated by you.

## Compatibility

Invar implements a broad, actively-tested subset of the Redis command set, including stream commands. See [COMPATIBILITY.md](COMPATIBILITY.md) for the full list and known gaps.

## License

Apache 2.0. See [LICENSE](LICENSE).