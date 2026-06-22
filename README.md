# Invar

![Logo](/logo.png)

![GitHub Release](https://img.shields.io/github/v/release/hardpointlabs/invar) ![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/hardpointlabs/invar/release.yml)

## Overview

Invar is a lightweight durable document database. Its main goals are:

* Lightweight: low resource-usage, single binary
* Simple consistency & durability guarantees
* Ops friendliness: simple backups and encryption
* Open: Apache 2.0-licensed

It supports clients which speak the Redis Serialization Protocol (RESP), and has incubating support for MongoDB client drivers.

If you need to store JSON documents reliably, without the complexity or licensing constraints of other systems, try Invar.

## Backed by Hardpoint

Invar is backed by [Hardpoint Labs](https://hardpoint.dev) and powers our enterprise products. If you need to offer comprehensive tenant isolation for enterprise customers without throwing away your existing stack, [give us a try](https://docs.hardpoint.dev/who-is-hardpoint-for).

---

## Getting started

We ship Docker builds of our latest releases which you can pull and run as a 1-liner:

```
docker run -it -v /tmp:/var/run/invar -p 6379:7379 ghcr.io/hardpointlabs/invar:latest redis
```

### Redis client compatibility

See the [compatibility](./COMPATIBILITY.md) docs for more details.

### Mongo driver compatibility

We're still working on a stable release with Mongo wire protocol support; please create an [issue](https://github.com/hardpointlabs/invar/issues) if this is something you're interested in.

---

## Design goals

* Everything is persisted as keys in BadgerDB under the hood (a fast LSM-tree-based, ACID-compliant key store written in pure Go).
* It's not expected that this runs in a cluster: one daemon, one database
* The goal is not to rival the in-memory speed of Redis (or the speed of mongo when mmap is working well, although it should get close). Instead the goal is light weight, allowing individual DBs to scale down to a few MBs of RAM and minimal CPU when idling, so many of them can run concurrently on underlying hardware through some hypervisor such as [Firecracker](https://firecracker-microvm.github.io/)
* Since BadgerDB supports transactions, we aim to support them
