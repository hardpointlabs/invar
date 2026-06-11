# Invar


Invar is a lightweight daemon acting as a NoSQL document database, with protocol support for some well-known data stores.

## Redis compatibility

See the [compatibility](./COMPATIBILITY.md) docs for more details.

## Design goals

* Everything is persisted as keys in BadgerDB under the hood (a fast LSM-tree-based, ACID-compliant key store written in pure Go).
* It's not expected that this runs in a cluster: one daemon, one database
* The goal is not to rival the in-memory speed of Redis (or the speed of mongo when mmap is working well, although it should get close). Instead the goal is light weight, allowing individual DBs to scale down to a few MBs of RAM and minimal CPU when idling, so many of them can run concurrently on underlying hardware through some hypervisor such as [Firecracker](https://firecracker-microvm.github.io/)
* Since BadgerDB supports transactions, we aim to support them
* Recent AI-oriented, geospatial-oriented and free-text search additions to Redis and MongoDB are not on the table for now: we want solid compatibility with the features that have been part of these products since the early days