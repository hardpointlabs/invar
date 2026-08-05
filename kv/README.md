# KV Abstraction Layer

---

## About

This module presents a vendor-neutral abstraction over different LSM-tree implementations to give a single interface for reading/writing keys and managing transactions. It also gives standardised, lowest-common-denominator isolation and atomicity guarantees so callers can lean on this rather than having to understand the invariants of individual vendors.

## Usage

The KV API consists of the following principal components:

* `KeyValueStore` - The core database singleton. Manages the lifecycle of the DB and initiates transactions and merge operations.
* `Tx` - The transaction abstraction. Wraps the batch/tx key operations of the underlying data store
* `Entry` - A key-value pair to be _inserted_ into the store
* `Item` - An immutable key-value pair that was _read_ from the store

## Error handling

Different underlying KV implementations emit different error types (or none at all) for various failure modes. For this reason, callers should only program against the public error types in this package and not any BadgerDB/SlateDB errors.