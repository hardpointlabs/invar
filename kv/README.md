# KV Abstraction Layer

---

## About

This module presents a vendor-neutral abstraction over different LSM-tree implementations to give a single interface for reading/writing keys and managing transactions. It also gives standardised, lowest-common-denominator isolation and atomicity guarantees so callers can lean on this rather than having to understand the invariants of individual vendors.

## Guarantees

**Guarantees, in detail: see the package doc comment in `doc.go`**
(`go doc .` or your editor's hover-docs from anywhere `kv.KeyValueStore` is
imported). That's the single source of truth that implementors should consult
when calling this API.

## Usage

Quick orientation if you're new to this package:

- `KeyValueStore` / `Tx` — the main interface. Snapshot Isolation, not full
  serializability. Read `doc.go` before relying on any invariant stronger
  than "reads see a consistent snapshot."
- `linearizability_test.go` — the executable spec. If you're unsure
  whether a guarantee holds for a given backend, the answer is whatever
  that test currently reports, not what this file or `doc.go` assumes.
- `Merge` / `MergeOption` / `WriteHandle` — not implemented yet. Don't
  build against these until their own doc comments say otherwise.

## Error handling

Different underlying KV implementations emit different error types (or none at all) for various failure modes. For this reason, callers should only program against the public error types in this package and not any BadgerDB/SlateDB errors.