# Physical namespace ids for compound values

Status: Approved

Authors:

* Chris Mowforth


# Summary

Compound values (lists, hashes, sets, sorted sets, streams, bloom filters) store their elements under
internal keys that are derived from the *name* of the Redis key. The name is therefore the physical address
of the data, which makes `RENAME` impossible to implement in O(1), loses data on `MOVE`, and lets stale
entries reappear when a name is reused. This RFC proposes that each compound value's sentinel record the
physical prefix its elements live under: a unique, name-independent *namespace* assigned when the value is
created. `RENAME` then becomes a rewrite of the sentinel under a new name, exactly as Redis moves a pointer
to a value object, and orphaned entries can never be mistaken for the data of a newly created key.

# Motivation

## The contract we want

Redis documents `RENAME` as O(1): it moves a pointer to the value object from one slot of the key table to
another, together with the key's expiry. `DEL` is O(N) in the number of elements, because the elements must
be freed. A compound value in Redis owns its elements (listpack, quicklist, skiplist, hash table) *inside*
the object, so the elements' address is the object's address and the key's name plays no part in it.

## Why Invar cannot do that today

The LSM keyspace is flat, so every element is its own key. Those keys are built from the name:

| Type | Public key (sentinel) | Internal keys |
|---|---|---|
| hash | `<db>:<name>` → count | `-<db>:<name>\0<field>` |
| set | `<db>:<name>` → count | `-<db>:<name>\0<member>` |
| sorted set | `<db>:<name>` → count | `-<db>:<name>:score:<8B score>:<member>` and `-<db>:<name>:member:<member>` |
| list | `<db>:<name>` → size, head, tail (36 B) | `-<db>:<name>:<16 B node key>` |
| stream | `<db>:<name>` → 72 B metadata | `-<db>:<name>\0<version>:<ms>-<seq>` |
| bloom filter | `<db>:<name>` → metadata | `-<db>:<name>\0bf:<filter id>:p:<page>` |

`Session::private_key(name)` produces the `-<db>:<name>` prefix, and every command captures it from the
name when its op is constructed. Moving the sentinel without moving every element strands the elements
under the old name. This is what a naive `RENAME` does today: it replies `OK`, but `LRANGE`, `HGETALL`,
`SMEMBERS` and `ZRANGE` on the new name return nothing, and `LLEN` still reports the old size.

## Problems this causes

1. **`RENAME` and `MOVE` lose data.** Fixing them by re-keying every element is O(N), a single write
   transaction proportional to the size of the value on a single-writer store (branch
   `fix/rename-compound-types`). Redis documents both commands as O(1).
2. **Stale entries come back.** Because the name is the address, any leftover internal entry is picked up
   again as soon as the name is reused. We have reproduced, on current `master`:
   * `SET h str` over a hash, `DEL h`, then `HSET h g new`: `HGETALL h` returns `f old g new`.
   * a hash whose TTL expires, then `HSET` on the same name: the old fields reappear.
   * `DEL` of a stream or a bloom filter (fixed on branch `fix/del-cleans-internal-entries`): `XRANGE`
     returns the deleted stream's entries and `BF.EXISTS` reports the deleted filter's items.
3. **Name prefixes are ambiguous.** The internal keys of `foo` share the byte prefix `-0:foo` with those of
   `foo:bar`. A prefix scan on the name is therefore unsafe, and lists, whose nodes have no separator after
   the name, must be walked node by node to delete or move them.

# Guide Implementation

Think of a compound value as a small directory on disk, and its Redis key as a name that points at it.

**Namespace.** A *namespace* is the physical prefix under which one compound value stores its elements.
It is chosen when the value is created, never changes, and has nothing to do with the key's name. The
sentinel (the entry at the public key) records it.

Creating a hash, then renaming it:

```
HSET user:1 name ada        -- sentinel written at "0:user:1", namespace "+0:7f3a…" (16 random bytes)
                            -- field stored at "+0:7f3a…\0name"
RENAME user:1 user:2        -- sentinel copied to "0:user:2", "0:user:1" deleted. Nothing else is touched.
HGET user:2 name            -- reads the sentinel, finds "+0:7f3a…", reads "+0:7f3a…\0name"  → ada
HSET user:1 name grace      -- a new value, so a new namespace "+0:b81c…"; nothing of the old one is visible
```

Because a new value always gets a new namespace, entries left behind by an overwrite or an expiry can never
be read as part of a later key with the same name. They are garbage (wasted space), not corruption.

Engineers writing commands stop deriving `node_prefix` from the key name. A command reads the sentinel,
and asks it for the namespace. Almost every command already reads the sentinel first to check the type or
the count, so for them nothing is added.

# Reference Implementation

## Physical layout

A namespace is a complete physical prefix. Element keys are `namespace ++ suffix`, where `suffix` is exactly
what each type appends to its prefix today (`\0<field>`, `:score:…`, `:<node key>`, `\0<version>:…`,
`\0bf:…`). The per-type code that builds element keys does not change; only where it gets its prefix does.

* **New namespaces** are `+<db>:` followed by 16 random bytes (`+0:` + 16 B). The leading `+` is neither a
  digit (public keys) nor `-` (legacy internal keys), so new namespaces cannot collide with either, and the
  fixed 16-byte width makes each namespace an unambiguous prefix (`foo` and `foo:bar` stop sharing
  prefixes).
* **Legacy namespaces** are the name-derived prefix used today, `-<db>:<name>`. They are only ever produced
  by existing data, or by renaming it (below).
* The db number stays in the prefix so that `FLUSHDB`, which today is two `drop_prefix` calls (`<db>:` and
  `-<db>:`), remains a cheap prefix drop with a third call for `+<db>:`.

## The sentinel records the namespace

Each sentinel gets a trailer:

```
<existing sentinel bytes> || namespace (n bytes) || n (u32, big endian)
```

and the entry's type byte (`ValueType`, currently 0..=8) has its high bit (`0x80`) set to mark that the
trailer is present. A sentinel without the bit is a legacy sentinel and its namespace is derived from the
name, as today. Code reads the namespace through a single helper, so no command parses the trailer itself.

Setting the high bit makes older binaries *fail closed*: they compare the type byte for equality (about
fifteen places), see an unknown type, and reply `WRONGTYPE` instead of silently addressing the wrong prefix
and returning empty results. See the open question about the alternative.

## Creation, reads and writes

* **Create.** When a write finds no live sentinel, it generates a namespace and writes it with the sentinel
  in the same transaction. When it finds one, it reuses its namespace.
* **Resolve at run time.** Today a command's `node_prefix` is captured at construction, from the name alone.
  It would instead be resolved inside `DbOp::run` from the sentinel the op reads anyway. The ops that
  address elements without reading the sentinel today (for example `HGET` goes straight to the field key)
  gain one point read of it, and would then also be able to reply `WRONGTYPE` for a key of another type
  (`HGET` does not check the type today).

## Commands

* **`RENAME` / `RENAMENX`** are O(1): read the source sentinel, write the same bytes (so the same
  namespace) and the source's remaining TTL under the new name, delete the source name. For a legacy
  source the new sentinel records the legacy prefix `-<db>:<oldname>` as its namespace, so legacy keys are
  renamed in O(1) too and nothing is migrated. `RENAME k k` is a no-op. When the destination exists, its
  elements are deleted first (below); this O(size of the overwritten value) cost is accepted, as it is in
  Redis, where the overwritten value is freed.
* **`DEL` / `UNLINK`** read the sentinel and delete the namespace's elements by prefix scan. Because new
  namespaces are unambiguous, lists no longer need a node-by-node walk. Cost is proportional to the value,
  as in Redis.
* **Overwrites (`SET`, `GETSET`, `MSET`, `SETEX`, `*STORE` destinations).** A key that currently holds a
  compound value must have its elements deleted by the same path as `DEL`. This is a bug fix independent of
  the namespace, but with namespaces it is purely about reclaiming space, not about correctness.
* **Expiry.** Expiry is lazy and removes only the sentinel; the elements become orphaned. With namespaces
  this is garbage and not corruption. Reclaiming it is left to a follow-up (see Unresolved Questions).
* **`MOVE`** changes the db number, which is part of the prefix, so it remains a re-key of the elements:
  O(N), but correct. `MOVE` is rare, and keeping the db in the prefix keeps `FLUSHDB` cheap.
* **`FLUSHDB` / `FLUSHALL`** drop the additional `+<db>:` prefix.
* **`SCAN`, `DBSIZE`, `EXISTS`, `TYPE`** only look at public keys and the type byte (masked), and are
  unaffected.

## Rollout and rollback

1. **Cleanup first.** `DEL` removes all internal entries of every compound type, and removed list nodes are
   deleted (branch `fix/del-cleans-internal-entries`). Independent of this RFC.
2. **Release N, "read".** Understands flagged sentinels and resolves namespaces at run time, but still
   *creates* legacy keys. No behaviour change.
3. **Release N+1, "write".** Creation switches to new namespaces; `RENAME` becomes O(1).

Rolling back from N+1 to N is safe. Rolling back to the release before N makes keys created by N+1
unreadable (`WRONGTYPE`) until the upgrade is redone, but loses no data. Release N must therefore not be
skipped, and this is called out in the release notes.

## Testing

* The behavioural tests on `fix/rename-compound-types` (list, hash, set, sorted set, stream and bloom
  contents survive `RENAME`; the TTL moves; an existing compound destination is replaced, not merged;
  `RENAME k k` keeps the value; similarly named keys are unaffected) carry over unchanged.
* New integration entries in `test/redis-commands.json` for resurrection (overwrite + `DEL` + recreate,
  expiry + recreate) and for `RENAME` followed by recreating the old name.
* Golden-bytes unit tests for legacy sentinels (with and without trailer) so that old data stays readable.
* A unit test asserting that a rename writes a constant number of keys regardless of the size of the value.

# Drawbacks

* **Large, mechanical churn.** About 66 `private_key()` call sites and about 290 `node_prefix`/`page_prefix`
  references across hash, set, zset, list, stream and bloom move from construction time to run time.
* **One-way format change** (mitigated by the two-release rollout and fail-closed type byte).
* **Extra read** for the element-addressed ops that skip the sentinel today (`HGET`, `HMGET`, `HEXISTS`,
  `ZSCORE`, …). The sentinel is a hot, tiny record, but it is a read.
* **Less debuggable keys.** The name is no longer in the element keys, so a raw dump of the store cannot
  say which key an element belongs to without reading the sentinel.
* **Orphan accounting.** Garbage left by expiry or overwrite is no longer attributable to a name from its key
  alone, which makes a sweeper harder.
* **`MOVE` stays O(N).**
* **Departure from the Go on-disk layout** that several modules document as matching exactly.

# Rationale / Alternatives

**Why a namespace per value.** It is the same indirection Redis has (the value object), and it fixes three
separate defects (`RENAME`, `MOVE` correctness, stale-entry resurrection) with one concept, instead of a
special case for each.

* **Keep re-keying on `RENAME` (status quo on `fix/rename-compound-types`).** Correct, simple, no format
  change. But O(N) in one transaction on a single-writer store, contrary to the Redis contract, and it does
  nothing about resurrection. Kept as the behaviour of `MOVE`.
* **Forwarding pointer plus a claimed-prefix marker.** Keep name-derived prefixes; on rename, record the old
  prefix in the new sentinel and write a marker so that a later create of the old name does not reuse the
  prefix. Smaller on-disk change, but it adds a marker lifecycle and a read on every key creation, and it
  still leaves name-derived prefixes (and their ambiguity) for everything not renamed.
* **An alias table in the KV layer.** Map prefix → prefix and translate on every access. Every operation
  pays for it forever, and the table needs its own garbage collection.
* **Copy lazily in the background.** `RENAME` returns immediately and elements follow. Readers would have
  to consult both names, and a crash mid-way leaves a half-moved value; not acceptable for a store whose
  first rule is not to lose data.
* **Store the whole collection in one value.** This is what Redis effectively does in memory, but values
  would be unbounded in size and every element update would rewrite all of it.
* **A monotonic counter instead of random bytes.** Needs a shared hot key (or per-db allocation) that every
  creation conflicts on under optimistic concurrency. 128 random bits need no coordination.

# Unresolved Questions

> **Open Question:** Fail-closed marker. This RFC sets a high bit in the type byte so that older binaries
> reply `WRONGTYPE`. The alternative is a magic suffix on the sentinel value, which needs no change to type
> comparisons but makes an older binary silently read the wrong prefix. Is fail-closed worth masking the
> type byte in the roughly fifteen comparisons?

> **Open Question:** Should the db number stay in the namespace prefix? Keeping it makes `FLUSHDB` a prefix
> drop and `MOVE` an O(N) re-key. A db-less namespace would make `MOVE` O(1) but `FLUSHDB` would have to
> enumerate the sentinels of the db and delete each namespace.

> **Open Question:** Reclaiming orphans. Expired keys and overwritten values leave unreachable elements.
> Options: a background sweeper keyed on a per-namespace owner record, deleting a value's elements at
> expiry time instead of lazily, or accepting the garbage. Not needed for correctness once this RFC lands.

> **Open Question:** Every module's sentinel decoder must be audited for how it treats trailing bytes
> (several validate only a minimum length today). If any is strict, the trailer needs a per-type
> accommodation.

> **Open Question:** Is the Go on-disk layout parity still a goal? If a Go build must read Invar's data, this
> RFC is incompatible with it.

> **Open Question:** Cleanup on `RENAME` when the destination exists. This RFC pays O(size of destination)
> synchronously. Leaving the old elements as garbage for a sweeper would make that case O(1) as well.
