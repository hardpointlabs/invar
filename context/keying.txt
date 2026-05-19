Redis sets are not implemented by “prefixing keys” into the global keyspace, and they are not stored as a linked list/string blob under one key either.

Internally, a Redis Set value is usually backed by a real hash table (`dict`) dedicated to that set.

So conceptually:

```text
global DB keyspace:
  "myset" -> pointer to Set object

Set object:
  hash table:
    "alice" -> NULL
    "bob"   -> NULL
    "carol" -> NULL
```

The set members are *not* separate top-level Redis keys like:

```text
myset:alice
myset:bob
```

That would make operations like `SMEMBERS`, `SCARD`, `SINTER`, etc. awkward and inefficient.

Instead, Redis stores one top-level key (`myset`) whose value is an in-memory set structure.

---

## Complexity of `SISMEMBER`

`SISMEMBER` is expected **O(1)** average time, not linear.

Redis docs specify:

* `SISMEMBER`: O(1)
* `SADD`: O(1) per element
* `SREM`: O(1)
* `SMEMBERS`: O(N)
* `SINTER`: roughly proportional to input sizes

So your assumption about linear scans is not correct for normal sets.

---

## Actual encodings used

Redis uses multiple internal encodings depending on size and element type.

### 1. Small integer-only sets → `intset`

For tiny sets containing only integers:

```redis
SADD ids 1 2 3
```

Redis may use a compact contiguous integer array called an `intset`.

That gives excellent memory efficiency.

Membership checks here are basically binary search over sorted integers:

* O(log N)
* cache-friendly
* tiny footprint

Once it grows or gets non-integer members, Redis upgrades automatically.

---

### 2. General sets → hash table (`dict`)

Normal sets use Redis's internal dictionary implementation:

```c
dict<member, NULL>
```

The member is the key.
The value is unused.

So membership is standard hash-table lookup.

This is the common case.

---

## Why not namespace prefixes?

If Redis implemented sets via prefixed keys:

```text
set:foo:a
set:foo:b
set:foo:c
```

then:

### `SMEMBERS foo`

Would require scanning the entire DB for prefix matches.

That would be disastrous.

Redis databases are hash tables themselves:

```text
db dict:
  key -> redisObject*
```

There’s no ordered traversal by prefix.

So Redis instead stores composite structures as objects under one top-level key.

The same applies to:

* hashes
* zsets
* streams
* lists

Each key points to an internal data structure.

---

## Subtle point: Redis is already a giant hash table

The Redis database itself is:

```text
dict<key, value>
```

A Set value is then another internal structure.

So a Set is basically:

```text
global_hash["myset"] -> inner_hash["alice"]
```

Hash tables nested inside hash tables.

---

## Lists are different

You mentioned “wedging a list into a single key”.

Redis Lists are separate structures entirely:

* historically linked lists
* now quicklists (linked list of compressed listpacks)

Those *do* have more linear characteristics:

* `LINDEX` can be O(N)
* `LRANGE` proportional to traversal

But Sets are optimized specifically for membership tests.

---

## Interesting implementation detail

Redis aggressively optimizes small collections.

Examples:

* tiny hashes → listpack
* tiny zsets → listpack
* tiny sets of ints → intset

Then automatically upgrades to full hash tables / skip lists later.

That’s a huge reason Redis is memory-efficient despite being in-memory.

"yeah my bad I was looking at the SMISMEMBER docs and saw O(n). Let me ask another way, if I had to re-implement this and my only tool was an LSM tree (e.g. RocksDB, BadgerDB), couldn't I get away with prefixing? Given that I do get ordering (which the giant hash table concept doesn't give you), wouldn't that be less disastrous? Or is there some better alternative?"

Yes — with an LSM tree, prefixing becomes *much* more viable, and in fact that’s basically how many Redis-on-RocksDB style systems work internally.

Your intuition is correct:

* Redis’s in-memory hash-table architecture makes prefix scans terrible
* LSM engines *do* provide ordered iteration efficiently
* therefore composite types can absolutely be modeled as prefixed keys

Something like:

```text id="mqynzf"
set:myset:alice -> tombstone/value
set:myset:bob
set:myset:carol
```

becomes reasonable because:

* `SISMEMBER myset alice`
  → point lookup

* `SMEMBERS myset`
  → prefix scan over `set:myset:*`

* `SCARD myset`
  → harder, but solvable

This is actually a very common design.

---

# Why this works much better on LSMs

LSMs already optimize for:

* sorted key iteration
* prefix locality
* sequential scans
* range queries

Whereas Redis’s hash table gives you:

* fast point lookups
* terrible ordered traversal

So the storage engine fundamentally changes the tradeoffs.

---

# But there are still important complications

A naïve prefix encoding works, but production systems usually evolve beyond the simple model because of:

1. cardinality
2. write amplification
3. atomic multi-key ops
4. compaction costs
5. hot prefixes

---

# The obvious encoding

The straightforward mapping is:

```text id="r4ep5n"
SADD myset alice
=> PUT "S:myset:alice" = 1
```

Then:

```text id="7q99kp"
SISMEMBER myset alice
=> GET "S:myset:alice"
```

which is excellent.

---

# `SMEMBERS` becomes elegant

You can iterate:

```text id="rcc4ml"
seek("S:myset:")
while prefix matches:
    emit suffix
```

This is exactly what LSM iterators are good at.

---

# But `SCARD` becomes annoying

Redis hash-table sets know cardinality directly.

With prefix encoding:

```text id="k4p1zj"
COUNT(keys with prefix S:myset:)
```

is O(N).

So real systems usually maintain metadata:

```text id="9r7v3h"
META:myset -> count=12345
```

updated transactionally.

---

# The real challenge: set operations

This is where the architecture starts diverging from Redis internals.

Consider:

```redis id="x8j9fs"
SINTER set1 set2
```

In Redis:

* both are in-memory hash tables
* iterate smaller set
* hash probe larger set
* very fast

In LSM/prefix world:

You now have two sorted ranges:

```text id="59u0wh"
S:set1:*
S:set2:*
```

You have choices:

---

## Strategy 1 — Merge/intersection via iterators

Like merging sorted posting lists in an inverted index.

Efficient for large scans.

This is very database-ish.

---

## Strategy 2 — Point probes

Iterate smaller set:

```text id="mf8vb0"
for x in set1:
    GET S:set2:x
```

This resembles Redis behavior more closely.

Can be surprisingly good if Bloom filters help avoid disk reads.

---

# LSMs introduce tombstone complexity

Deletes are nasty in LSM systems.

```redis id="z4h9m5"
SREM myset alice
```

becomes:

```text id="w2r38h"
DELETE S:myset:alice
```

which is just a tombstone.

Now imagine millions of churny set members.

Compaction pressure rises dramatically.

This is one reason high-churn workloads can punish RocksDB.

Redis’s in-memory hash tables don’t suffer this issue.

---

# Another issue: hot prefixes

If you store:

```text id="3qkm4j"
S:followers:user123:*
```

and that set is enormous:

* compactions become skewed
* SST locality gets weird
* iterator pressure rises
* prefix Bloom filters may degrade

Many systems shard or hash-partition internally:

```text id="h2gc2v"
S:user123:07:alice
S:user123:91:bob
```

to distribute write load.

---

# This is basically how secondary indexes work

What you’re describing is extremely close to:

* inverted indexes
* Bigtable/Cassandra data models
* FoundationDB tuple encoding
* Dynamo-style composite keys

The “set” becomes a sparse ordered index.

---

# There’s also a hybrid approach

Some systems store:

```text id="k4wrlm"
set metadata -> compact blob
```

for small sets,

then spill into prefixed keys for large sets.

This mirrors Redis’s own “specialized small encoding then upgrade” philosophy.

Very common.

---

# Redis-on-RocksDB systems often do this

Systems like:

* KeyDB
* Dragonfly
* TiKV
* Cockroach Labs
* ScyllaDB

all wrestle with variants of this problem:

> how do you efficiently map rich data structures onto ordered KV engines?

And the answer is usually:

* composite/prefixed keys
* metadata side records
* careful iterator usage
* batching/transactions
* specialized encodings for small values

---

# The deeper conceptual shift

Redis data structures are:

```text id="2fyh7z"
object-oriented in-memory structures
```

LSM-backed systems become:

```text id="0cxqu5"
materialized ordered indexes
```

That changes the optimal algorithms completely.

You stop thinking:

> “what pointer structure represents a set?”

and start thinking:

> “what key layout minimizes write amplification and iterator cost?”

