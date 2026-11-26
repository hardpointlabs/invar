# A document storage crate

Status: Draft

Authors:

* Chris Mowforth


# Summary

Invar's `JSON.*` commands keep each document as one serialized JSON text under one key, and every
command is a load–modify–serialize–store cycle written directly against `kv::Tx`. This RFC proposes a new
workspace crate, `document` (`./src/document`, a sibling of `kv` and `redis`), that turns that into a small
embedded document storage engine:

* documents are **BSON** (the `bson` crate's `Document`/`Bson`), held in memory as such and persisted as
  BSON bytes;
* documents live in first-class **collections**; the Redis layer uses one collection per DB keyspace;
* **path resolution is pluggable**: the crate has one internal path model and one resolver/mutator, and
  each protocol supplies the parser for its own path syntax (JSONPath today, dotted field notation later);
* documents are changed with a typed **mutation** vocabulary (set, remove, arithmetic, string and array
  edits, rename) that reports exactly what changed;
* **secondary indexes** (ordered, optionally multikey, compound, unique, sparse or partial) are maintained
  in the same `kv` transaction as the document write;
* **filters** are a small predicate AST the crate can evaluate, use to choose an index, and apply as a
  residual check.

The crate persists through `KeyValueStore`/`Tx` only; it knows nothing about SlateDB, Fjall, RESP or any
particular query language. `redis/json.rs` is re-based onto it **without any change to the observable
behaviour of the existing `JSON.*` commands**. Indexing, filtering and richer mutation are what the crate
adds for future consumers (`FT.*`, and other document-database protocols).

# Motivation

Today `redis/json.rs` mixes four concerns in 1,900 lines: a JSON value model (`JValue`), a JSONPath engine
(`parse_path`, `resolve_*`, `ensure_parent`), a document type with command-shaped methods, and the wire
ops. The storage model is "the whole document is one value", and the only mutation strategy is
get + modify + put of the entire document.

That is a reasonable MVP and is deliberately kept as the physical strategy in this RFC (documents are
typically small and the kv value limit is far larger than any realistic document). But it does not
generalise to what we want next:

1. **Other consumers.** `FT.CREATE … ON JSON` / `FT.SEARCH` need secondary indexes over JSON documents, and
   we expect to speak other document-database protocols whose command vocabulary (queries, updates,
   index management) is far richer than `JSON.*`. They need a shared engine, not a second copy of the
   JSON module.
2. **Fast lookup by predicate.** `{"eq": {"foo": 1}}`-style lookups have no answer today other than a full
   scan with a parse per document.
3. **Richer in-document mutation.** `JSON.*` already needs set/delete/append/trim/insert/pop/arithmetic,
   each implemented by hand against `JValue`. Other protocols need the same primitives with different
   path syntax and targeting rules (all matches, filtered array elements, rename). One implementation of
   path traversal and mutation should serve all of them.
4. **A value model that does not lose information.** `JValue::Number(f64)` and a sorted `BTreeMap` cannot
   represent distinct integer widths, binary data, timestamps, ids, or field order. Other protocols use
   those. BSON is a superset of the JSON data model with those types and ordered fields, and a mature
   Rust implementation exists. Making BSON the one internal model avoids a JSON model + a BSON model +
   a conversion layer inside the engine.
5. **Collections.** In a flat LSM keyspace, "the set of documents that an index covers" needs a name and a
   key range. Indexes, scans, and catalog data all belong to a collection.
6. **Correctness.** Once an index exists there are two representations of one fact. Keeping them in step
   is the hard part and must be designed once, in one place, not per command.

Non-goals:

* A query language, query parser, aggregation or pipeline engine. The crate offers primitives (paths,
  predicates, cursors, index scans) that such engines would be built from, and a deliberately minimal
  access-path chooser. Richer planning and execution live above it.
* Changing the behaviour of any existing command, including its error strings, number formatting, key
  ordering, and TTL behaviour (see below).
* Sub-document physical storage (path-per-key). Documents remain one record each (see Alternatives).
* Replacing `RedisStore`/`DbOp` batching, `WATCH`, or `MULTI/EXEC`. The crate runs *inside* a transaction
  the host owns.

# Guide Implementation

Think of the crate as three layers, with the host (the Redis layer) on top:

```
  redis/json.rs  — arg parsing, JSONPath parser, error strings, Go-style JSON rendering, RESP replies
        │   uses
        ▼
  document       — Collection: records, mutations, indexes, predicates, cursors
        │   uses
        ▼
  kv             — Tx / KeyValueIterator over SlateDB or Fjall
```

## The concepts

**Document.** A `bson::Document`: an ordered map of field name → `Bson` value, nested to any depth.
`Bson` aliased as `Value`. A document has an opaque `DocumentId` (bytes) which is *not* part of the body; for
Redis it is the key name.

**Collection.** A named container of documents plus its index catalog. A `Collection` is a cheap handle
(`Collection::open(keyspace, options)` does no I/O) bound to a `Keyspace`: the pair of physical key
prefixes the host gives it (below). Redis uses one collection per DB number.

**Path.** A `DocumentPath` is a sequence of segments (`Field`, `Index`, `Wildcard`, `Descend`). The crate
resolves and mutates *paths*, never path *strings*. A `PathLanguage` turns a protocol's syntax into a
`DocumentPath`; the Redis adapter provides JSONPath, and a dotted-notation language is the second
implementation we expect.

**Mutation.** A value describing an in-document change: `Set`, `Remove`, `Arithmetic`, `String`, `Array`,
`Rename`, each aimed at a path. Applying mutations returns what changed and any value the command wants to
reply with (new length, popped element, new number).

**Predicate.** A tree of comparisons over paths (`Eq`, `Lt`, `Exists`, `And`, `Or`, `Any`, …). Used for
filtering, for deciding whether a *partial* index covers a document, and for predicate-based array edits.

**Index.** A definition (fields, options, lifecycle state) plus entries in the keyspace. Only `Ready`
indexes are used to answer reads; all non-`Dropping` indexes are maintained by every write.

## What using it looks like

Creating and querying (Rust, as a host would):

```rust
let products = Collection::open(keyspace, CollectionOptions::default());

// insert — inside a tx the host already holds
products.insert(tx, &id("p1"), doc!{ "name": "earbuds", "price": 25, "tags": ["audio", "usb"] }).await?;

// in-document mutation: several changes, one read, one write
let out = products.update(tx, &id("p1"), &[
    Mutation::new(path("price"), Op::Arithmetic(Arith::Add(Value::Int32(5)))),
    Mutation::new(path("tags"), Op::Array(ArrayOp::Push(vec!["wireless".into()]))),
], UpdateOptions::default()).await?;
// out.changes = [price: 25 -> 30, tags: [..2] -> [..3]]

// an index, then a filtered read that uses it
products.create_index(tx, IndexSpec::ordered("by_tag").field(path("tags"))).await?;   // multikey: one entry per tag
let mut cur = products.find(tx, Filter::new(Predicate::eq(path("tags"), "audio"))).await?;
while let Some(doc) = cur.next().await? { /* … */ }
```

And what that does to the keyspace (Redis host, DB 0, doc id `p1`; `R` and `I` defined below):

```
R "p1"                                   -> [rev 3][BSON {name, price:30, tags:[audio,usb,wireless]}]
I 02 <idx 1> <str "audio">    "p1"       -> 00
I 02 <idx 1> <str "usb">      "p1"       -> 00
I 02 <idx 1> <str "wireless"> "p1"       -> 00
```

## How engineers should think about it

* The crate never commits. Every method takes the host's `&dyn Tx`. A document write and all its index
  maintenance are therefore atomic with whatever else the host does in that transaction.
* **Index entries are an optimisation, never the authority.** Every indexed read fetches the document and
  re-checks the predicate. Entries may be *stale* (point at a document that no longer matches) but must
  never be *missing*. This one invariant lets us be relaxed about lazy expiry and about index builds, and
  strict about write paths.
* The Redis adapter stays thin and protocol-shaped: it parses arguments and paths, calls the crate, and
  maps the crate's *structured* errors to the exact error strings clients see today.

# Reference Implementation

## Crate layout

```
src/document/
  Cargo.toml          deps: kv, bson, async-trait, thiserror, bytes (no redis, no slatedb/fjall)
  lib.rs
  model/    value.rs  path.rs  resolve.rs  compare.rs
  mutation/ op.rs  apply.rs  change.rs
  predicate/ ast.rs  eval.rs
  record.rs           record header + body codec, legacy hook
  keyspace.rs         Keyspace, key construction
  collection.rs       Collection: CRUD, rename, scan, find
  index/    definition.rs  catalog.rs  kind.rs  encoding.rs  maintain.rs  build.rs  scan.rs  plan.rs
  error.rs
```

Workspace: add `src/document` to `members`; `redis` depends on `document`. `document` depends on `kv` only.

## Value model

```rust
pub use bson::{Bson as Value, Document};
pub struct DocumentId(Bytes);          // opaque; the host decides what it means
```

* In-memory work uses `bson::Document` directly; encode/decode happens once per record read/write, at the
  storage boundary. (`RawDocument` can be used later for projection without a full decode.)
* **Missing and null are distinct**: resolution returns *no result* for a missing path and
  `Value::Null` for an explicit null. Predicates, index extraction and index key encoding all keep the
  distinction.
* The crate imposes no size limit on a document; a host that has one enforces it.
* Comparison is a policy, not a property of `Value`:

```rust
pub trait ValueComparator: Send + Sync {
    fn compare(&self, a: &Value, b: &Value) -> Ordering;
    /// Order-preserving bytes: `a.cmp(b) == encode(a).cmp(encode(b))` must hold. Index keys are built from this.
    fn encode_sort_key(&self, v: &Value, out: &mut Vec<u8>) -> Result<(), UnsupportedType>;
}
```

  The default `NaturalOrder` defines a total order over types (rank: missing < null < numbers < strings <
  documents < arrays < binary < id < bool < datetime/timestamp …) and compares all numeric types by
  mathematical value. Collation is a future comparator.
* Types the encoder does not yet support (e.g. decimal128) make index *writes* fail with a typed error
  rather than being silently skipped, because a skipped value is a missing entry.

## Paths

```rust
pub enum Segment { Field(String), Index(i64) /* negative counts from the end */, Wildcard, Descend }
pub struct DocumentPath(Vec<Segment>);

pub trait PathLanguage {
    type Error;
    fn parse(&self, input: &str) -> Result<DocumentPath, Self::Error>;
}
```

The **resolver** returns matches *with provenance*, not a bare `Vec<&Value>`:

```rust
pub struct Match<'a> { pub value: &'a Value, pub at: ConcretePath }   // ConcretePath = Vec<Step{Field|Index}>
pub fn resolve<'a>(doc: &'a Document, path: &DocumentPath, opts: ResolveOptions) -> Vec<Match<'a>>;
```

`ConcretePath` records which array element each match came from. That is what makes array-correlated
predicates (below), multikey extraction, positional updates and `Change` records possible; a flattened
value list cannot support them.

`ResolveOptions` carries the few semantic knobs where path languages legitimately differ:

* `array_traversal`: `Explicit` (a field step applied to an array finds nothing; the path must say
  `[*]`/`Wildcard`) or `Implicit` (a field step applied to an array applies to each element). JSONPath uses
  `Explicit`; dotted notation is expected to need `Implicit`.
* `field_order`: object iteration order for `Wildcard`/`Descend` (see Redis compatibility).

Resolution and mutation are separate code paths: mutation needs a concrete, creatable location
(`create_parents`, array bounds) and rejects ambiguity according to the mutation, which resolution does not.

## Mutations

```rust
pub struct Mutation { pub path: DocumentPath, pub op: Op }

pub enum Op {
    Set    { value: Value, when: SetWhen /* Always | IfMissing | IfPresent */, create_parents: bool },
    Remove,                                         // object field, or array element (later elements shift down)
    Clear,                                          // empty the container / zero the number
    Arithmetic { op: Arith /* Add(n) | Mul(n) | Min(n) | Max(n) */, missing: Missing /* Error | AsZero */ },
    String(StringOp),                               // Append(s)
    Array(ArrayOp),                                 // Push(vals) | Insert{at, vals} | Pop{at} | Trim{start, stop}
                                                    //   | AddToSet(vals) | Pull(Predicate)
    Rename { to: DocumentPath },
}
```

* **Targeting.** A mutation applies to *every* match of its path (a literal path has one). The crate
  reports how many targets each mutation hit; hosts that must forbid wildcards (`JSON.*` does) reject them
  before calling.
* **Atomicity.** `update` loads the record once, applies all mutations in order to the in-memory document,
  and writes once. Any error aborts the whole call with no write.
* **Result.** Each applied mutation yields the values the caller may want back:

```rust
pub struct UpdateOutcome {
    pub revision: u64,
    pub changes: Vec<Change>,           // one per modified location, in application order
    pub outputs: Vec<Vec<Value>>,       // per mutation, per target: new number / new length / popped element
}
pub struct Change { pub at: ConcretePath, pub old: Option<Value>, pub new: Option<Value> }
```

  `Change` drives incremental index maintenance below, and is the hook for change feeds and cache
  invalidation later. `Arithmetic` follows numeric promotion for the operand types (double + double stays
  double, which is what `JSON.NUMINCRBY` relies on).
* **Errors** are structured (`PathError::{NotFound, IndexOutOfRange, TypeMismatch{expected, found},
  Ambiguous, UnsupportedSegment, …}`); this is what lets the adapter reproduce today's error strings
  exactly.
* **Extension point.** `UpdateOptions { context: MutationContext }` reserves room for variables and
  per-call array filters (so filtered-element updates can be added without changing `Mutation`).

## Predicates and filtering

```rust
pub enum Predicate {
    Eq(DocumentPath, Value), Ne(..), Lt(..), Lte(..), Gt(..), Gte(..),
    In(DocumentPath, Vec<Value>),
    Exists(DocumentPath, bool),
    Type(DocumentPath, BsonType),
    And(Vec<Predicate>), Or(Vec<Predicate>), Not(Box<Predicate>),
    /// Some single element of the array at `path` satisfies `inner` (inner paths are relative to that element).
    Any(DocumentPath, Box<Predicate>),
}
```

Example, using JSON-like documents. Given

```json
{ "items": [ {"sku": "a", "qty": 1}, {"sku": "b", "qty": 9} ] }
```

`And[Eq(items.sku, "a"), Gt(items.qty, 5)]` matches (the two conditions are satisfied by *different*
elements), while `Any(items, And[Eq(sku, "a"), Gt(qty, 5)])` does not (no single element satisfies both).
Both are expressible only because the resolver preserves element identity.

* Evaluation takes a `ValueComparator` and an `ArrayMode` (`Explicit`/`Implicit`, as for paths). Under
  `Implicit`, a comparison against an array-valued path matches if the array itself or any element
  matches; this is the same rule multikey indexes assume when they index each element.
* `Eq(path, Null)` matches explicit null only under `Explicit` null semantics; matching missing as well is
  a flag on the comparison (`null_matches_missing`), so a protocol can pick.
* The crate evaluates predicates but does not parse any query language. Protocol adapters translate their
  own syntax (`@name:(earbuds)`, `{"eq": {"foo": 1}}`, …) into `Predicate`.

## Key layout

The **host supplies two prefixes**; the crate appends fixed suffixes and never writes outside them:

```rust
pub struct Keyspace {
    pub records: Vec<u8>,    // R: record key = R ++ document id
    pub internal: Vec<u8>,   // I: catalog + index entries
    pub record_meta: u8,     // metadata byte stamped on every record entry
}
```

```
R ++ <id>                                  -> record
I ++ 00                                    -> collection record  (format, options, next_index_id)
I ++ 01 ++ <index id: u32 BE>              -> index definition   (spec, state, index format version)
I ++ 02 ++ <index id: u32 BE> ++ <key>      -> index entry
```

**Record value:**

```
01                 record format marker (a byte that cannot start JSON text)
<revision: u64 BE> bumped on every write of the record; used for compare-and-set
<BSON document bytes>
```

The marker is what separates new records from legacy JSON-text values written by earlier Invar versions
(see Compatibility). Record format versioning is independent of index format versioning, which lives in
each index definition.

**Index entry keys** are built from the comparator's order-preserving bytes:

```
non-unique:  I 02 <idx> <enc(f1)> <enc(f2)> … <doc id>      -> 00   (reserved byte; room for covered values)
unique:      I 02 <idx> <enc(f1)> <enc(f2)> …                -> <doc id>
```

Per-field encoding is `tag ++ payload`, self-delimiting so fields can be concatenated and the document id
can follow raw:

| Type | Tag order (ascending) | Payload |
|---|---|---|
| missing | lowest | none |
| null | | none |
| number (all numeric types) | | order-preserving transform of the value (sign-flipped IEEE-754 for the double, plus an exact-integer tiebreak so large `i64`s don't collide); `-0` = `0`; NaN sorts first |
| string | | UTF-8 with `00` escaped as `00 FF`, terminated `00 01` |
| document / array | | recursive encoding of the members, terminated |
| binary | | escaped bytes, terminated |
| id, boolean, datetime, timestamp | | fixed width, big endian, sign bit flipped where signed |

A **descending** field is the bitwise complement of its whole encoding (valid because encodings are
prefix-free). `kv` iterators are forward-only, so descending order is achieved by encoding, not by reverse
scans. Property tests assert `compare(a, b) == enc(a).cmp(enc(b))` over generated values of every type.

Why unique indexes drop the id from the key: two concurrent transactions inserting different documents with
the same unique value then write the *same key*, and `kv`'s write–write conflict detection turns what would
otherwise be a phantom-read race under snapshot isolation into an ordinary `Error::Conflict` for the loser,
without needing SSI.

## Collections

```rust
pub struct Collection { /* keyspace, options, comparator, cached catalog */ }

pub struct CollectionOptions {
    pub field_order: FieldOrder,                   // Insertion (default) | Sorted
    pub comparator: Arc<dyn ValueComparator>,
    pub legacy_decoder: Option<Arc<dyn LegacyDecoder>>,
}
```

`FieldOrder::Sorted` makes the crate keep every object's fields in byte-lexicographic order, including when
a mutation adds a field (insert at the sorted position), so that iteration (`Wildcard`, `Descend`, output)
is deterministic. Collection options are written to the collection record on first use and are checked on
open.

Methods (all take `tx: &dyn Tx`; none commits):

```rust
// documents
async fn get(&self, tx, id) -> Result<Option<Stored>>;            // Stored { id, revision, doc, ttl }
async fn get_many(&self, tx, ids: &[DocumentId]) -> Result<Vec<Option<Stored>>>;
async fn insert(&self, tx, id, doc, opts: WriteOptions) -> Result<Stored>;   // AlreadyExists unless opts.replace
async fn put(&self, tx, id, doc, opts: WriteOptions) -> Result<Stored>;      // upsert
async fn update(&self, tx, id, muts: &[Mutation], opts: UpdateOptions) -> Result<UpdateOutcome>;
async fn delete(&self, tx, id) -> Result<bool>;
async fn rename(&self, tx, from, to, opts) -> Result<bool>;       // moves the record (keeps TTL) and its index entries

pub struct WriteOptions { ttl: TtlPolicy /* Clear | Keep | Set(Duration) */, expected_revision: Option<u64>, replace: bool }

// reads over many documents
async fn scan(&self, tx, spec: ScanSpec) -> Result<Box<dyn DocumentCursor>>;
async fn index_scan(&self, tx, spec: IndexScanSpec) -> Result<Box<dyn IndexCursor>>;
async fn find(&self, tx, filter: Filter) -> Result<Box<dyn DocumentCursor>>;
fn explain(&self, filter: &Filter) -> Plan;

// index DDL
async fn create_index(&self, tx, spec: IndexSpec) -> Result<IndexId>;
async fn drop_index(&self, tx, id: IndexId) -> Result<()>;
async fn indexes(&self, tx) -> Result<Vec<IndexDefinition>>;
async fn index_stats(&self, tx, id) -> Result<IndexStats>;       // entries, estimated bytes
```

* **Expiry.** A record whose `Item::is_expired()` is true is treated as absent. A write path that
  encounters an expired record removes its index entries before replacing or deleting it.
* **Records of other types.** `scan` skips entries whose metadata byte is not `record_meta`. Redis public
  keys of every type share `<db>:`; `scan` (and so a future `FT.SEARCH` with no usable index) filters by
  type byte, and takes an id prefix bound (`FT.CREATE … PREFIX`) to narrow the range. A *point* operation
  (`get`, `update`, …) on a key whose type byte differs returns `WrongType` (typed error; the adapter maps
  it to the existing Redis `WRONGTYPE`).
* **Cursors** are first-class (`async fn next(&mut self) -> Result<Option<Stored>>`) so a future pipeline
  can stream rather than materialise. They wrap `KeyValueIterator`, hold the `Tx`'s snapshot, and accept an
  optional residual `Predicate`, `limit`, and a resume key.
* **Optimistic concurrency.** `expected_revision` gives compare-and-set on top of snapshot isolation
  without reading the body twice.

## Indexes

### Definition and lifecycle

```rust
pub struct IndexDefinition {
    pub id: IndexId,                  // u32, allocated from the collection record
    pub name: String,
    pub kind: String,                 // "ordered" is built in
    pub fields: Vec<IndexField>,      // { path: DocumentPath, direction: Asc|Desc }
    pub options: IndexOptions,        // unique, sparse, partial_filter: Option<Predicate>, comparator name
    pub multikey: Vec<bool>,          // per field; monotonic, set when an array is first indexed
    pub state: IndexState,            // Building | Ready | Dropping
    pub format: u16,                  // entry encoding version
}
```

Extension is by kind, so type-specific indexing (a `FT.CREATE … TAG/TEXT/NUMERIC` attribute, say) can
reuse the catalog, maintenance, and transactional machinery without the core knowing their semantics:

```rust
pub trait IndexKind: Send + Sync {
    fn name(&self) -> &'static str;
    /// All entry keys (excluding the doc id) this document contributes to the index.
    fn extract(&self, doc: &Document, def: &IndexDefinition, cx: &ExtractCx) -> Result<Vec<Vec<u8>>, IndexError>;
    /// Turn a typed scan request into a key range under `I 02 <idx>`.
    fn bounds(&self, def: &IndexDefinition, req: &ScanRequest) -> Result<KeyRange, IndexError>;
}
```

Only `ordered` ships in this RFC.

### Extraction

For each indexed field: resolve the path (with `Implicit` array traversal), encode each distinct resolved
value, de-duplicated per document (one entry per distinct value per document).

* **Missing vs null.** A missing field encodes as `missing`, an explicit null as `null`. A **sparse** index
  skips documents where *all* indexed fields are missing. A **partial** index skips documents that do not
  satisfy `partial_filter`.
* **Multikey.** An array at an indexed path contributes one value per element (an empty array contributes
  one `[]` entry so that a non-sparse index covers every document). The first time an array is indexed for
  a field, `multikey[field]` is set in the definition, in the same transaction.
* **Compound + multikey.** The entry set is the cross product of the per-field value sets *only when at
  most one field of the index is multikey in this document*. If two indexed fields both resolve through
  arrays, the write fails with `IndexError::ParallelArrays` rather than indexing combinations that never
  co-occurred. Correlating them (resolving both through the *same* array elements) is possible because
  extraction keeps provenance, and is left to a later version.
* Fields of an unsupported type fail the write (see Value model).

### Maintenance (the part that must be right)

Every write path runs the same routine inside the host's `tx`:

```
old = existing record (if any)           // already read for update/delete
new = resulting document (if any)
for each index not Dropping, if affected(index, changes):
    k_old = extract(old)   k_new = extract(new)
    for k in k_old \ k_new: tx.delete(entry(k))
    for k in k_new \ k_old: tx.set(entry(k))        // unique: see below
write/delete the record (revision+1)
```

* **Affected** means some `Change.at` is an ancestor or descendant of an indexed path (or of a path the
  partial filter reads). Updates that touch only other fields skip the index entirely. `insert`, `put`
  and `delete` treat the whole document as changed.
* Set differencing, not delete-all/insert-all: entries that don't change aren't rewritten.
* **Unique.** Inserting key `k` does a point get; if present and owned by another document, verify the
  owner is still live *and* still extracts `k` (entries may be stale); if so, `DuplicateKey`, otherwise
  overwrite the stale entry. A concurrent insert of the same value collides on the same key at commit
  (`Error::Conflict`).
* **Invariants.**
  1. *No missing entries*: every live record has an entry for each key each non-`Dropping` index extracts
     from it.
  2. *Stale entries allowed*: an entry may refer to a document that has been deleted, expired, or changed.
  3. *Readers verify*: every indexed read fetches the record and re-evaluates the full predicate, so (2)
     never changes a result.
  4. *Planner uses only `Ready` indexes.*
  Stale entries are garbage, not corruption; a `verify`/`compact` pass can later drop them.
* **Records changed outside the crate.** The invariants hold only if no code replaces or removes a record
  entry without going through the crate while the collection has indexes. Redis key-level commands that
  can do so for a JSON-typed key (`DEL`/`UNLINK`, `RENAME`, overwrite by `SET`-like commands, `MOVE`,
  `FLUSHDB`, expiry) must call `Collection::delete`/`rename` (or, for `FLUSHDB`, drop `I`) for such keys.
  Today no Redis collection has indexes, so this is a no-op to introduce; it is a hard requirement of the
  `FT.CREATE` work, which should land with a test per command.

### Build and drop

* `create_index` registers the definition in state `Building`. From that commit, every write maintains it
  (rule: all non-`Dropping` indexes). A backfill then scans the collection in batches, each batch its own
  transaction that extracts and sets entries (idempotent), and finally flips the state to `Ready`. Because
  of the "stale entries allowed" rule, a backfill batch racing a concurrent update can at worst write a
  stale entry, never lose one.
* `drop_index` sets `Dropping` (writers stop maintaining it, readers stop using it), then
  `drop_prefix`-style range deletion removes the entries and definition.
* **DDL vs in-flight writes.** A write transaction that read the catalog *before* an index was registered
  and commits *after* it would leave that document unindexed (snapshot isolation does not conflict
  read-only catalog reads with the DDL write). Invar is single-writer per store, so the crate provides a
  `CatalogGate` (an in-process async `RwLock`) per collection: document-mutating transactions hold the read
  side from before `begin` until commit/discard, and index registration holds the write side. The host
  integration point is where `conn.rs` begins mutating transactions; it only needs wiring when the first
  index can exist. If the single-writer assumption is ever relaxed, the replacement is fence keys written
  by every mutation (see Unresolved Questions).
* The catalog is cached in the `Collection` handle and revalidated against the collection record's
  `catalog_epoch` read inside each write transaction; a changed epoch reloads it.

## Access paths: minimal planner and explain

The planner picks an *access path* and a *residual*; it is not an optimiser.

```rust
pub enum Plan {
    FullScan   { residual: Option<Predicate> },
    IndexScan  { index: IndexId, bounds: IndexBounds, residual: Option<Predicate> },
    // later: Intersect(Vec<Plan>), Union(Vec<Plan>)
}
pub struct IndexBounds { pub eq_prefix: Vec<Value>, pub range: Option<(Bound<Value>, Bound<Value>)> }   // leading equalities, then one range
```

Rules (phase 3): split the filter into top-level conjuncts; for each `Ready` index whose leading field(s)
are constrained by `Eq`/`In`/range conjuncts, score by length of equality prefix, prefer unique; pick the
best, everything else is residual. An index is eligible only if it can return a **superset** of matches:
a sparse index cannot serve a predicate that matches missing fields; a partial index only serves a filter
that syntactically implies its `partial_filter`; a multikey index is valid only for predicates evaluated
with `Implicit` array semantics. `Or`/`Not` fall back to `FullScan` until `Union` is added. `explain` returns
the `Plan`, and `IndexStats` is exposed so a smarter planner above the crate can do cost-based choice.

`index_scan` is also public: a query engine that plans for itself can scan index ranges directly (inclusive
and exclusive bounds, prefix scans on compound indexes, cursor resume). Entry values are not invertible in
version 1 of the encoding (numbers are normalised), so `IndexHit` returns the document id and raw key but
not decoded values; covered reads are therefore not promised yet, and the reserved value byte leaves room
for them.

## Transactions and errors

* All methods use the `Tx` passed in, so isolation is whatever the host chose (`Snapshot` by default;
  `WATCH` use of SSI keeps working because the crate reads and writes through the same `Tx`).
* `kv::Error::Conflict` is passed through unchanged; callers already retry/handle it.
* `document::Error` is an enum: `Kv(kv::Error)`, `Corrupt{..}`, `AlreadyExists`, `WrongType`,
  `RevisionMismatch`, `Path(PathError)`, `Index(IndexError)` (`DuplicateKey`, `ParallelArrays`,
  `UnsupportedType`, `NotReady`), `Codec(..)`.

## Redis integration

### Placement

Per DB `n`: `Keyspace { records: "<n>:", internal: "+<n>:" ++ 16×00, record_meta: ValueType::Json }`.

* The record key is **exactly the existing public key** `<n>:<name>` with `ValueType::Json` metadata, so
  `SCAN`, `KEYS`, `EXISTS`, `TYPE`, `DBSIZE`, `TTL`/`EXPIRE`, `WATCH`, `DEL`, `RENAME` continue to see an
  ordinary public key and need no change in this RFC.
* The internal prefix reuses RFC 0001's physical-namespace shape (`+<db>:` followed by 16 bytes) with a
  reserved all-zero namespace, which cannot collide with a randomly generated one and is already covered by
  the `+<db>:` prefix that `FLUSHDB` drops. (Open question below.)

### What stays in `redis/json.rs` and what moves

Stays: argument parsing, `FPHA`, the JSONPath parser (as a `PathLanguage` implementation producing a
`DocumentPath`), `JsonResult`/`WireOp`, Go-compatible JSON rendering (`go_json_number`, `push_json_string`,
`serialize_value`), RESP rendering, and the mapping from `document::Error` to today's error strings.

Moves out: `JValue`, `parse_json`, the path engine (`resolve_*`, `ensure_parent`), `JsonDocument`,
`load_doc`/`write_doc`, and the direct `tx.get/set`.

| Command | Crate call |
|---|---|
| `JSON.SET` | `put` (root) or `update` with `Op::Set { when, create_parents: true }` for `NX`/`XX`/path forms |
| `JSON.GET`, `TYPE`, `ARRLEN`, `OBJKEYS`, `OBJLEN`, `STRLEN`, `RESP`, `ARRINDEX` | `get` + `resolve` |
| `JSON.MGET` | `get_many` + `resolve` |
| `JSON.DEL` / `CLEAR` | `update` with `Remove` / `Clear` (root `DEL` clears the root to `null`, as today) |
| `ARRAPPEND` / `ARRINSERT` / `ARRPOP` / `ARRTRIM` | `Op::Array(Push / Insert / Pop / Trim)`, outputs carry new length / popped element |
| `NUMINCRBY` / `NUMMULTBY` | `Op::Arithmetic` (`Add` with `missing: AsZero`; `Mul` with `missing: Error`) |
| `STRAPPEND` | `Op::String(Append)` |

### Compatibility: behaviour that must not change

The existing integration suite under `./test` is the acceptance test. The places where BSON would
naturally behave differently, and how the adapter keeps today's behaviour:

1. **Root can be any JSON value; a BSON document root is always an object.** The adapter stores
   `{ "v": <root> }`. The JSONPath `PathLanguage` maps `$` to `[Field("v")]`, `$.a.b` to
   `[Field("v"), Field("a"), Field("b")]`, so the envelope is invisible to commands (and to future `FT`
   index paths).
2. **Numbers.** Every JSON number is converted to BSON `Double`, mirroring today's `f64`. Integers above
   2^53 are rounded as they are now; output formatting is the unchanged Go-style renderer. (Preserving
   integer widths would be an observable improvement and is not part of this change.)
3. **Key order.** Today objects are `BTreeMap`s, so output and wildcard iteration are sorted. The Redis
   collection uses `FieldOrder::Sorted`; duplicate keys in input resolve last-wins, as `serde_json` does.
4. **TTL.** Today the JSON commands write `Entry::new(..)` and therefore *clear* a key's TTL on every
   modification (unlike the other types, which use `set_keeping_ttl`). The adapter passes
   `TtlPolicy::Clear` to preserve that exactly; switching to `Keep` is a one-word follow-up once decided
   (Unresolved Questions).
5. **Error strings, `WRONGTYPE`, `NX`/`XX` semantics, wildcard/recursive restrictions on `SET`/`DEL`** are
   implemented in the adapter on top of the crate's structured errors; the crate offers the capability
   (multi-target mutation), the adapter keeps today's limits.
6. **Missing keys.** Reads of an absent or expired key behave as today.
7. **Invalid stored JSON** remains unreachable through the wire path (input is validated before it is
   converted).

### Existing data and rollout

Existing values are JSON text under metadata `Json`. Because a record starts with the marker byte `01`, which
cannot begin JSON text, the two are distinguishable. The Redis collection installs a `LegacyDecoder`
that parses JSON text into `{ "v": … }`. Reads accept both; **every write converts the record to the new
format** (lazy, per key; no offline migration, no scan).

Downgrade: a binary that predates this change sees a new-format record as invalid JSON and replies with the
"invalid JSON document" error for that key (no data is lost or rewritten by reads; a whole-key overwrite such
as `SET` works). If that is not acceptable, use RFC 0001's two-release pattern (see Unresolved Questions).

## Testing

* **Acceptance:** the existing `JSON.*` cases under `./test` pass unmodified; `cargo test --workspace` and
  `./run-tests.sh`.
* **Differential test (temporary):** during the port, keep the old engine under `#[cfg(test)]` and run
  randomised command sequences through both, asserting identical replies and identical rendered
  documents; remove when confidence is high.
* **Golden bytes:** legacy JSON-text values still decode; new records have the expected header.
* **Unit/property:** resolver provenance; mutation outcomes and `Change` lists; sort-key order equals
  comparator order for every type including descending and compound; extraction (multikey de-dup, empty
  array, missing vs null, parallel arrays rejected); set-difference maintenance.
* **Index consistency under concurrency** (Fjall in-process): concurrent updates to the same document;
  concurrent unique inserts (exactly one wins); backfill racing writers (no missing entries); randomised
  mutation sequences checked against a full-scan oracle for every predicate after every step.

## Phasing

1. **Crate + port (no indexes):** model, paths, resolver, mutations, record format, collection CRUD +
   `rename`, legacy decoder, port `json.rs`. No observable change; this is the shippable unit.
2. **Single-field ordered indexes:** catalog, key encoding, maintenance, `Building/Ready`, `CatalogGate`,
   equality/range `index_scan`, unique.
3. **Filtering:** predicates, cursors, `find`/`explain`, multikey, compound, sparse/partial, `Or` via union
   and `And` via intersection.
4. **Additional kinds and comparators** (the `FT.*` attribute types; collation) and the Redis key-level
   command hooks.

# Drawbacks

* **A one-way on-disk change** for JSON keys (lazy, per key). Downgrade behaviour is "error on keys written
  by the new version", not data loss.
* **More machinery than the status quo for the same commands.** An envelope document, a header on every
  record, an adapter error-mapping layer, and a heavier dependency (`bson`) for functionality that mostly
  does not use BSON-only types yet.
* **Compatibility shims are real code:** forcing doubles, sorted fields and TTL-clearing means the new
  engine deliberately reproduces some behaviours (TTL loss, f64 rounding) we might otherwise choose to fix.
* **Scans over a shared prefix.** Redis public keys of all types share `<db>:`, so a collection scan reads
  and discards non-JSON entries. Acceptable for point commands and `PREFIX`-narrowed `FT` indexes, poor for
  an unnarrowed scan in a mostly non-JSON db.
* **Whole-document rewrite per mutation** remains. Large documents cost O(size) per `JSON.NUMINCRBY`.
* **Index maintenance has a correctness surface** (the invariants above, key-level Redis commands that must
  cooperate, the DDL gate) that no other Invar type has.
* The planner is intentionally weak; callers expecting a general query optimiser will need the layer above.

# Rationale / Alternatives

* **BSON as the one internal model** vs. a bespoke enum or `serde_json::Value`. A bespoke model means
  writing and maintaining comparison, encoding and codecs; `serde_json::Value` cannot hold ids, binary,
  dates, integer widths or field order without being worked around later. BSON is a superset of JSON
  data, has a maintained Rust crate, and its binary form is directly storable. JSON/RESP stays a boundary
  concern of the Redis layer.
* **Path-per-key (materialised subdocument) storage** (every leaf its own kv entry). Makes in-document
  updates O(path) but makes reads and scans O(leaves), explodes key count, complicates atomic multi-path
  mutation, and turns index maintenance into a diff over many keys. Not justified while documents are
  modest; the `Collection` API hides the physical form, so it can be revisited without touching callers.
* **Binary structural updates in place** (patching BSON bytes). A later optimisation behind `update`;
  compatible with this API because `update` already works on a mutation list.
* **Pluggable codec trait** (JSON, BSON as interchangeable serialisations of one model). Rejected: a second
  model in the engine is exactly the duplication we want to avoid, and the JSON↔BSON conversion needed by
  Redis is a boundary function, not a storage choice.
* **A single path string language in the crate.** Rejected: the syntaxes differ in how arrays and numeric
  field names behave, and the Redis one has exact error strings to preserve. Parsers stay in adapters; the
  crate shares only the model, resolution and mutation.
* **A high-level `find(predicate)` as the crate's main interface.** Kept only as a convenience over
  `scan`/`index_scan`/`explain`; the primary surface is the lower-level primitives so a real query engine
  can plan for itself.
* **Index entries as the source of truth / transactional index rebuild only.** Rejected in favour of
  "stale allowed, missing never, readers verify", which makes lazy expiry, online build and key-level
  Redis commands tractable.
* **One `kv` abstraction per crate / hiding `Tx`.** The crate uses `kv::Tx` directly: the isolation
  semantics (snapshot, write–write conflicts, SSI for `WATCH`) are exactly what index maintenance needs,
  and wrapping them would only lose information.
* **A new top-level key class for collection internals** instead of the reserved `+<db>:` namespace: more
  `FLUSHDB`/`drop_prefix` call sites for no benefit.

# Unresolved Questions

> **Open Question:** TTL on JSON mutation. The current JSON commands clear a key's expiry whenever they
> modify it, which differs from the other types (and from Redis). This RFC preserves it (`TtlPolicy::Clear`).
> Do you want that fixed as a follow-up (`Keep`), and should it be a separate change?

> **Open Question:** Numbers. This RFC converts all JSON numbers to `Double` to keep today's behaviour
> exactly. Should the Redis collection instead preserve integers (`Int32`/`Int64`) once this lands, accepting
> that integers above 2^53 would stop being rounded?

> **Open Question:** Rollout. Single release with lazy upgrade (as written) or RFC 0001's two-release
> read-then-write rollout so that a rollback is safe? The latter needs the crate to be able to write the
> legacy text form for one release.

> **Open Question:** Internal prefix. Reuse RFC 0001's `+<db>:` with a reserved all-zero namespace (as
> written), or add a new key class and a further `FLUSHDB` prefix drop?

> **Open Question:** DDL vs in-flight writes. The `CatalogGate` relies on the single-writer-per-store
> invariant. The alternative that does not (fence keys: every document mutation writes one of N small keys
> that index DDL also writes, so DDL conflicts with concurrent mutations) costs an extra write on every
> mutation. Is the in-process gate acceptable?

> **Open Question:** Other protocols' path and comparison semantics. The two knobs here (`array_traversal`
> and the numeric-field-name-as-index rule, plus type ordering in `NaturalOrder` and `null_matches_missing`)
> are placeholders chosen from first principles. When the second `PathLanguage`/comparator is built I will
> need the exact rules supplied rather than inferred.

> **Open Question:** Parallel arrays in compound indexes are rejected at write time. Is a hard error the
> right default, or should the write succeed and the index be marked not usable for those paths?

> **Open Question:** Reverse iteration. `kv` iterators are forward-only, so descending order comes from
> encoding. A reverse-scan capability in `kv` would allow `limit`-with-`sort` optimisations for single-field
> indexes; not required by this RFC.

> **Open Question:** Reclaiming stale index entries and the orphans left by lazy expiry (a `verify` pass, or
> removing entries at expiry time), as for RFC 0001's orphaned namespaces.

> **Open Question:** Crate name and location (`document` at `./src/document`) and whether the `bson` crate's
> licence and size are acceptable as a hard dependency of the default build.
