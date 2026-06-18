# Session and Cursor State — Implementation Specification

```yaml
provenance:
  urls_fetched_in_order:
    - https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/batch_cursor.go
    - https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/session/client_session.go
    - https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/session/server_session.go
    - https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/session/session_pool.go
    - https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/session/session_pool_test.go
    - https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/session/cluster_clock.go
    - https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/session/client_session_test.go
    - https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/session/server_session_test.go
    - https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/operation.go
      (truncated; key sections read via shell grep/sed)
    - https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/mongo/cursor.go
    - https://bsonspec.org/spec.html
    - https://github.com/mongodb/mongo-go-driver/tree/v2.6.1/x/mongo/driver/session
      (directory listing only)
```

---

## Overview

This document specifies the server-side state that the Go driver v2.6.1 expects
to exist between requests, covering:

1. **Cursor lifecycle** — what `cursorID` means, when it becomes invalid, and
   how the driver tracks cursor exhaustion.
2. **Logical session IDs** — the `lsid` field, server-session lifecycle, the
   session pool, cluster-time gossiping, and `operationTime` propagation.
3. **Which commands require a cursor response vs. inline results** — the
   complete classification table.

Prior documents in this series define the wire protocol
(`wire-protocol-messages.md`), the handshake (`04-connection-handshake.md`),
all core command payloads (`05-core-command-specifications.md`), and error
handling (`06-error-response-structure.md`). This document does not re-define
those structures; it focuses on the *stateful* aspects that must persist across
multiple round trips.

---

## 1. Cursor Lifecycle

### 1.1 What is a cursorID?

A cursor ID is a signed 64-bit integer (`int64`) assigned by the server when a
command produces a result set that does not fit in a single response, or when
the client requested a cursor interface (even for a small result set). The ID
identifies server-side state — the server retains the result set position and
the server-side cursor until it is exhausted or killed.

**Special value `0`**: A cursor ID of `0` is the sentinel meaning "cursor is
exhausted and no server-side state exists". The driver never sends a `getMore`
command when `cursorID == 0`.

From `batch_cursor.go`:
```go
func (bc *BatchCursor) Next(ctx context.Context) bool {
    if bc.firstBatch {
        bc.firstBatch = false
        return !bc.currentBatch.Empty()
    }
    if bc.id == 0 || bc.server == nil {
        return false    // cursor exhausted, no getMore
    }
    bc.getMore(ctx)
    return !bc.currentBatch.Empty()
}
```

### 1.2 Cursor ID BSON type

Cursor IDs are always BSON `int64` (type `0x12`). The driver's
`NewCursorResponse` enforces this strictly:

```go
case "id":
    id, ok := elem.Value().Int64OK()
    if !ok {
        return CursorResponse{},
            fmt.Errorf("id should be an int64 but it is a BSON %s", elem.Value().Type)
    }
    curresp.ID = id
```

A server **must** encode the cursor ID as BSON `int64`. Encoding it as `int32`
or `double` will cause the driver to return an error and abort the operation.

### 1.3 Cursor response sub-document (firstBatch)

Commands that produce a cursor return a top-level `cursor` document in the
response. The driver's `NewCursorResponse` parses the cursor sub-document
directly (not the outer response body). See `05-core-command-specifications.md`
for the full response shape. The fields parsed are:

| Field | BSON type | Required | Notes |
|---|---|---|---|
| `id` | Int64 (strict) | Yes | 0 = exhausted |
| `ns` | String | Yes | Must contain exactly one `.`; split into db + collection |
| `firstBatch` | Array | Yes | Documents from the first round trip |
| `postBatchResumeToken` | Document | No | Change stream resume token |

The `ns` field is split by the first `.`:
```go
database, collection, ok := strings.Cut(ns, ".")
if !ok {
    return CursorResponse{}, errors.New("ns field must contain a valid namespace, but is missing '.'")
}
```

The server must return the namespace in `"<db>.<collection>"` format. For
`listIndexes` the namespace uses the system collection form
`"<db>.$cmd.listIndexes.<coll>"`, and for `listCollections` it uses
`"<db>.$cmd.listCollections"`.

### 1.4 Cursor response sub-document (nextBatch)

`getMore` responses use `nextBatch` instead of `firstBatch`. The driver reads:

```go
batch, ok := response.Lookup("cursor", "nextBatch").ArrayOK()
if !ok {
    return fmt.Errorf("cursor.nextBatch should be an array but is a BSON %s", ...)
}
```

The server **must** use the key `"nextBatch"` (not `"firstBatch"`) in `getMore`
responses. The driver errors if `nextBatch` is missing or is not a BSON array.

### 1.5 Cursor exhaustion

The cursor is exhausted when:

1. The server returns `cursor.id == 0` in any response (initial command or any
   `getMore`). The driver sets `bc.id = 0` and will never issue another
   `getMore`.
2. The driver calculates that the `limit` has been reached (legacy operations
   only; modern cursors delegate this to the server).

When `cursor.id == 0`, the server has already released all server-side cursor
state. No `killCursors` is necessary.

### 1.6 When does a cursor become invalid?

A cursor becomes invalid (server-side state is gone) in any of these cases:

1. `cursor.id == 0` — server reports exhaustion.
2. `killCursors` command is sent by the client (see §1.8).
3. Server-side cursor timeout: the server expires idle cursors after its
   configured timeout period. The driver does not track this timeout; it simply
   receives an error (`CursorNotFound`, code 43) on the next `getMore`.
4. Network error on a `getMore`: in load-balanced mode, if a `getMore` produces
   a network error the driver sets `bc.id = 0` to prevent a follow-up
   `killCursors` on a broken connection (code path in `batch_cursor.go`
   `getMore`).
5. Session `endSession` without `killCursors`: implicit sessions are closed when
   the cursor is exhausted (`closeImplicitSession` called from `mongo/cursor.go`
   when `bc.ID() == 0`). A non-exhausted cursor that goes out of scope without
   `Close()` being called will leak the server-side state until the server
   times out the cursor.

### 1.7 getMore command

The driver issues `getMore` automatically when iterating. The `getMore` command
is sent to the **same database** and (nominally) the same server/connection as
the command that created the cursor. The command structure is defined in
`05-core-command-specifications.md`. Key behavioral rules from
`batch_cursor.go`:

- `getMore` is only sent when `bc.id != 0`.
- `batchSize` is omitted from `getMore` when the calculated value is `<= 0`.
- `maxTimeMS` is only included for `tailable awaitData` cursors (when
  `maxAwaitTime` is set on the cursor).
- `comment` is only included on wire version ≥ 9 (MongoDB 4.4+).
- `OmitMaxTimeMS: true` is set on the `getMore` operation — the automatically
  calculated `maxTimeMS` (from context deadline) is **never** appended to
  `getMore`; only the explicit `maxAwaitTime` value produces a `maxTimeMS`.

After receiving a `getMore` response:
1. The driver updates `bc.id` from `cursor.id`. If the new ID is `0`, the cursor
   is exhausted.
2. The driver updates `bc.currentBatch` from `cursor.nextBatch`.
3. `postBatchResumeToken` is updated if present.
4. If the cursor is now exhausted (`bc.id == 0`), the driver unpins any pinned
   connection (load-balanced mode).

### 1.8 killCursors command

Sent by `BatchCursor.KillCursor` when `Close()` is called while `bc.id != 0`
(i.e., the cursor has not been fully iterated). The command structure is defined
in `05-core-command-specifications.md`.

Behavioral rules:
- Never called when `bc.id == 0` or `bc.server == nil`.
- Always sends exactly one cursor ID per command (the driver calls `KillCursor`
  one ID at a time).
- The response body is ignored. Any `ok: 1` response is sufficient.
- Uses `Legacy: LegacyKillCursors` flag — see §1.9.
- Read preference is omitted (`omitReadPreference: true`).

### 1.9 Legacy operation flags

The `BatchCursor` uses `LegacyOperationKind` flags that affect how `getMore`
and `killCursors` are encoded. In the v2.6.1 driver these legacy paths may still
produce `OP_MSG` commands but with special handling. The flags are:

| Operation | Flag | Effect |
|---|---|---|
| `getMore` | `LegacyGetMore` | Sets `Legacy: LegacyGetMore`; driver skips session/readPref fields for legacy cursors |
| `killCursors` | `LegacyKillCursors` | Sets `Legacy: LegacyKillCursors`; omits read preference |

For a modern (≥ wire version 8) server, both `getMore` and `killCursors` are
sent as normal OP_MSG commands; the `Legacy` flag primarily affects whether
session fields are included.

### 1.10 Cursor pinning (load-balanced mode)

When the server is behind a load balancer (`logicalSessionTimeoutMinutes` is not
tracked; `ServiceID` is present):

- If a cursor's initial response has `cursor.id != 0`, the connection that
  received that response is **pinned to the cursor**.
- All subsequent `getMore` and `killCursors` commands for that cursor are sent
  on the **same pinned connection**.
- When the cursor is exhausted (`cursor.id == 0` on a `getMore`), or when
  `Close()` is called, the connection is unpinned.
- If a network error occurs during `getMore` in LB mode, the cursor ID is set
  to `0` to prevent a `killCursors` on the broken connection.

For non-load-balanced deployments, cursor commands are sent to the same
server (using normal connection pool selection) but not pinned to a specific
TCP connection.

### 1.11 Cursor and batch size

`batchSize` affects how many documents the server returns per batch. The driver
tracks the following:

| Variable | Type | Notes |
|---|---|---|
| `bc.batchSize` | int32 | Sent as `batchSize` in `getMore` when > 0 |
| `bc.limit` | int32 | Legacy limit; when nonzero, driver calculates `getMore` batch size to not exceed remaining count |
| `bc.numReturned` | int32 | Running total of documents returned; used for limit calculation |

The batch-size calculation (`calcGetMoreBatchSize`):
- If `limit == 0`: use `batchSize` directly.
- If `limit != 0` and `numReturned + batchSize >= limit`: use `limit - numReturned`.
- If the result would be `<= 0`: close the cursor without another `getMore`.

---

## 2. Logical Session IDs

### 2.1 What is a logical session?

A logical session is a client-managed concept that groups related operations for
the server. Each session has a **session ID** (`lsid`) which the server uses to
associate operations with server-side session state (e.g., for retryable writes
and transactions). The Go driver also maintains a **transaction number**
(`txnNumber`) per session for ordering writes.

Sessions are supported when the server response to `hello`/`isMaster` includes
`logicalSessionTimeoutMinutes` as a non-null integer. See
`04-connection-handshake.md` for how this field is parsed.

The `sessionsSupported` predicate (in `operation.go`) is:
```go
func sessionsSupported(wireVersion *description.VersionRange) bool {
    return wireVersion != nil
}
```
Sessions are supported as long as the wire version range is known (i.e., after
a successful handshake). The server must also advertise
`logicalSessionTimeoutMinutes` for **explicit sessions** to be attached to
commands:

```go
// If client != nil and IsImplicit==false and SessionTimeoutMinutes==nil → error
if client != nil && !client.IsImplicit && desc.SessionTimeoutMinutes == nil {
    return nil, fmt.Errorf("current topology does not support sessions")
}
// If sessions not supported by wire version or server has no session timeout,
// don't add lsid at all
if client == nil || !sessionsSupported(desc.WireVersion) || desc.SessionTimeoutMinutes == nil {
    return dst, nil
}
```

### 2.2 The `lsid` field structure

Session IDs are a BSON sub-document containing a single field `id` which holds
a UUID (Binary, subtype 4):

```bson
{
  "lsid": {
    "id": BinData(4, "<16 random bytes>")
  }
}
```

From `server_session.go`:
```go
const UUIDSubtype byte = 4  // BSON binary subtype for UUID

func newServerSession() (*Server, error) {
    id, err := uuid.New()  // generates 16 random bytes (v4 UUID)
    idx, idDoc := bsoncore.AppendDocumentStart(nil)
    idDoc = bsoncore.AppendBinaryElement(idDoc, "id", UUIDSubtype, id[:])
    idDoc, _ = bsoncore.AppendDocumentEnd(idDoc, idx)
    return &Server{SessionID: idDoc, LastUsed: time.Now()}, nil
}
```

The `lsid` field value is the entire `{"id": BinData(4, ...)}` document,
appended as an embedded document element.

### 2.3 When is `lsid` sent?

`lsid` is appended to every command that has a non-nil `session.Client` **and**
where both of the following are true:
1. `sessionsSupported(desc.WireVersion)` is true (wire version is known).
2. `desc.SessionTimeoutMinutes != nil` (server advertised session timeout).

From `addSession` in `operation.go`:
```go
dst = bsoncore.AppendDocumentElement(dst, "lsid", client.SessionID)
```

The `SessionID` is the `{"id": BinData(4, ...)}` document built once when the
server session is created and reused for all operations in that session.

Every command that goes through the `Operation.Execute` path and has a
`Client` session will include `lsid`.

### 2.4 Session types: implicit vs. explicit

| Type | `IsImplicit` | Created by | Lifetime | Notes |
|---|---|---|---|---|
| Implicit | `true` | Driver internally for each operation | Single command or cursor iteration | No server session checked out until after connection checkout; reused for cursor lifetime |
| Explicit | `false` | Application calls `StartSession()` | Controlled by application via `EndSession()` | Errors if server doesn't support sessions |

Implicit sessions: `NewImplicitClientSession` creates a `Client` with
`IsImplicit: true` and `Server = nil` (deferred checkout). The `SetServer()`
call happens after a connection is obtained, limiting implicit sessions to ≤
`maxPoolSize`. When a cursor backed by an implicit session is exhausted,
`closeImplicitSession()` in `mongo/cursor.go` calls `EndSession()`.

### 2.5 Server-session pool lifecycle

The server-session pool (`session.Pool`) is a LIFO (last-in-first-out) free
list of `Server` objects:

- **GetSession**: Pops from the head of the list, skipping expired sessions.
  If the list is empty or all sessions are expired, creates a new server
  session.
- **ReturnSession**: Pushes to the head of the list after checking expiry and
  dirtiness. Dirty sessions (marked via `MarkDirty()`, e.g., after a network
  error) are discarded rather than returned to the pool.
- **Expiry**: A session is considered expired if its last-used time is more
  than `(logicalSessionTimeoutMinutes - 1)` minutes ago. The pool uses the
  latest topology description (received via a channel) to obtain the current
  `timeoutMinutes`. In load-balanced mode, sessions never expire.

Test-confirmed LIFO behaviour from `session_pool_test.go`:
```
checkout first, checkout second
return first, return second
next checkout → gets second (most recently returned)
next checkout → gets first
```

### 2.6 Session dirtiness

A session is marked dirty (`ss.Dirty = true`) when a network error occurs
during an operation using that session. Dirty sessions are not returned to the
pool and are discarded on `ReturnSession`. This ensures that a tainted session
(which may have left the server in an inconsistent state) is not reused.

From `operation.go` in the `networkError` helper:
```go
if op.Client != nil {
    op.Client.MarkDirty()
}
```

### 2.7 Transaction number (`txnNumber`)

`txnNumber` is an `int64` that monotonically increases within a session.
It is sent to the server for:

1. **Retryable writes** (when `retryWrite == true` and `op.Type == Write`):
   ```go
   if op.Type == Write && retryWrite {
       dst = bsoncore.AppendInt64Element(dst, "txnNumber", op.Client.TxnNumber)
   }
   ```
2. **Transactions** (when `client.TransactionRunning()` or
   `client.RetryingCommit` is true):
   ```go
   if client.TransactionRunning() || client.RetryingCommit {
       dst = bsoncore.AppendInt64Element(dst, "txnNumber", op.Client.TxnNumber)
       ...
   }
   ```

`IncrementTxnNumber()` is called exactly once before the first attempt of a
retryable write, or when `StartTransaction()` is called. It is **not** called
again on retries of the same write.

### 2.8 Transaction state fields

When inside a transaction, the following additional fields are appended:

| Field | BSON type | When sent | Value |
|---|---|---|---|
| `txnNumber` | Int64 | Every command in a transaction | Current transaction number |
| `startTransaction` | Boolean | First command of a transaction only | `true` |
| `autocommit` | Boolean | Every command in a transaction | always `false` |

State transitions:
- `StartTransaction()` sets state → `Starting`; increments `txnNumber`.
- `ApplyCommand()` (called after server selection) sets state → `InProgress`
  when state was `Starting`.
- `CommitTransaction()` sets state → `Committed`.
- `AbortTransaction()` sets state → `Aborted`.
- After a `Committed` or `Aborted` operation executes, `ApplyCommand` resets
  state → `None` and clears transaction options.

The full transaction state machine (`client_session_test.go` confirms):
```
None → Starting (StartTransaction)
Starting → InProgress (first ApplyCommand)
InProgress → Committed (CommitTransaction)
InProgress → Aborted (AbortTransaction)
Committed → Starting (StartTransaction on a new transaction)
Aborted → Starting (StartTransaction on a new transaction)
```

Illegal transitions that return errors:
- `StartTransaction` when `InProgress` → `ErrTransactInProgress`
- `CommitTransaction` when `None` → `ErrNoTransactStarted`
- `CommitTransaction` when `Aborted` → `ErrCommitAfterAbort`
- `AbortTransaction` when `None` → `ErrNoTransactStarted`
- `AbortTransaction` when `Committed` → `ErrAbortAfterCommit`
- `AbortTransaction` when `Aborted` → `ErrAbortTwice`

### 2.9 Retryable writes require non-standalone

`retryWritesSupported` (from `operation.go`):
```go
func retryWritesSupported(s description.Server) bool {
    return s.SessionTimeoutMinutes != nil && s.Kind != description.ServerKindStandalone
}
```

Retryable writes require:
1. The server advertised `logicalSessionTimeoutMinutes` (sessions supported).
2. The server is **not** a standalone — only replica sets and sharded clusters
   support retryable writes.

---

## 3. Cluster Time and Operation Time Gossiping

### 3.1 `$clusterTime` (sent by client)

When a session and/or cluster clock is present and sessions are supported, the
driver appends `$clusterTime` to every command body. The value is the most
recent cluster time seen by the session or global clock (whichever is larger):

```go
func (op Operation) addClusterTime(dst []byte, desc description.SelectedServer) []byte {
    if (clock == nil && client == nil) || !sessionsSupported(desc.WireVersion) {
        return dst
    }
    var clusterTime bson.Raw
    if clock != nil {
        clusterTime = clock.GetClusterTime()
    }
    if client != nil {
        clusterTime = session.MaxClusterTime(clusterTime, client.ClusterTime)
    }
    // Extracts the nested "$clusterTime" sub-document and appends it
    val, err := clusterTime.LookupErr("$clusterTime")
    return append(bsoncore.AppendHeader(dst, bsoncore.Type(val.Type), "$clusterTime"), val.Value...)
}
```

The `$clusterTime` value sent is the sub-document stored inside the outer
`{ "$clusterTime": { ... } }` wrapper, so the wire message contains:
```bson
"$clusterTime": { "clusterTime": Timestamp(...), ... }
```

### 3.2 `$clusterTime` structure on the wire

The cluster time document has the shape (from test file
`client_session_test.go`):
```bson
{
  "$clusterTime": {
    "clusterTime": Timestamp(seconds, ordinal)
  }
}
```

The driver compares cluster times using `MaxClusterTime`, which extracts
`$clusterTime.clusterTime` as a BSON Timestamp and compares the `(T, I)` tuple
lexicographically (`T` first, then `I`).

### 3.3 `$clusterTime` in server responses

After every round trip, `op.updateClusterTimes(res)` is called. It looks up
`$clusterTime` at the top level of the response document and advances both the
session's cluster time and the global cluster clock:

```go
func (op Operation) updateClusterTimes(response bsoncore.Document) {
    value, err := response.LookupErr("$clusterTime")
    if err != nil { return }  // silently ignored if absent
    clusterTime := bsoncore.BuildDocumentFromElements(nil,
        bsoncore.AppendValueElement(nil, "$clusterTime", value))
    if sess != nil {
        _ = sess.AdvanceClusterTime(bson.Raw(clusterTime))
    }
    if clock != nil {
        clock.AdvanceClusterTime(bson.Raw(clusterTime))
    }
}
```

**For a server implementation**: include `$clusterTime` in all responses when
sessions are supported. If omitted, the driver simply keeps its current cluster
time; this is not an error.

### 3.4 `operationTime` in server responses

After every round trip, `op.updateOperationTime(res)` is called. It looks up
`operationTime` at the top level of the response document:

```go
func (op Operation) updateOperationTime(response bsoncore.Document) {
    opTimeElem, err := response.LookupErr("operationTime")
    if err != nil { return }  // silently ignored if absent
    t, i := opTimeElem.Timestamp()
    _ = sess.AdvanceOperationTime(&bson.Timestamp{T: t, I: i})
}
```

The `operationTime` value must be a BSON Timestamp (type `0x11`). The session
retains the maximum `operationTime` seen.

**For a server implementation**: include `operationTime` in responses when
sessions are supported. The Timestamp value should represent the logical time of
the operation.

### 3.5 `readConcern.afterClusterTime` (causal consistency)

When causal consistency is enabled on a session (`client.Consistent == true`)
and `client.OperationTime != nil`, the driver adds `afterClusterTime` inside
`readConcern`:

```go
if client.Consistent && client.OperationTime != nil {
    data = bsoncore.AppendTimestampElement(data, "afterClusterTime",
        client.OperationTime.T, client.OperationTime.I)
}
```

The `readConcern` document becomes:
```bson
{
  "readConcern": {
    "level": "...",          // if a level is set
    "afterClusterTime": Timestamp(T, I)
  }
}
```

This requires wire version support (`sessionsSupported(desc.WireVersion)`).

### 3.6 `readConcern.atClusterTime` (snapshot reads)

When the session is a snapshot session (`client.Snapshot == true`) and
`client.SnapshotTimeSet == true`, the driver adds `atClusterTime`:

```go
if client.Snapshot && client.SnapshotTimeSet {
    data = bsoncore.AppendTimestampElement(data, "atClusterTime",
        client.SnapshotTime.T, client.SnapshotTime.I)
}
```

Snapshot reads require wire version ≥ 13 (`readSnapshotMinWireVersion = 13`).
The snapshot time is set once (from the first `find`/`aggregate`/`distinct`
response's `cursor.atClusterTime` or top-level `atClusterTime`) and never
changes for the lifetime of the session.

The field `UpdateSnapshotTime` in `client_session.go` reads:
```go
if c == nil || c.SnapshotTimeSet { return }  // immutable once set
// tries "cursor.atClusterTime" first, then top-level "atClusterTime"
subDoc := response
if cur, ok := response.Lookup("cursor").DocumentOK(); ok {
    subDoc = cur
}
ssTimeElem, err := subDoc.LookupErr("atClusterTime")
```

**For a server implementation**: include `atClusterTime` inside the `cursor`
sub-document of `find`/`aggregate`/`distinct` responses when snapshot reads are
requested:
```bson
{
  "cursor": {
    "id": ...,
    "ns": ...,
    "firstBatch": [...],
    "atClusterTime": Timestamp(T, I)   ← snapshot time
  }
}
```

### 3.7 `recoveryToken` (transactions with sharding)

After each round trip, `op.Client.UpdateRecoveryToken(bson.Raw(res))` is called.
The method looks up `recoveryToken` at the top level of the response:

```go
func (c *Client) UpdateRecoveryToken(response bson.Raw) {
    token, err := response.LookupErr("recoveryToken")
    if err != nil { return }
    c.RecoveryToken = token.Document()
}
```

The recovery token is a BSON embedded document. Its structure is server-defined.
The driver stores it on the session and uses it when committing/aborting a
distributed transaction.

---

## 4. Commands Requiring Cursor Responses vs. Inline Results

### 4.1 Cursor-returning commands

These commands return a `cursor` sub-document in the response. The driver calls
`NewCursorResponse` to parse the cursor and constructs a `BatchCursor` that
issues `getMore` on subsequent iterations:

| Command | Notes |
|---|---|
| `find` | Always returns cursor |
| `aggregate` | Always returns cursor (even for `$out`/`$merge` — but those write to a collection; the cursor itself has 0 or 1 batch) |
| `listCollections` | Returns cursor with namespace `<db>.$cmd.listCollections` |
| `listIndexes` | Returns cursor with namespace `<db>.$cmd.listIndexes.<coll>` |

The `cursor` response structure for all of these is identical:
```bson
{
  "ok": 1,
  "cursor": {
    "id": Int64,             // 0 = exhausted
    "ns": "<db>.<coll>",    // namespace
    "firstBatch": [ ... ]   // documents
    // optionally: "postBatchResumeToken", "atClusterTime"
  }
}
```

**Important**: `listCollections` and `listIndexes` require a `cursor` document
in the request body (always sent, possibly empty `{}`). See
`05-core-command-specifications.md`.

### 4.2 Inline-result commands

These commands return their results directly in the response document — no
`cursor` sub-document, no `getMore` lifecycle:

| Command | Key response fields | Notes |
|---|---|---|
| `insert` | `n` (int64) | Inline write result |
| `update` | `n`, `nModified`, `upserted` | Inline write result |
| `delete` | `n` | Inline write result |
| `findAndModify` | `value`, `lastErrorObject` | Returns one document or null |
| `createIndexes` | `createdCollectionAutomatically`, `indexesAfter`, `indexesBefore` | |
| `dropIndexes` | `nIndexesWas` | |
| `drop` | `nIndexesWas`, `ns` | |
| `dropDatabase` | `dropped` (optional) | Driver ignores response body |
| `create` | *(body ignored)* | |
| `listDatabases` | `databases`, `totalSize` | Inline array; no cursor |
| `killCursors` | `cursorsKilled`, etc. | Response body ignored by driver |
| `ping` | *(ok: 1 sufficient)* | |
| `buildInfo` | `version`, etc. | |
| `hello`/`isMaster` | (handshake fields) | See `04-connection-handshake.md` |
| `commitTransaction` | *(ok: 1 sufficient)* | |
| `abortTransaction` | *(ok: 1 sufficient)* | |
| `endSessions` | *(ok: 1 sufficient)* | |

### 4.3 Change streams

Change streams are implemented as `aggregate` with a `$changeStream` pipeline
stage. They return a cursor response like any other `aggregate`. The cursor is
a **tailable awaitData** cursor: `tailable: true` and `awaitData: true` are
set on the `find` command (change streams internally use `aggregate`, not
`find`). The `BatchCursor.maxAwaitTime` field drives `maxTimeMS` in subsequent
`getMore` commands.

### 4.4 Determining cursor response at the protocol level

A server can determine whether to return a cursor based on the command name:

- `find`, `aggregate`, `listCollections`, `listIndexes` → **always** return
  cursor response.
- All other commands → return inline response.

The driver uses `ErrNoCursor` (from `ExtractCursorDocument`) to detect when a
response lacks a `cursor` field. For cursor-returning commands the driver always
expects a `cursor` field; absence causes `ErrNoCursor`.

---

## 5. Response Fields the Driver Reads on Every Round Trip

The following fields are read from the response document on **every** command
response (not just specific commands), when a session is attached:

| Field | BSON type | Purpose | Behaviour if absent |
|---|---|---|---|
| `$clusterTime` | Document | Advances session/global cluster time | Silently ignored |
| `operationTime` | Timestamp (type `0x11`) | Advances session operation time | Silently ignored |
| `recoveryToken` | Document | Stored on session for distributed transactions | Silently ignored |

These are read in `readWireMessage` in `operation.go` after every response,
regardless of whether the command succeeded or failed.

For `find`, `aggregate`, and `distinct` operations specifically, the driver
also calls `UpdateSnapshotTime` to set the snapshot time for snapshot sessions.

---

## 6. Session Field Encoding Reference

### 6.1 Fields appended to every command (when sessions supported)

In `createMsgWireMessage`, after the command body and before `$db`, the
following session-related fields may be appended (in this order):

```
readConcern      (if set; may include afterClusterTime or atClusterTime)
writeConcern     (if set; write operations only)
lsid             (if session present and sessions supported)
txnNumber        (if retryable write or in transaction)
startTransaction (if first command of transaction)
autocommit       (if in transaction)
$clusterTime     (if session/clock present and sessions supported)
[apiVersion]     (Server API, if set)
maxTimeMS        (if > 0)
$db              (always)
$readPreference  (if applicable)
```

### 6.2 Minimal session-aware command example

A `find` command with a session, inside a transaction, on the first command:

```bson
{
  "find": "mycollection",
  "filter": {},
  "readConcern": {
    "level": "snapshot",
    "atClusterTime": Timestamp(1234567890, 1)
  },
  "lsid": { "id": BinData(4, "...16 bytes...") },
  "txnNumber": Int64(1),
  "startTransaction": true,
  "autocommit": false,
  "$clusterTime": { "clusterTime": Timestamp(...), ... },
  "maxTimeMS": Int64(5000),
  "$db": "mydb"
}
```

---

## 7. Summary: Server-Side State Requirements

A server implementing this protocol must maintain the following state:

### Per cursor-ID

| State | Lifetime | Notes |
|---|---|---|
| Result set position | From cursor creation until exhaustion or kill | Advances on each `getMore` |
| `cursorID` | From cursor creation until exhaustion, kill, or timeout | Must be unique per server; `0` is reserved for "no cursor" |
| Namespace | Duration of cursor | Returned in each response's `cursor.ns` |

### Per logical session (lsid)

| State | Notes |
|---|---|
| Session ID (`lsid`) | UUID; echoed in responses via `lsid` for some commands; used to route retryable writes |
| Transaction number (`txnNumber`) | Monotonically increasing; used to detect duplicate writes |
| Transaction state | Tracks whether a transaction is in progress for this session |

The server does **not** need to echo `lsid` back in responses for normal
commands. It only becomes relevant for transaction-related responses
(`commitTransaction`, `abortTransaction`). The driver does not read `lsid`
from server responses.

### Session timeout

The server advertises `logicalSessionTimeoutMinutes` in `hello`/`isMaster`
responses. Sessions unused for longer than this period may be reaped. The
driver's pool treats a session as expired if it has less than 1 minute
remaining before the timeout.
