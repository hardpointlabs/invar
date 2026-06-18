# MongoDB Core Command Specifications

## Provenance

URLs fetched, in order:

1. https://github.com/mongodb/mongo-go-driver/tree/v2.6.1/x/mongo/driver/operation
2. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/operation/find.go
3. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/operation/update.go
4. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/operation/delete.go
5. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/operation/aggregate.go
6. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/operation/create_indexes.go
7. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/operation/list_indexes.go
8. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/operation/drop_indexes.go
9. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/operation/list_databases.go
10. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/operation/list_collections.go
11. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/operation/drop_collection.go
12. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/operation/create.go
13. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/operation/command.go
14. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/operation/drop_database.go
15. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/operation/find_and_modify.go
16. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/operation/hello.go
17. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/batch_cursor.go
18. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/operation.go
19. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/errors.go
20. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/mongo/insert.go
21. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/mongo/collection.go
22. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/description/server.go
23. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/internal/driverutil/description.go

---

## Overview

This document specifies every MongoDB command that the Go driver v2.6.1 sends.
All commands are exchanged as BSON documents carried in OP_MSG Section Type 0
(single document body) or Type 1 (document sequence). The first element of
every command document is the command name. Every command body appended by the
driver framework also includes `$db` (the target database as a string) and, when
applicable, `$readPreference`, session fields, `writeConcern`, `readConcern`,
`maxTimeMS`, and `apiVersion`/`apiStrict`/`apiDeprecationErrors`.

These driver-appended fields are described in doc `04-connection-handshake.md`.
The sections below describe only the **command-specific** payload — the fields
emitted by each operation's `command()` builder function. Response field
extraction is derived from each operation's `processResponse()` / `buildXResult()`
function.

---

## Common Response Structure

Every successful OP_MSG response from the server must be a BSON document
containing:

```
{
  "ok": <1 | 1.0 | true>,   // int32, int64, double, or boolean equal to 1
  // ... command-specific fields ...
}
```

**Error response structure** — when `ok` is not 1 the driver's
`ExtractErrorFromServerResponse` reads:

| Field | BSON type | Description |
|-------|-----------|-------------|
| `ok` | int32 / int64 / double / boolean | Must be 0 for an error |
| `errmsg` | string | Human-readable error message |
| `code` | int32 | Numeric error code |
| `codeName` | string | Symbolic error code name |
| `errorLabels` | array of strings | Labels such as `"RetryableWriteError"`, `"TransientTransactionError"` |
| `writeErrors` | array of documents | Per-document write errors (see below) |
| `writeConcernError` | document | Write-concern failure (see below) |
| `topologyVersion` | document `{processId: ObjectId, counter: int64}` | SDAM topology version |

**writeErrors array element:**

| Field | Type | Description |
|-------|------|-------------|
| `index` | int64 | 0-based index of the offending document |
| `code` | int64 | Error code |
| `errmsg` | string | Error message |
| `errInfo` | document (optional) | Additional error information |

**writeConcernError document:**

| Field | Type | Description |
|-------|------|-------------|
| `code` | int64 | Error code |
| `codeName` | string | Symbolic name |
| `errmsg` | string | Error message |
| `errInfo` | document (optional) | Additional error information |
| `errorLabels` | array of strings | Error labels |

**Important**: The driver accepts `ok: 1` as success even when `writeErrors` or
`writeConcernError` are present. Those are returned as a `WriteCommandError`, not
as a command-level failure.

---

## Cursor Response Sub-document

Commands that return a cursor embed it in a top-level `cursor` document:

```
{
  "ok": 1,
  "cursor": {
    "id":         <int64>,         // 0 means cursor is exhausted
    "ns":         "<db>.<coll>",   // Required: full namespace string
    "firstBatch": [ <doc>, ... ],  // Array of result documents
    // optional:
    "postBatchResumeToken": { ... } // Change stream resume token
  }
}
```

The driver parses this in `NewCursorResponse`:
- `cursor.id` — int64 (BSON TypeInt64); driver errors if not int64
- `cursor.ns` — string; must contain exactly one `.`; split into `database` and `collection`
- `cursor.firstBatch` — BSON array; driver errors if not array
- `cursor.postBatchResumeToken` — BSON document; optional

---

## getMore

Sent by the driver automatically when iterating a cursor whose `id != 0`.

### Request

```
{
  "getMore":    <int64>,   // cursor ID  ← MUST be first element
  "collection": <string>,  // collection name (no db prefix)
  "batchSize":  <int32>,   // optional; omitted when 0
  "maxTimeMS":  <int64>,   // optional; only for tailable awaitData cursors
  "comment":    <value>,   // optional; only on wire version >= 9 (MongoDB 4.4)
  "$db":        <string>   // appended by framework
}
```

Fields:
- `getMore` (int64, **required**): The cursor ID returned from the previous command.
- `collection` (string, **required**): The name of the collection (without db prefix).
- `batchSize` (int32, optional): How many documents to return. Omitted when 0.
- `maxTimeMS` (int64, optional): Only sent for tailable awaitData cursors.
- `comment` (any BSON value, optional): Requires wire version ≥ 9.

### Response

```
{
  "ok": 1,
  "cursor": {
    "id":        <int64>,
    "ns":        "<db>.<coll>",
    "nextBatch": [ <doc>, ... ]   // ← NOTE: "nextBatch" not "firstBatch"
  }
}
```

The driver reads `cursor.id` and `cursor.nextBatch` from the getMore response.
`cursor.postBatchResumeToken` is also read if present. When `cursor.id` is 0 the
cursor is exhausted and no further `getMore` commands are sent.

---

## killCursors

Sent when a cursor is closed before being fully iterated (`BatchCursor.KillCursor`).

### Request

```
{
  "killCursors": <string>,          // collection name  ← MUST be first element
  "cursors":     [ <int64>, ... ],  // array of cursor IDs to kill
  "$db":         <string>
}
```

Fields:
- `killCursors` (string, **required**): Collection name.
- `cursors` (array of int64, **required**): Exactly one cursor ID in the array (driver only ever kills one at a time via `KillCursor`).

### Response

The driver ignores the response body for killCursors. A minimal valid response is:

```
{
  "ok": 1,
  "cursorsKilled":    [ <int64>, ... ],  // optional
  "cursorsNotFound":  [ <int64>, ... ],  // optional
  "cursorsAlive":     [ <int64>, ... ],  // optional
  "cursorsUnknown":   [ <int64>, ... ]   // optional
}
```

---

## find

### Request

```
{
  "find":               <string>,   // collection name  ← MUST be first element
  "filter":             <document>, // query filter; omitted if nil
  "sort":               <document>, // sort spec; omitted if nil
  "projection":         <document>, // field projection; omitted if nil
  "hint":               <value>,    // string or document; omitted if zero
  "skip":               <int64>,    // omitted if nil
  "limit":              <int64>,    // omitted if nil
  "batchSize":          <int32>,    // omitted if nil
  "singleBatch":        <bool>,     // omitted if nil
  "comment":            <value>,    // any BSON value; omitted if zero type
  "maxTimeMS":          <int64>,    // appended by framework if applicable
  "readConcern":        <document>, // appended by framework if applicable
  "max":                <document>, // exclusive upper index bound; omitted if nil
  "min":                <document>, // inclusive lower index bound; omitted if nil
  "returnKey":          <bool>,     // omitted if nil
  "showRecordId":       <bool>,     // NOTE: field name is "showRecordId" (not "showRecordID")
  "tailable":           <bool>,     // omitted if nil
  "awaitData":          <bool>,     // omitted if nil
  "noCursorTimeout":    <bool>,     // omitted if nil
  "allowPartialResults":<bool>,     // omitted if nil
  "allowDiskUse":       <bool>,     // requires wire version >= 4; omitted if nil
  "collation":          <document>, // requires wire version >= 5; omitted if nil
  "let":                <document>, // omitted if nil; requires server 5.0+
  "oplogReplay":        <bool>,     // omitted if nil
  "snapshot":           <bool>,     // omitted if nil
  "rawData":            <bool>,     // only on wire version >= 27; omitted if nil
  "$db":                <string>    // appended by framework
}
```

**Required fields:** `find` (the collection name).

**Optional fields (all may be omitted):** every other field above.

The driver sends `find` as the BSON key and the collection name as a string
value. If `limit` is negative, the driver sets `singleBatch: true` and sends
`limit` as the absolute value.

### Response

Cursor response (see "Cursor Response Sub-document" above).

```
{
  "ok": 1,
  "cursor": {
    "id":         <int64>,
    "ns":         "<db>.<coll>",
    "firstBatch": [ <doc>, ... ]
  }
}
```

---

## insert

The insert command uses OP_MSG **document sequence** (Section Type 1) when the
server supports it. The command body carries the header fields; the actual
documents are sent as a document sequence with identifier `"documents"`.

### Request Body (Section Type 0)

```
{
  "insert":                    <string>,   // collection name  ← MUST be first element
  "ordered":                   <bool>,     // omitted if nil; default true in driver
  "bypassDocumentValidation":  <bool>,     // omitted if nil; requires wire version >= 4
  "comment":                   <value>,    // any BSON value; omitted if zero type
  "writeConcern":               <document>, // appended by framework
  "rawData":                   <bool>,     // only on wire version >= 27
  "$db":                       <string>    // appended by framework
}
```

### Document Sequence (Section Type 1, identifier = `"documents"`)

Each document in the sequence is a BSON document to be inserted. The driver
ensures each document has an `_id` field (generated as ObjectID if missing).

Alternatively (batch fallback / legacy): the driver may send the documents as
an array field `"documents"` within the body document.

### Response

```
{
  "ok":         1,
  "n":          <int32 or int64>   // number of documents inserted
}
```

The driver reads `n` as int64 (via `AsInt64OK` which accepts int32 or int64).

On write errors the response also contains `writeErrors` and/or `writeConcernError`
as described in the Common Response Structure section.

---

## update

The update command uses OP_MSG document sequence with identifier `"updates"`.

### Request Body (Section Type 0)

```
{
  "update":                    <string>,   // collection name  ← MUST be first element
  "ordered":                   <bool>,     // omitted if nil; default true
  "bypassDocumentValidation":  <bool>,     // omitted if nil; requires wire version >= 4
  "comment":                   <value>,    // any BSON value; omitted if zero type
  "let":                       <document>, // omitted if nil; requires server 5.0+
  "writeConcern":               <document>, // appended by framework
  "rawData":                   <bool>,     // only on wire version >= 27
  "$db":                       <string>    // appended by framework
}
```

Note: `hint` and `arrayFilters` are validated at the command level (wire version
checks) but the actual `hint` and `arrayFilters` values are embedded **inside
each update statement document**, not in the top-level command body.

### Document Sequence (Section Type 1, identifier = `"updates"`)

Each update statement is a BSON document:

```
{
  "q":            <document>,   // query filter (required)
  "u":            <document or array or value>,  // update/pipeline/replacement (required)
  "multi":        <bool>,       // optional; default false
  "upsert":       <bool>,       // optional; default false
  "collation":    <document>,   // optional
  "arrayFilters": <array>,      // optional; requires wire version >= 6
  "hint":         <string or document>, // optional; requires wire version >= 5
  "sort":         <document>    // optional (used for findAndModify-like behavior)
}
```

### Response

```
{
  "ok":        1,
  "n":         <int32 or int64>,   // number of documents matched
  "nModified": <int32 or int64>,   // number of documents modified
  "upserted":  [                   // only present when upsert occurred
    {
      "index": <int64>,            // 0-based index into update statements
      "_id":   <any>               // _id of the upserted document
    },
    ...
  ]
}
```

The driver reads:
- `n` via `AsInt64OK` (accepts int32 or int64)
- `nModified` via `AsInt64OK`
- `upserted` as a BSON array; each element is a document with `index` (int64) and
  `_id` (any BSON type, unmarshalled via `bson.Unmarshal`)

**Upsert behaviour**: When `upsert: true` and no document matches the query, the
server inserts a new document and returns it in `upserted`. The driver
decrements `MatchedCount` by 1 and sets `UpsertedID` to the `_id` value.

---

## delete

The delete command uses OP_MSG document sequence with identifier `"deletes"`.

### Request Body (Section Type 0)

```
{
  "delete":        <string>,    // collection name  ← MUST be first element
  "ordered":       <bool>,      // omitted if nil; default true
  "comment":       <value>,     // any BSON value; omitted if zero type
  "let":           <document>,  // omitted if nil; requires server 5.0+
  "writeConcern":  <document>,  // appended by framework
  "rawData":       <bool>,      // only on wire version >= 27
  "$db":           <string>     // appended by framework
}
```

### Document Sequence (Section Type 1, identifier = `"deletes"`)

Each delete statement is a BSON document:

```
{
  "q":         <document>,   // query filter (required)
  "limit":     <int32>,      // 0 = delete all matching; 1 = delete one (required)
  "collation": <document>,   // optional
  "hint":      <string or document>  // optional; requires wire version >= 5
}
```

### Response

```
{
  "ok": 1,
  "n":  <int32 or int64>   // number of documents deleted
}
```

The driver reads `n` via `AsInt64OK`.

---

## aggregate

### Request

```
{
  "aggregate":                 <string or int32>,  // collection name, OR int32(1) for DB-level agg  ← MUST be first element
  "pipeline":                  <array>,    // array of stage documents (required)
  "cursor":                    {           // required; batchSize goes here
    "batchSize": <int32>                   // optional; omitted if nil
  },
  "allowDiskUse":              <bool>,     // omitted if nil
  "bypassDocumentValidation":  <bool>,     // omitted if nil
  "collation":                 <document>, // omitted if nil; requires wire version >= 5
  "comment":                   <value>,    // any BSON value; omitted if zero type
  "hint":                      <value>,    // string or document; omitted if zero type
  "let":                       <document>, // omitted if nil; requires server 5.0+
  "maxTimeMS":                 <int64>,    // appended by framework
  "readConcern":                <document>, // appended by framework if applicable
  "writeConcern":               <document>, // appended by framework if pipeline has $out/$merge
  "rawData":                   <bool>,     // only on wire version >= 27
  "$db":                       <string>    // appended by framework
}
```

**`aggregate` value**: For collection-level aggregation the value is a string
(the collection name). For database-level aggregation (`db.aggregate(...)`) the
driver sets the value to `int32(1)`.

**`cursor` document**: Always sent; contains `batchSize` if non-nil.

The driver uses `bsoncore.AppendArrayElement` to append the pipeline (not
`AppendDocumentElement`) because the pipeline is itself a BSON array.

Custom options (map\[string\]bsoncore.Value) are merged directly into the command
body after `let`.

### Response

Cursor response:

```
{
  "ok": 1,
  "cursor": {
    "id":         <int64>,
    "ns":         "<db>.<coll>",
    "firstBatch": [ <doc>, ... ]
  }
}
```

---

## createIndexes

### Request

```
{
  "createIndexes": <string>,   // collection name  ← MUST be first element
  "indexes":       <array>,    // array of index specification documents (required)
  "commitQuorum":  <value>,    // string or int32; optional; requires wire version >= 9
  "writeConcern":  <document>, // appended by framework
  "rawData":       <bool>,     // only on wire version >= 27
  "$db":           <string>    // appended by framework
}
```

**`indexes` array element structure** (each element is a document):

```
{
  "key":                <document>,  // index key spec, e.g. {"field": 1}  (required)
  "name":               <string>,    // index name (required)
  "unique":             <bool>,      // optional
  "sparse":             <bool>,      // optional
  "expireAfterSeconds": <int32>,     // optional; TTL index
  "partialFilterExpression": <document>, // optional
  "storageEngine":      <document>,  // optional
  "weights":            <document>,  // optional; text index
  "defaultLanguage":    <string>,    // optional; text index
  "languageOverride":   <string>,    // optional; text index
  "textIndexVersion":   <int32>,     // optional
  "2dsphereIndexVersion": <int32>,   // optional
  "bits":               <int32>,     // optional; 2d index
  "min":                <double>,    // optional; 2d index
  "max":                <double>,    // optional; 2d index
  "bucketSize":         <double>,    // optional; geoHaystack index
  "collation":          <document>,  // optional
  "hidden":             <bool>       // optional
}
```

### Response

```
{
  "ok":                           1,
  "createdCollectionAutomatically": <bool>,    // true if collection was created
  "indexesAfter":                 <int32>,     // number of indexes after creation
  "indexesBefore":                <int32>      // number of indexes before creation
}
```

The driver reads `createdCollectionAutomatically` via `BooleanOK`,
`indexesAfter` and `indexesBefore` via `AsInt32OK`.

---

## listIndexes

### Request

```
{
  "listIndexes": <string>,  // collection name  ← MUST be first element
  "cursor": {
    "batchSize": <int32>    // optional; omitted if nil
  },
  "rawData":   <bool>,      // only on wire version >= 27
  "$db":       <string>     // appended by framework
}
```

**`cursor` document**: Always sent (even if empty `{}`).

### Response

Cursor response:

```
{
  "ok": 1,
  "cursor": {
    "id":         <int64>,
    "ns":         "<db>.$cmd.listIndexes.<coll>",  // namespace uses system collection
    "firstBatch": [
      {
        "v":    <int32>,     // index version
        "key":  <document>,  // index key spec
        "name": <string>     // index name
        // ... other index fields ...
      },
      ...
    ]
  }
}
```

---

## dropIndexes

### Request

```
{
  "dropIndexes": <string>,         // collection name  ← MUST be first element
  "index":       <string or document>, // index name string or key spec document (required)
  "writeConcern":<document>,       // appended by framework
  "rawData":     <bool>,           // only on wire version >= 27
  "$db":         <string>          // appended by framework
}
```

**`index` field**: Either a string (index name, `"*"` to drop all non-`_id`
indexes) or a BSON document (index key specification). The driver passes `any`
type and dispatches on `string` vs `bsoncore.Document`.

### Response

```
{
  "ok":          1,
  "nIndexesWas": <int32>   // number of indexes that existed before the drop
}
```

The driver reads `nIndexesWas` via `AsInt32OK`.

---

## listDatabases

### Request

```
{
  "listDatabases":      1,          // int32(1)  ← MUST be first element
  "filter":             <document>, // optional query filter on database names
  "nameOnly":           <bool>,     // optional; return only database names
  "authorizedDatabases":<bool>,     // optional; only return databases the user can access
  "$db":                <string>    // always "admin"
}
```

Note: `listDatabases` is always run against the `admin` database.

### Response

```
{
  "ok": 1,
  "databases": [
    {
      "name":       <string>,
      "sizeOnDisk": <int64>,
      "empty":      <bool>
    },
    ...
  ],
  "totalSize": <int64>   // total size of all database files in bytes
}
```

The driver reads:
- `databases` as a BSON array, then each element as a document with `name`
  (string), `sizeOnDisk` (int64 via `AsInt64OK`), `empty` (bool)
- `totalSize` as int64 via `AsInt64OK`

When `nameOnly: true`, `sizeOnDisk` and `empty` may be absent or zero.

---

## listCollections

### Request

```
{
  "listCollections":      1,          // int32(1)  ← MUST be first element
  "filter":               <document>, // optional query filter
  "nameOnly":             <bool>,     // optional
  "authorizedCollections":<bool>,     // optional
  "cursor": {
    "batchSize": <int32>              // optional; omitted if nil
  },
  "rawData":              <bool>,     // only on wire version >= 27
  "$db":                  <string>    // appended by framework
}
```

**`cursor` document**: Always sent. Contains `batchSize` if set.

### Response

Cursor response:

```
{
  "ok": 1,
  "cursor": {
    "id":         <int64>,
    "ns":         "<db>.$cmd.listCollections",
    "firstBatch": [
      {
        "name":    <string>,
        "type":    <string>,  // "collection" or "view" or "timeseries"
        "options": <document>,
        "info": {
          "readOnly":   <bool>,
          "uuid":       <binary>
        },
        "idIndex": <document>  // the _id index spec; absent for views
      },
      ...
    ]
  }
}
```

When `nameOnly: true`, each document contains only `name` and `type`.

---

## drop (dropCollection)

### Request

```
{
  "drop":         <string>,   // collection name  ← MUST be first element
  "writeConcern": <document>, // appended by framework
  "$db":          <string>    // appended by framework
}
```

No additional fields in the command body.

### Response

```
{
  "ok":          1,
  "nIndexesWas": <int32>,  // number of indexes in the dropped collection
  "ns":          <string>  // fully-qualified namespace of the dropped collection
}
```

The driver reads `nIndexesWas` via `AsInt32OK` and `ns` via `StringValueOK`.

**Note**: If the collection does not exist, the server returns an error with
code 26 (`NamespaceNotFound`). The Go driver handles this as a non-fatal
condition in some call sites.

---

## dropDatabase

### Request

```
{
  "dropDatabase": 1,          // int32(1)  ← MUST be first element
  "writeConcern": <document>, // appended by framework
  "$db":          <string>    // appended by framework
}
```

### Response

```
{
  "ok": 1,
  "dropped": <string>   // optional; the name of the dropped database
}
```

The driver ignores the response body for this command (no `processResponse`
function defined). A minimal `{"ok": 1}` is sufficient.

---

## create (createCollection)

### Request

```
{
  "create":                      <string>,    // collection name  ← MUST be first element
  "capped":                      <bool>,      // optional; if true, creates a capped collection
  "size":                        <int64>,     // optional; max size in bytes for capped collections
  "max":                         <int64>,     // optional; max number of documents in capped collection
  "storageEngine":               <document>,  // optional
  "validator":                   <document>,  // optional; document validation rules
  "validationLevel":             <string>,    // optional; "off", "moderate", "strict"
  "validationAction":            <string>,    // optional; "warn", "error"
  "indexOptionDefaults":         <document>,  // optional
  "viewOn":                      <string>,    // optional; source collection/view for a view
  "pipeline":                    <array>,     // optional; aggregation pipeline for a view
  "collation":                   <document>,  // optional; requires wire version >= 5
  "changeStreamPreAndPostImages":<document>,  // optional; MongoDB 6.0+
  "timeseries":                  <document>,  // optional; time series collection config
  "expireAfterSeconds":          <int64>,     // optional; TTL for time series
  "encryptedFields":             <document>,  // optional; queryable encryption config
  "clusteredIndex":              <document>,  // optional; clustered index spec
  "writeConcern":                <document>,  // appended by framework
  "$db":                         <string>     // appended by framework
}
```

### Response

The driver ignores the response body (no `processResponse` function). A minimal
valid response is:

```
{
  "ok": 1
}
```

---

## ping

The driver calls `ping` as a generic command via the `Command` operation
(not via a dedicated operation struct). The driver does send a `ping` command
during connection pool maintenance.

### Request

```
{
  "ping": 1,      // int32(1)  ← MUST be first element
  "$db":  <string>
}
```

### Response

```
{
  "ok": 1
}
```

The response body is not parsed beyond checking `ok`. Any document with
`ok: 1` is a valid ping response.

---

## buildInfo

The driver calls `buildInfo` as a generic `Command` operation to verify
server compatibility. The driver inspects the wire version to determine which
features are available; `buildInfo` is used to obtain the server version string
and wire version range in some testing and diagnostic paths.

### Request

```
{
  "buildInfo": 1,   // int32(1)  ← MUST be first element
  "$db":        <string>
}
```

### Response

```
{
  "ok":             1,
  "version":        <string>,   // e.g. "7.0.5"
  "versionArray":   [ <int32>, <int32>, <int32>, <int32> ],
  "gitVersion":     <string>,
  "sysInfo":        <string>,
  "loaderFlags":    <string>,
  "compilerFlags":  <string>,
  "allocator":      <string>,
  "openssl":        <document>,
  "javascriptEngine": <string>,
  "bits":           <int32>,    // 32 or 64
  "debug":          <bool>,
  "maxBsonObjectSize": <int32>, // typically 16777216 (16 MiB)
  "modules":        <array>
}
```

The driver does not parse individual fields from `buildInfo` responses — it
treats the response as an opaque `bsoncore.Document`. Any document with `ok: 1`
and `version` present is sufficient.

---

## hello / isMaster (handshake)

Refer to `04-connection-handshake.md` for the full hello/isMaster specification.
This section only notes the fields parsed that affect command routing.

The driver parses the following fields from the hello response that affect
command behaviour:

| Field | BSON type | Usage |
|-------|-----------|-------|
| `maxBsonObjectSize` | int64 | Stored as `MaxDocumentSize` (uint32) |
| `maxMessageSizeBytes` | int64 | Stored as `MaxMessageSize` (uint32) |
| `maxWriteBatchSize` | int64 | Stored as `MaxBatchCount` (uint32) |
| `minWireVersion` | int64 | Lower bound of `WireVersion` range |
| `maxWireVersion` | int64 | Upper bound of `WireVersion` range; determines feature availability |
| `logicalSessionTimeoutMinutes` | int64 | Session timeout |
| `ok` | int64 | Must equal 1 |

**Wire version feature gates used by commands:**

| Wire version | Feature |
|---|---|
| ≥ 4 | `allowDiskUse` (find), `bypassDocumentValidation` (insert/update) |
| ≥ 5 | `collation` (find/aggregate/update/delete/create), `hint` (update/delete) |
| ≥ 6 | `arrayFilters` (update/findAndModify) |
| ≥ 8 | `hint` (findAndModify) |
| ≥ 9 | `commitQuorum` (createIndexes), `comment` on getMore |
| ≥ 13 | Read preference routing for aggregate with output stage |
| ≥ 27 | `rawData` on all commands |

---

## findAndModify

### Request

```
{
  "findAndModify": <string>,          // collection name  ← MUST be first element
  "query":         <document>,        // selection criteria (optional but typical)
  "update":        <value>,           // update doc, pipeline array, or replacement doc (optional)
  "remove":        <bool>,            // if true, delete the matched doc (optional)
  "new":           <bool>,            // if true, return modified doc instead of original (optional)
  "fields":        <document>,        // field projection for returned doc (optional)
  "sort":          <document>,        // determines which doc to modify if multiple match (optional)
  "upsert":        <bool>,            // if true and no match, insert new doc (optional)
  "bypassDocumentValidation": <bool>, // optional
  "collation":     <document>,        // optional; requires wire version >= 5
  "arrayFilters":  <array>,           // optional; requires wire version >= 6
  "hint":          <value>,           // optional; requires wire version >= 8
  "comment":       <value>,           // optional
  "let":           <document>,        // optional; requires server 5.0+
  "writeConcern":  <document>,        // appended by framework
  "rawData":       <bool>,            // only on wire version >= 27
  "$db":           <string>           // appended by framework
}
```

Either `remove: true` or `update` must be present (but not both).

### Response

```
{
  "ok": 1,
  "value": <document or null>,   // the matched document (before or after modification)
  "lastErrorObject": {
    "updatedExisting": <bool>,
    "upserted":        <any>       // _id of upserted document, absent if not upserted
  }
}
```

The driver reads:
- `value` as a document (or null if no document matched)
- `lastErrorObject.updatedExisting` as bool (via `bson.Unmarshal`)
- `lastErrorObject.upserted` as any type (via `bson.Unmarshal`)

---

## Batch Splitting

For `insert`, `update`, and `delete`, the driver may split documents across
multiple wire messages if the batch is too large. The splitting algorithm:

1. Uses `driver.Batches` which tracks documents and `Ordered` setting.
2. Documents are batched into OP_MSG Section Type 1 (document sequence) up to
   `MaxBatchCount` and `MaxMessageSize` limits from the hello response.
3. Responses are accumulated: `N`, `NModified`, `Upserted` are summed across
   batches. `Upserted[i].Index` is offset by the batch's starting index.
4. If `ordered: true` (the default) and a batch encounters a write error, the
   driver stops processing and returns the accumulated error.
5. If `ordered: false`, all batches are attempted and all errors accumulated.

---

## Common Command Routing Notes

### `$db` field

Every OP_MSG command body includes a `$db` field containing the target database
name. For `listDatabases`, the database is always `"admin"`. For `ping` and
`buildInfo`, the database is also `"admin"`.

### `$readPreference` field

The framework appends `$readPreference` when the command is a read operation and
the read preference is not primary, OR when routing through a mongos. The value
is a document `{"mode": <string>}`.

### Session fields

When a session is attached, the framework appends:
- `lsid`: `{"id": <binary UUID>}` — the logical session ID
- `txnNumber`: int64 — transaction number (for retryable writes and transactions)
- `startTransaction`: bool — true on the first command in a transaction
- `autocommit`: bool (false) — always false when inside a transaction

### Write concern field

The framework appends `writeConcern` (document) for write operations. The
document may contain:
- `w`: int32 or string (e.g., `1`, `"majority"`, `"<tagSetName>"`)
- `j`: bool — journal acknowledgement
- `wtimeout`: int64 — timeout in ms

An unacknowledged write concern (`w: 0`) causes the driver to set the OP_MSG
`moreToCome` flag and not wait for a response.

### Read concern field

The framework appends `readConcern` (document) for read operations when set.
May contain `level` (string, e.g., `"local"`, `"majority"`, `"snapshot"`) and
`afterClusterTime` (Timestamp).

### `maxTimeMS` field

Appended by the framework when a timeout context is active, calculated as
`contextDeadline - now - minimumRoundTripTime`. The driver omits `maxTimeMS`
from `getMore` on non-awaitData cursors to avoid "cursor not found" errors. The
field is also omitted from `find` and `aggregate` that return user-managed
cursors (the `OmitMaxTimeMS` flag).

---

## Error Code Reference (Partial)

Retryable error codes that the driver recognises:

| Code | Name |
|------|------|
| 6 | HostUnreachable |
| 7 | HostNotFound |
| 26 | NamespaceNotFound |
| 50 | MaxTimeMSExpired |
| 89 | NetworkTimeout |
| 91 | ShutdownInProgress |
| 134 | ReadConcernMajorityNotAvailableYet |
| 189 | PrimarySteppedDown |
| 262 | ExceededTimeLimit |
| 391 | ReauthenticationRequired (triggers automatic reauthentication) |
| 9001 | SocketException |
| 10107 | NotWritablePrimary |
| 11600 | InterruptedAtShutdown |
| 11602 | InterruptedDueToReplStateChange |
| 13435 | NotPrimaryNoSecondaryOk |
| 13436 | NotPrimaryOrSecondary |

**Special handling for code 391**: The driver catches this error code and
re-authenticates on the same connection before retrying the operation.

**Special handling for code 50** (`MaxTimeMSExpired`): The driver wraps this
error with `context.DeadlineExceeded` so callers can use `errors.Is(err,
context.DeadlineExceeded)` for either client-side or server-side timeouts.

**Special handling for code 20 with message prefix `"transaction numbers"`**:
Indicates an unsupported storage engine for retryable writes; the driver returns
`ErrUnsupportedStorageEngine`.

Node-is-recovering codes (trigger SDAM state transition): 11600, 11602, 13436, 189, 91.

Not-primary codes (trigger SDAM state transition): 10107, 13435, 10058.
