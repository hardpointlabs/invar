# MongoDB Error Response Structure — Complete Implementation Specification

```yaml
provenance:
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/errors.go"
    fetched: true
    purpose: >
      Primary source. Contains ExtractErrorFromServerResponse (the canonical
      BSON-to-error decoder), all error type definitions (Error, WriteCommandError,
      WriteConcernError, WriteError, WriteErrors), and all numeric error code
      constants (retryableCodes, nodeIsRecoveringCodes, notPrimaryCodes, etc.)
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/operation.go"
    fetched: true
    purpose: >
      createWireMessage / roundTrip / readWireMessage paths; confirms how
      ExtractErrorFromServerResponse is invoked; error-label augmentation logic
      per wire version; reauthentication (code 391) handling.
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/operation_test.go"
    fetched: true
    purpose: >
      createExhaustServerResponse helper and exhaustAllowed/moreToCome test —
      confirms minimal {ok:1} response structure used in tests.
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/internal/driverutil/description.go"
    fetched: true
    purpose: >
      NewTopologyVersion (TopologyVersion BSON decode); NewServerDescription
      (confirms 'ok' field parsing for handshake responses);
      MinWireVersion / MaxWireVersion constants.
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/description/server.go"
    fetched: true
    purpose: >
      TopologyVersion struct definition (ProcessID bson.ObjectID, Counter int64);
      ServerKind constants.
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/topology/server.go"
    fetched: true
    purpose: >
      ProcessError (SDAM error handling); getWriteConcernErrorForProcessing;
      extractTopologyVersion; wireVersion42 constant (= 8).
```

---

## Overview

This document specifies the exact BSON document structure of all error response
variants that the Go driver v2.6.1 reads from the server. The driver's canonical
decoder is `ExtractErrorFromServerResponse` in
`x/mongo/driver/errors.go`. Every field name, BSON type, and type-coercion rule
below is derived directly from that function and from the supporting type
definitions in the same file.

The existing documents in this series already describe the **success** response
shape (see `05-core-command-specifications.md`). This document focuses
exclusively on **error** responses.

---

## 1. When Is a Response an Error?

`ExtractErrorFromServerResponse` iterates every element of the response
document. The `ok` field drives the primary dispatch:

```go
case "ok":
    switch elem.Value().Type {
    case bsoncore.TypeInt32:
        if elem.Value().Int32() == 1  { ok = true }
    case bsoncore.TypeInt64:
        if elem.Value().Int64() == 1  { ok = true }
    case bsoncore.TypeDouble:
        if elem.Value().Double() == 1 { ok = true }
    case bsoncore.TypeBoolean:
        if elem.Value().Boolean()     { ok = true }
    }
```

The function returns `nil` (no error) only when **all** of the following hold:

1. `ok` equals `1` (via the type check above), **and**
2. neither `writeErrors` nor `writeConcernError` are present in the document.

If `ok != 1` → returns a `driver.Error`.
If `ok == 1` but `writeErrors` and/or `writeConcernError` are present →
returns a `driver.WriteCommandError`.

---

## 2. Command-Level Error Document (`ok: 0`)

### 2.1 Full Field Reference

When `ok != 1`, the driver constructs a `driver.Error` from the following fields:

| Field | BSON type accepted | Driver field | Notes |
|---|---|---|---|
| `ok` | Int32, Int64, Double, Boolean | (triggers error path) | Must be absent or ≠ 1 |
| `errmsg` | String | `Error.Message` | Human-readable message; defaults to `"command failed"` if absent |
| `code` | Int32 (only) | `Error.Code` (int32) | Numeric error code; 0 if absent |
| `codeName` | String | `Error.Name` | Symbolic name string; `""` if absent |
| `errorLabels` | Array of String | `Error.Labels` ([]string) | See §2.2 |
| `topologyVersion` | Document | `Error.TopologyVersion` | See §4 |

**Key detail**: `code` is read with `Int32OK()` only — not `AsInt64OK()`. The
field **must** be BSON `int32` (type `0x10`) for the driver to parse it.
If the field is BSON `int64`, the driver's `Int32OK()` call returns `false` and
`code` remains `0`. If you implement a server, always encode `code` as BSON
`int32`.

```go
// From ExtractErrorFromServerResponse (errors.go):
case "code":
    if c, okay := elem.Value().Int32OK(); okay {
        code = c
    }
```

### 2.2 `errorLabels` Array

`errorLabels` is a BSON array (type `0x04`) whose elements are BSON strings.
The driver reads it with `ArrayOK()` then iterates `arr.Values()`, calling
`StringValueOK()` on each element. Non-string elements are silently skipped.

Label strings the driver recognises (defined as constants in `errors.go`):

| Label string | Constant name | Meaning |
|---|---|---|
| `"NetworkError"` | `NetworkError` | Operation failed due to a network problem |
| `"RetryableWriteError"` | `RetryableWriteError` | Write is safe to retry |
| `"TransientTransactionError"` | `TransientTransactionError` | Transaction can be retried |
| `"UnknownTransactionCommitResult"` | `UnknownTransactionCommitResult` | Commit result unknown |
| `"NoWritesPerformed"` | `NoWritesPerformed` | No writes were executed |
| `"SystemOverloadedError"` | `ErrSystemOverloadedError` | Server overloaded; adaptive retry |
| `"RetryableError"` | `ErrRetryableError` | Combined with overload for adaptive retry |

### 2.3 Minimal Error Response

The smallest valid error document:

```bson
{ "ok": 0 }
```

The driver fills in `errmsg = "command failed"`, `code = 0`, and `Name = ""`.

### 2.4 Canonical Error Response

```bson
{
  "ok":           { $numberInt: "0" },
  "errmsg":       "some error message",
  "code":         { $numberInt: "2" },
  "codeName":     "BadValue",
  "errorLabels":  [ "TransientTransactionError" ]
}
```

Wire encoding note: `ok`, `code` are BSON `int32` (type `0x10`). `errmsg`,
`codeName`, and each label are BSON UTF-8 strings (type `0x02`). `errorLabels`
is a BSON array (type `0x04`) containing string elements.

### 2.5 `topologyVersion` in Error Responses

When present in a command error (`ok: 0`) response, the driver decodes
`topologyVersion` from the top-level document and attaches it to `Error.TopologyVersion`.
This is used by the SDAM layer to decide whether to discard a stale error.
See §4 for the exact BSON structure.

### 2.6 Special-case: Code 50 (`MaxTimeMSExpired`)

When `Error.Code == 50`, the driver wraps `context.DeadlineExceeded`:

```go
if err.Code == 50 {
    err.Wrapped = context.DeadlineExceeded
}
```

This allows callers to use `errors.Is(err, context.DeadlineExceeded)` for both
client-side and server-side timeouts.

### 2.7 Special-case: Code 20 with `"transaction numbers"` prefix

```go
// UnsupportedStorageEngine
func (e Error) UnsupportedStorageEngine() bool {
    return e.Code == 20 &&
           strings.HasPrefix(strings.ToLower(e.Message), "transaction numbers")
}
```

When code 20 is returned with a message beginning with `"transaction numbers"`
(case-insensitive), the driver returns `ErrUnsupportedStorageEngine` instead of
the raw error.

### 2.8 Special-case: Code 391 (`ReauthenticationRequired`)

The driver catches code 391 mid-operation and automatically re-authenticates
before retrying:

```go
case Error:
    if tt.Code == 391 {
        if op.Authenticator != nil {
            // ... calls Authenticator.Reauth() ...
            // ... then resetForRetry() ...
        }
    }
```

---

## 3. Write Errors (`ok: 1` with `writeErrors` / `writeConcernError`)

When `ok == 1` but `writeErrors` and/or `writeConcernError` are present, the
function returns a `driver.WriteCommandError` instead of `driver.Error`.

### 3.1 Top-level Shape

```bson
{
  "ok":               { $numberInt: "1" },
  "n":                { $numberInt: "0" },      // command-specific count fields
  "writeErrors":      [ ... ],                  // optional; array of WriteError docs
  "writeConcernError": { ... },                 // optional; single document
  "errorLabels":      [ ... ]                   // optional; array of strings
}
```

**Important**: `errorLabels` at the top level of a write response (whether `ok:
1` or `ok: 0`) is always captured and placed into `WriteCommandError.Labels` (if
it is a write error) or `Error.Labels` (if it is a command error).

### 3.2 `writeErrors` Array Element

Each element of the `writeErrors` BSON array is a BSON document. The driver
decodes it as a `driver.WriteError`:

```go
type WriteError struct {
    Index   int64
    Code    int64
    Message string
    Details bsoncore.Document   // the "errInfo" field
    Raw     bsoncore.Document   // the full element document
}
```

Field decoding:

| BSON field | Accepted types | Driver field | Accessor |
|---|---|---|---|
| `index` | Int32 or Int64 | `WriteError.Index` (int64) | `AsInt64OK()` |
| `code` | Int32 or Int64 | `WriteError.Code` (int64) | `AsInt64OK()` |
| `errmsg` | String | `WriteError.Message` | `StringValueOK()` |
| `errInfo` | Document | `WriteError.Details` | `DocumentOK()` |

```go
// From ExtractErrorFromServerResponse (errors.go):
case "writeErrors":
    arr, exists := elem.Value().ArrayOK()
    ...
    for _, val := range vals {
        var we WriteError
        doc, exists := val.DocumentOK()
        ...
        if index, exists := doc.Lookup("index").AsInt64OK(); exists {
            we.Index = index
        }
        if code, exists := doc.Lookup("code").AsInt64OK(); exists {
            we.Code = code
        }
        if msg, exists := doc.Lookup("errmsg").StringValueOK(); exists {
            we.Message = msg
        }
        if info, exists := doc.Lookup("errInfo").DocumentOK(); exists {
            we.Details = make([]byte, len(info))
            copy(we.Details, info)
        }
        we.Raw = doc
        wcError.WriteErrors = append(wcError.WriteErrors, we)
    }
```

Note that `index` and `code` for write errors use `AsInt64OK()`, which accepts
both BSON `int32` and `int64`. This is different from the top-level `code` field
(which uses `Int32OK()` only).

#### Minimal `writeErrors` element:

```bson
{
  "index":  { $numberInt: "0" },
  "code":   { $numberInt: "11000" },
  "errmsg": "E11000 duplicate key error ..."
}
```

#### Full `writeErrors` element (with `errInfo`):

```bson
{
  "index":   { $numberInt: "0" },
  "code":    { $numberInt: "121" },
  "errmsg":  "Document failed validation",
  "errInfo": {
    "failingDocumentId": { ... },
    "details":           { ... }
  }
}
```

### 3.3 `writeConcernError` Document

The `writeConcernError` top-level field is a BSON embedded document. The driver
decodes it as a `driver.WriteConcernError`:

```go
type WriteConcernError struct {
    Name            string
    Code            int64
    Message         string
    Details         bsoncore.Document
    Labels          []string
    TopologyVersion *description.TopologyVersion
    Raw             bsoncore.Document
}
```

Field decoding:

| BSON field | Accepted types | Driver field | Accessor |
|---|---|---|---|
| `code` | Int32 or Int64 | `WriteConcernError.Code` (int64) | `AsInt64OK()` |
| `codeName` | String | `WriteConcernError.Name` | `StringValueOK()` |
| `errmsg` | String | `WriteConcernError.Message` | `StringValueOK()` |
| `errInfo` | Document | `WriteConcernError.Details` | `DocumentOK()` |
| `errorLabels` | Array of String | appended to top-level `labels` | `ArrayOK()` + `StringValueOK()` |

```go
// From ExtractErrorFromServerResponse (errors.go):
case "writeConcernError":
    doc, exists := elem.Value().DocumentOK()
    ...
    wcError.WriteConcernError = new(WriteConcernError)
    wcError.WriteConcernError.Raw = doc
    if code, exists := doc.Lookup("code").AsInt64OK(); exists {
        wcError.WriteConcernError.Code = code
    }
    if name, exists := doc.Lookup("codeName").StringValueOK(); exists {
        wcError.WriteConcernError.Name = name
    }
    if msg, exists := doc.Lookup("errmsg").StringValueOK(); exists {
        wcError.WriteConcernError.Message = msg
    }
    if info, exists := doc.Lookup("errInfo").DocumentOK(); exists {
        wcError.WriteConcernError.Details = ...
    }
    if errLabels, exists := doc.Lookup("errorLabels").ArrayOK(); exists {
        // Labels from writeConcernError.errorLabels are appended
        // to the top-level labels slice
        ...
    }
```

**Note**: The `topologyVersion` field inside `writeConcernError` is **not**
decoded by `ExtractErrorFromServerResponse` directly; it is attached to
`WriteConcernError.TopologyVersion` by the SDAM layer at a higher level
(`extractTopologyVersion` in `topology/server.go`).

However, `topology/server.go`'s `extractTopologyVersion` function reads
`WriteConcernError.TopologyVersion` from the already-decoded Go struct — it
does not re-parse the raw BSON. The actual TopologyVersion BSON decoding for
write concern errors therefore happens indirectly. If you want the driver to
correctly handle SDAM transitions for write concern errors, the
`writeConcernError` document must include `topologyVersion`; the driver's SDAM
layer will attach it after reading the error (via the field stored on the
`WriteConcernError` struct, which is populated during the `ProcessError` call).

#### Minimal `writeConcernError`:

```bson
{
  "code":   { $numberLong: "64" },
  "errmsg": "waiting for replication timed out"
}
```

#### Full `writeConcernError`:

```bson
{
  "code":        { $numberLong: "91" },
  "codeName":    "ShutdownInProgress",
  "errmsg":      "The server is in quiesce mode and will shut down",
  "errInfo":     { "note": "..." },
  "errorLabels": [ "RetryableWriteError" ]
}
```

### 3.4 `WriteCommandError` — Labels Accumulation

`errorLabels` values come from **two** sources in the response document:

1. Top-level `errorLabels` array (on the command response body itself).
2. `writeConcernError.errorLabels` array (nested inside the write concern error).

Both are read into the same `labels` slice. The final `labels` slice is then
assigned to `WriteCommandError.Labels`.

```go
// After the loop:
if len(wcError.WriteErrors) > 0 || wcError.WriteConcernError != nil {
    wcError.Labels = labels          // accumulated from both sources
    if wcError.WriteConcernError != nil {
        wcError.WriteConcernError.TopologyVersion = tv
    }
    wcError.Raw = doc
    return wcError
}
```

---

## 4. `topologyVersion` Sub-document

`topologyVersion` may appear at the **top level** of an error response (in `ok:
0` command errors) and is referenced by `WriteConcernError.TopologyVersion`
(decoded via the SDAM layer). The BSON structure is identical in both cases.

```bson
{
  "processId": ObjectId("..."),   // BSON ObjectID (12 bytes, type 0x07)
  "counter":   { $numberLong: "0" }  // BSON int64 (type 0x12)
}
```

Go struct:

```go
type TopologyVersion struct {
    ProcessID bson.ObjectID   // bson.ObjectID = [12]byte
    Counter   int64
}
```

BSON decode (from `driverutil.NewTopologyVersion`):

```go
func NewTopologyVersion(doc bson.Raw) (*description.TopologyVersion, error) {
    for _, element := range elements {
        switch element.Key() {
        case "processId":
            tv.ProcessID, ok = element.Value().ObjectIDOK()
            // MUST be ObjectID type; error if not
        case "counter":
            tv.Counter, ok = element.Value().Int64OK()
            // MUST be int64 type; error if not
        }
    }
    return &tv, nil
}
```

**Type constraints**: `processId` must be BSON ObjectID (type `0x07`). `counter`
must be BSON `int64` (type `0x12`). Neither accepts type coercion — wrong types
cause `NewTopologyVersion` to return an error, which causes
`ExtractErrorFromServerResponse` to leave `tv` as `nil` on the resulting
`driver.Error`.

When comparing topology versions to decide whether to discard a stale error,
the driver uses `CompareTopologyVersions`:

```go
func CompareTopologyVersions(receiver, response *TopologyVersion) int {
    if receiver == nil || response == nil { return -1 }
    if receiver.ProcessID != response.ProcessID { return -1 }
    if receiver.Counter == response.Counter { return 0 }
    if receiver.Counter < response.Counter { return -1 }
    return 1
}
```

---

## 5. `ok` Field Encoding Rules

The `ok` field is the single most important field in any response. The following
BSON types are accepted as "success" when they represent the value `1`:

| BSON type | Type byte | Accepted as `ok == 1` |
|---|---|---|
| Int32 (`0x10`) | 16 | value `int32(1)` |
| Int64 (`0x12`) | 18 | value `int64(1)` |
| Double (`0x01`) | 1 | value `float64(1.0)` |
| Boolean (`0x08`) | 8 | value `true` (byte `0x01`) |

Any other BSON type for `ok` causes the field to be treated as "not ok" (the
driver uses a `switch` statement so unmatched types fall through without setting
`ok = true`).

For a **handshake** (`hello`/`isMaster`) response, a different decoder is used
(`NewServerDescription` in `driverutil/description.go`). It uses `AsInt64OK()`
for `ok`, which accepts Int32 and Int64 only — not Double or Boolean.

For **command responses** (all post-handshake OP_MSG responses),
`ExtractErrorFromServerResponse` is used and accepts all four types above.

---

## 6. Error Code Numeric Values

All codes are `int32` in the `driver.Error` struct. All codes below are sourced
directly from constants and arrays in `x/mongo/driver/errors.go` and
`x/mongo/driver/topology/server.go`.

### 6.1 Retryable Error Codes

Defined in `retryableCodes` (errors.go):

| Code | Symbolic name |
|---|---|
| 6 | `HostUnreachable` |
| 7 | `HostNotFound` |
| 89 | `NetworkTimeout` |
| 91 | `ShutdownInProgress` |
| 134 | `ReadConcernMajorityNotAvailableYet` |
| 189 | `PrimarySteppedDown` |
| 262 | `ExceededTimeLimit` |
| 9001 | `SocketException` |
| 10107 | `NotWritablePrimary` |
| 11600 | `InterruptedAtShutdown` |
| 11602 | `InterruptedDueToReplStateChange` |
| 13435 | `NotPrimaryNoSecondaryOk` |
| 13436 | `NotPrimaryOrSecondary` |

These codes cause `Error.RetryableRead()` and/or `Error.RetryableWrite()` to
return `true`, which in turn causes the driver to retry the operation (subject
to retry mode and wire version guards).

### 6.2 Node-Is-Recovering Codes

Defined in `nodeIsRecoveringCodes` (errors.go):

| Code | Symbolic name |
|---|---|
| 91 | `ShutdownInProgress` |
| 189 | `PrimarySteppedDown` |
| 11600 | `InterruptedAtShutdown` |
| 11602 | `InterruptedDueToReplStateChange` |
| 13436 | `NotPrimaryOrSecondary` |

When the code matches and the message contains `"node is recovering"` (when code
is 0), `Error.NodeIsRecovering()` returns `true`. This triggers SDAM to mark the
server as Unknown and request an immediate re-check.

### 6.3 Node-Is-Shutting-Down Codes

Defined in `nodeIsShuttingDownCodes` (errors.go):

| Code | Symbolic name |
|---|---|
| 91 | `ShutdownInProgress` |
| 11600 | `InterruptedAtShutdown` |

When `Error.NodeIsShuttingDown()` returns `true`, the driver
**synchronously** clears the connection pool (in addition to marking the server
Unknown) rather than waiting for the pool-clear to happen asynchronously.

### 6.4 Not-Primary Codes

Defined in `notPrimaryCodes` (errors.go):

| Code | Symbolic name |
|---|---|
| 10058 | `NotMaster` (legacy, not in list but checked) |
| 10107 | `NotWritablePrimary` |
| 13435 | `NotPrimaryNoSecondaryOk` |

Additionally, when `code == 0` and `message` contains `"not master"` (the
constant `LegacyNotPrimaryErrMsg`), `Error.NotPrimary()` returns `true`.

### 6.5 Special-Purpose Codes

| Code | Symbolic name | Special handling |
|---|---|---|
| 20 | *(varies)* | If message starts with `"transaction numbers"` (case-insensitive), returns `ErrUnsupportedStorageEngine` |
| 26 | `NamespaceNotFound` | `Error.NamespaceNotFound()` returns true; some call sites treat this as non-fatal |
| 50 | `MaxTimeMSExpired` | `Error.Wrapped` is set to `context.DeadlineExceeded` |
| 79 | `UnknownReplWriteConcernCode` | `unknownReplWriteConcernCode = int32(79)` — commits with this code do NOT get `UnknownTransactionCommitResult` label |
| 100 | `UnsatisfiableWriteConcernCode` | `unsatisfiableWriteConcernCode = int32(100)` — commits with this code do NOT get `UnknownTransactionCommitResult` label |
| 391 | `ReauthenticationRequired` | Triggers automatic reauthentication before retry |

---

## 7. `decodeResult` — How the Driver Dispatches on `ok`

After decoding an OP_MSG or OP_REPLY response, the operation layer calls
`decodeResult(opcode, rem)`. For OP_MSG responses this ultimately calls
`ExtractErrorFromServerResponse`. The function:

1. Returns `nil` error if `ok == 1` and no write errors → success path.
2. Returns `driver.WriteCommandError` if `ok == 1` but write errors present.
3. Returns `driver.Error` if `ok != 1`.

The operation loop (`Execute`) then type-switches on the returned error:

```go
switch tt := err.(type) {
case WriteCommandError:
    // handles write-level errors; may retry
case Error:
    // handles command-level errors; may retry, reauthenticate, etc.
case nil:
    // success
default:
    // other errors (network, etc.)
}
```

---

## 8. OP_REPLY Legacy Error Handling (`QueryFailure` flag)

For OP_REPLY responses (used only during the legacy `isMaster` handshake), the
driver has a different error path. When the `QueryFailure` flag (bit 1) is set
in `responseFlags`, the first document in the reply is treated as an error
document. The driver wraps it in a `QueryFailureError`:

```go
type QueryFailureError struct {
    Message  string
    Response bsoncore.Document
    Wrapped  error
}
```

The error message is `"command failure"` and the raw response document is
attached. No field-by-field decoding is performed; the entire document is
preserved as `Response`. This error type is **not** retried; it propagates
directly to the caller.

The `QueryFailure` flag is only set when the server cannot execute the `isMaster`
query at all (e.g. the query was sent to a database that has an auth error).
Normal command errors in the OP_REPLY era were still delivered as `{"ok": 0, ...}`
documents with `QueryFailure` clear.

---

## 9. Wire Encoding of a Complete Error Response

### 9.1 `ok: 0` command error (OP_MSG)

```
[MsgHeader: 16 bytes]
[flagBits: 0x00000000]
[sectionType: 0x00]
[BSON document]:
  int32   totalLength
  0x10 "ok\0"        int32(0)
  0x02 "errmsg\0"    int32(len+1) "Unauthorized\0"
  0x10 "code\0"      int32(13)
  0x02 "codeName\0"  int32(len+1) "Unauthorized\0"
  0x00
```

### 9.2 `ok: 1` with `writeErrors` (OP_MSG)

```
[MsgHeader: 16 bytes]
[flagBits: 0x00000000]
[sectionType: 0x00]
[BSON document]:
  int32   totalLength
  0x10 "ok\0"          int32(1)
  0x10 "n\0"           int32(0)
  0x04 "writeErrors\0"
    int32   arrayLength
    0x03 "0\0"          // first element
      int32   elemLength
      0x10 "index\0"    int32(0)
      0x12 "code\0"     int64(11000)
      0x02 "errmsg\0"   int32(len+1) "E11000 duplicate key...\0"
      0x00
    0x00  // array terminator
  0x00
```

### 9.3 `ok: 1` with `writeConcernError` (OP_MSG)

```
[MsgHeader: 16 bytes]
[flagBits: 0x00000000]
[sectionType: 0x00]
[BSON document]:
  int32   totalLength
  0x10 "ok\0"                int32(1)
  0x10 "n\0"                 int32(1)
  0x03 "writeConcernError\0"
    int32   subdocLength
    0x12 "code\0"            int64(91)
    0x02 "codeName\0"        int32(len+1) "ShutdownInProgress\0"
    0x02 "errmsg\0"          int32(len+1) "...\0"
    0x00
  0x00
```

---

## 10. Behaviour Differences by Wire Version

### 10.1 `RetryableWriteError` label addition (pre-4.4 servers)

For servers with `maxWireVersion < 9` (MongoDB < 4.4), the driver adds the
`RetryableWriteError` label itself (the server does not):

```go
preRetryWriteLabelVersion := connDesc.WireVersion != nil && connDesc.WireVersion.Max < 9
// In Write path:
if retryableErr && preRetryWriteLabelVersion && retryEnabled && !inTransaction {
    tt.Labels = append(tt.Labels, RetryableWriteError)
}
```

For servers with `maxWireVersion >= 9`, the driver trusts the server's
`errorLabels` array exclusively for retryability decisions.

### 10.2 Pool clearing on `NodeIsRecovering` / `NotPrimary` (< 4.2 servers)

```go
// wireVersion42 = 8 (defined in topology/server.go)
if cerr.NodeIsShuttingDown() || wireVersion == nil || wireVersion.Max < wireVersion42 {
    res = driver.ConnectionPoolCleared
    s.pool.clear(err, serviceID)
}
```

For servers older than 4.2 (`maxWireVersion < 8`), the pool is **synchronously**
cleared on any `NodeIsRecovering` or `NotPrimary` error. For 4.2+ servers,
clearing is deferred unless the node is shutting down.

### 10.3 Mongos write concern error retryability (< 4.4)

```go
func (wce WriteConcernError) Retryable(...) bool {
    if serverKind == description.ServerKindMongos && wireVersion.Max < 9 {
        return false   // pre-4.4 mongos already retried; don't retry again
    }
    // otherwise check retryableCodes
}
```

---

## 11. Summary: Field Type Requirements

| Field | Location | BSON type | Note |
|---|---|---|---|
| `ok` | top-level | Int32 / Int64 / Double / Boolean | 1 = success; anything else = error |
| `errmsg` | top-level | String | defaults to `"command failed"` |
| `code` | top-level | **Int32 only** | `Int32OK()` — not `AsInt64OK()` |
| `codeName` | top-level | String | symbolic name |
| `errorLabels` | top-level | Array of String | non-strings skipped |
| `topologyVersion` | top-level | Document `{processId: ObjectID, counter: int64}` | strict types |
| `writeErrors[].index` | writeErrors | Int32 or Int64 | `AsInt64OK()` |
| `writeErrors[].code` | writeErrors | Int32 or Int64 | `AsInt64OK()` |
| `writeErrors[].errmsg` | writeErrors | String | |
| `writeErrors[].errInfo` | writeErrors | Document | optional |
| `writeConcernError.code` | writeConcernError | Int32 or Int64 | `AsInt64OK()` |
| `writeConcernError.codeName` | writeConcernError | String | |
| `writeConcernError.errmsg` | writeConcernError | String | |
| `writeConcernError.errInfo` | writeConcernError | Document | optional |
| `writeConcernError.errorLabels` | writeConcernError | Array of String | merged into top-level labels |
| `topologyVersion.processId` | topologyVersion | ObjectID (12 bytes) | `ObjectIDOK()` — strict |
| `topologyVersion.counter` | topologyVersion | **Int64 only** | `Int64OK()` — not `AsInt64OK()` |
