# MongoDB Driver Connection Handshake Specification

## Provenance

URLs fetched, in order:

1. https://github.com/mongodb/mongo-go-driver/tree/v2.6.1/x/mongo/driver
2. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/operation/hello.go
3. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/topology/connection.go
4. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/description/server.go
5. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/internal/driverutil/description.go
6. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/internal/handshake/handshake.go
7. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/internal/driverutil/hello.go
8. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/operation.go
9. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/driver.go
10. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/legacy.go
11. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/wiremessage/wiremessage.go
12. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/drivertest/channel_conn.go
13. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/topology/server_test.go
14. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/topology/server.go
15. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/topology/connection_test.go
16. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/topology/topology_test.go
17. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/topology/rtt_monitor_test.go
18. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/topology/topology.go
19. https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/topology/topology_options.go

---

## Overview

This document specifies the exact connection handshake performed by the MongoDB Go driver v2.6.1
(`commit 685cf13847cf82b628769502fabf130e5aa84d2f`). The implementation agent must produce server
responses that satisfy the driver's handshake parsing exactly, or the driver will disconnect.

The handshake has two phases:

1. **Initial handshake** — runs once when a new TCP connection is established. Sends a hello
   command with client metadata, receives a server description response.
2. **Finish handshake** — runs authentication if credentials were provided. For unauthenticated
   connections this is a no-op.

---

## Phase 1 — Wire Protocol Selection

The driver selects between two wire protocols for the first message depending on configuration:

| Condition | Protocol | Command name |
|-----------|----------|-------------|
| `serverAPI != nil` OR `loadBalanced == true` | OP_MSG | `"hello"` |
| Otherwise (default) | OP_QUERY | `"isMaster"` |

Source: `operation/hello.go` `isLegacyHandshake()` and `command()`:

```go
// isLegacyHandshake returns True if server API version is not requested and
// loadBalanced is False. If this is the case, then the drivers MUST use legacy
// hello for the first message of the initial handshake with the OP_QUERY protocol
func isLegacyHandshake(srvAPI *driver.ServerAPIOptions, loadbalanced bool) bool {
    return srvAPI == nil && !loadbalanced
}
```

The specific condition for using `OP_QUERY` vs `OP_MSG` in `createWireMessage()` in `operation.go`:

```go
isLegacy := isLegacyHandshake(op, desc)
// isLegacyHandshake(op, desc) returns true when:
//   op.Legacy == LegacyHandshake  AND
//   desc.WireVersion == nil OR desc.WireVersion.Max == 0
// (i.e., we don't know the server's wire version yet)
```

**In practice**: for a fresh connection with no prior server description, `desc.WireVersion` is nil
(zero value), so `isInitialHandshake` is true. Combined with `op.Legacy = driver.LegacyHandshake`
being set on the operation, the driver sends `OP_QUERY` with `"isMaster"` by default.

---

## Phase 1 — OP_QUERY Handshake (Default / Legacy Path)

### Wire message format

The legacy handshake uses `OP_QUERY` (opcode 2004).

**Wire message header** (16 bytes, little-endian):
```
int32  messageLength   // total message length including header
int32  requestID       // monotonically incrementing request ID
int32  responseTo      // 0 for client requests
int32  opCode          // 2004 = OP_QUERY
```

**OP_QUERY body** (after header):
```
int32  flags            // QueryFlags: 0 for no flags, or SecondaryOK (bit 1) if targeting a secondary
cstring fullCollectionName  // "<dbname>.$cmd" = "admin.$cmd"
int32  numberToSkip     // 0
int32  numberToReturn   // -1
document query          // the command document
```

The namespace is always `"admin.$cmd"`.

### Command document fields sent by the driver

The `handshakeCommand()` method in `operation/hello.go` calls `command()` first, then appends
additional fields. Full field list, in order:

#### Fields from `command()` (always present):

| Field | Type | Value | Notes |
|-------|------|-------|-------|
| `"isMaster"` | int32 | `1` | First key — this IS the command name |
| `"helloOk"` | bool | `true` | Tells server driver understands the `hello` command |

Optional fields from `command()` (only present when applicable):

| Field | Type | Condition |
|-------|------|-----------|
| `"topologyVersion"` | document | Only on heartbeats after first connection (not initial handshake) |
| `"maxAwaitTimeMS"` | int64 | Only on streaming heartbeats |
| `"loadBalanced"` | bool (`true`) | Only when `loadBalanced == true` (and never sent as `false`) |
| `"backpressure"` | bool (`true`) | Always appended after `loadBalanced` check |

The `"backpressure": true` field appears unconditionally at the end of `command()`.

#### Fields from `handshakeCommand()` (appended after `command()` fields):

| Field | Type | Condition |
|-------|------|-----------|
| `"saslSupportedMechs"` | string | Only if `saslSupportedMechs != ""` (authentication with username) |
| `"speculativeAuthenticate"` | document | Only if speculative auth doc provided |
| `"compression"` | array of strings | Always present (may be empty array `[]`) |
| `"client"` | document | Client metadata, present if non-empty (see below) |

#### `"compression"` field

Always an array. When no compressors are configured, it is an empty array `[]`. When compressors
are configured, it is an array of strings such as `["snappy", "zlib", "zstd"]`.

#### `"client"` metadata document

Maximum size: **512 bytes** (for sharded clusters; 1024 for standalone/replica set, but the driver
uses 512 as the universal limit).

Structure (fields omitted in order to fit within size limit):

```
{
  "application": { "name": "<appname>" },     // omitted if appname is ""
  "driver": {
    "name": "mongo-go-driver",                // or "mongo-go-driver|<outerLibName>"
    "version": "<driver version>"             // or "<ver>|<outerLibVer>"
  },
  "os": {
    "type": "<runtime.GOOS>",                 // e.g. "linux", "darwin", "windows"
    "architecture": "<runtime.GOARCH>"        // e.g. "amd64", "arm64" — omitted when truncating
  },
  "platform": "<runtime.Version()>",          // e.g. "go1.21.0" — truncated if needed
  "env": {                                    // omitted entirely if no FaaS/container detected
    "name": "<faas-name>",                    // "aws.lambda", "azure.func", "gcp.func", "vercel"
    "memory_mb": <int32>,                     // FaaS-specific, omitted when truncating
    "region": "<string>",                     // FaaS-specific, omitted when truncating
    "timeout_sec": <int32>,                   // GCP-only, omitted when truncating
    "container": {                            // omitted if no container detected
      "runtime": "docker",                    // present if /.dockerenv exists
      "orchestrator": "kubernetes"            // present if KUBERNETES_SERVICE_HOST is set
    }
  }
}
```

**Truncation order** when document exceeds 512 bytes:
1. Omit `env` fields except `env.name`
2. Omit `os` fields except `os.type`
3. Omit entire `env` document
4. Omit `platform` entirely
5. If still too large, return nil (don't send `"client"` field at all)

**Heartbeats do not include the `"client"` field.** Only the initial per-connection handshake sends
client metadata. This is confirmed by `server_test.go`:

```go
if includesClientMetadata(t, wm) {
    t.Fatal("client metadata not expected in heartbeat but found")
}
```

---

## Phase 1 — OP_MSG Handshake (API Version / Load Balanced Path)

When `serverAPI != nil` or `loadBalanced == true`, the driver uses `OP_MSG` (opcode 2013).

**Wire message header** (16 bytes): same format as OP_QUERY.

**OP_MSG body** (after header):
```
uint32 flagBits         // MsgFlag: 0 for normal, ExhaustAllowed (bit 16) if streaming supported
uint8  sectionType      // 0 = SingleDocument
document body           // the command document
```

The command document in OP_MSG includes the same fields as in OP_QUERY, but the command name is
`"hello"` (not `"isMaster"`) and the document additionally includes:
- `"$db": "admin"` — always appended
- `"$readPreference": {...}` — only if applicable

The key difference in the command name selection (from `command()`):

```go
if h.loadBalanced || h.serverAPI != nil || desc.HelloOK {
    dst = bsoncore.AppendInt32Element(dst, "hello", 1)
} else {
    dst = bsoncore.AppendInt32Element(dst, handshake.LegacyHello, 1)  // "isMaster"
}
```

---

## Phase 1 — Server Response Requirements

The server response must be an **OP_REPLY** (opcode 1) for the OP_QUERY path, or an **OP_MSG** for
the OP_MSG path.

### OP_REPLY format (for legacy OP_QUERY handshake)

```
int32   messageLength
int32   requestID
int32   responseTo      // must match the requestID from the client's query
int32   opCode          // 1 = OP_REPLY
int32   responseFlags   // ReplyFlag: 0 is fine; AwaitCapable (bit 3) may be set
int64   cursorID        // 0
int32   startingFrom    // 0
int32   numberReturned  // 1
document doc            // the response document
```

The test helper `MakeReply()` in `drivertest/channel_conn.go` constructs exactly this:

```go
func MakeReply(doc bsoncore.Document) []byte {
    var dst []byte
    idx, dst := wiremessage.AppendHeaderStart(dst, 10, 9, wiremessage.OpReply)
    dst = wiremessage.AppendReplyFlags(dst, 0)
    dst = wiremessage.AppendReplyCursorID(dst, 0)
    dst = wiremessage.AppendReplyStartingFrom(dst, 0)
    dst = wiremessage.AppendReplyNumberReturned(dst, 1)
    dst = append(dst, doc...)
    return bsoncore.UpdateLength(dst, idx, int32(len(dst[idx:])))
}
```

The minimal valid response used in tests is `{ok: 1}` (confirmed by `makeHelloReply()` in
`rtt_monitor_test.go`):

```go
func makeHelloReply() []byte {
    doc := bsoncore.NewDocumentBuilder().AppendInt32("ok", 1).Build()
    return drivertest.MakeReply(doc)
}
```

### Response document fields parsed by the driver

The driver parses the response document via `NewServerDescription()` in
`internal/driverutil/description.go`. Every field listed below is parsed by a `switch` on the
element key. Any unrecognized key is silently ignored.

#### Mandatory for a successful handshake

| Field | BSON type | Notes |
|-------|-----------|-------|
| `"ok"` | int32/int64/double | **MUST be `1`**. Any other value sets `LastError = "not ok"` and the driver considers the server broken. |

#### Fields that determine server kind

The driver determines the server kind from a combination of these fields:

| Field | BSON type | Notes |
|-------|-----------|-------|
| `"isWritablePrimary"` | bool | `true` → writable primary |
| `"ismaster"` | bool | Legacy alias for `isWritablePrimary` (lowercase) |
| `"secondary"` | bool | `true` → RS secondary |
| `"arbiterOnly"` | bool | `true` → RS arbiter |
| `"hidden"` | bool | `true` → RS hidden member |
| `"isreplicaset"` | bool | `true` → RS ghost (announced RS but no set name yet) |
| `"setName"` | string | Non-empty → this is a replica set member |
| `"msg"` | string | `"isdbgrid"` → this is a mongos |

Server kind determination logic (in priority order from `NewServerDescription()`):

```
if isReplicaSet == true:
    Kind = RSGhost
elif setName != "":
    if isWritablePrimary:  Kind = RSPrimary
    elif hidden:           Kind = RSMember
    elif secondary:        Kind = RSSecondary
    elif arbiterOnly:      Kind = RSArbiter
    else:                  Kind = RSMember
elif msg == "isdbgrid":
    Kind = Mongos
else:
    Kind = Standalone   ← default
```

#### Wire version (critical for compatibility)

| Field | BSON type | Notes |
|-------|-----------|-------|
| `"minWireVersion"` | int32/int64 | Minimum wire protocol version supported by server |
| `"maxWireVersion"` | int32/int64 | Maximum wire protocol version supported by server |

These two fields are parsed into a `VersionRange{Min, Max}` struct stored as `desc.WireVersion`.

**Driver compatibility check** (from `topology_test.go` and `description.go`):

```go
const (
    MinWireVersion = 8   // driver requires server to support AT LEAST wire version 8 (MongoDB 4.2)
    MaxWireVersion = 25  // driver supports UP TO wire version 25
)
```

The topology will set a `CompatibilityErr` if:
- `server.WireVersion.Min > MaxWireVersion (25)` — server requires a higher protocol than driver supports
- `server.WireVersion.Max < MinWireVersion (8)` — server only supports older protocol than driver requires

This means:
- **`maxWireVersion` MUST be `>= 8`** or the driver will consider the server incompatible
- **`minWireVersion` MUST be `<= 25`** or the driver will consider the server incompatible

For a modern simulated server, returning `minWireVersion: 8, maxWireVersion: 25` (or any value in
the range `[8, 25]` for max) will pass compatibility checks.

#### Fields that control capacity and limits

| Field | BSON type | Required? | Default used if absent | Notes |
|-------|-----------|-----------|------------------------|-------|
| `"maxBsonObjectSize"` | int32/int64 | No | Driver uses its own hard-coded limit | Stored as `desc.MaxDocumentSize` (uint32). Standard MongoDB value: `16777216` (16 MiB) |
| `"maxMessageSizeBytes"` | int32/int64 | No | `defaultMaxMessageSize = 48000000` | Stored as `desc.MaxMessageSize` (uint32). Standard MongoDB value: `48000000` (48 MB) |
| `"maxWriteBatchSize"` | int32/int64 | No | Not stored (zero = no limit used) | Stored as `desc.MaxBatchCount` (uint32). Standard MongoDB value: `100000` |

From `topology/connection.go`:

```go
var defaultMaxMessageSize uint32 = 48000000
// ...
// In the case of a hello response where MaxMessageSize has not yet been set, use the hard-coded
// defaultMaxMessageSize instead.
maxMessageSize := c.desc.MaxMessageSize
if maxMessageSize == 0 {
    maxMessageSize = defaultMaxMessageSize
}
```

This means `"maxMessageSizeBytes"` is **not strictly required**; the driver falls back to 48 MB.
However, if `"maxBsonObjectSize"` is absent, the driver may reject large documents. Both fields
should be provided to avoid unexpected behavior.

#### Session support

| Field | BSON type | Required? | Notes |
|-------|-----------|-----------|-------|
| `"logicalSessionTimeoutMinutes"` | int32/int64 | No | Stored as `*int64`. If absent (`nil`), sessions are not supported. Standard value: `30` |

If `logicalSessionTimeoutMinutes` is absent, the driver's session support is disabled for this
server. For a server that should support sessions, return this field.

#### Time

| Field | BSON type | Required? | Notes |
|-------|-----------|-----------|-------|
| `"localTime"` | datetime | No | **Not parsed at all by `NewServerDescription()`** — the driver does not extract `localTime` from the hello response. It is not stored anywhere in the description. |

**Important:** `"localTime"` does NOT need to be returned. The driver ignores it in the handshake
response.

#### Other fields parsed but not critical

| Field | BSON type | Notes |
|-------|-----------|-------|
| `"helloOk"` | bool | If `true`, stored as `desc.HelloOK`. Enables `"hello"` command in subsequent heartbeats instead of `"isMaster"`. |
| `"compression"` | array of strings | Stored as `desc.Compression`. Enables wire compression if a mutually supported compressor is found. |
| `"hosts"` | array of strings | RS member list |
| `"passives"` | array of strings | RS passive member list |
| `"arbiters"` | array of strings | RS arbiter list |
| `"primary"` | string | RS primary address |
| `"me"` | string | This server's canonical address (used to set `desc.CanonicalAddr`) |
| `"electionId"` | ObjectID | RS election ID |
| `"setName"` | string | RS set name |
| `"setVersion"` | int32/int64 | RS set version |
| `"tags"` | document | String→string map of server tags |
| `"topologyVersion"` | document `{processId: ObjectID, counter: int64}` | For streaming monitoring |
| `"serviceId"` | ObjectID | Present only for load-balanced deployments |
| `"readOnly"` | bool | Whether this is a read-only server |
| `"passive"` | bool | Whether this is a passive server |
| `"iscryptd"` | bool | Whether this is a mongocryptd |
| `"lastWrite"` | document `{lastWriteDate: datetime}` | Last write time |
| `"connectionId"` | int32/int64 | **Server-assigned connection ID** — stored as `serverConnectionID` on the connection. Read via `h.res.Lookup("connectionId").AsInt64OK()`. |
| `"speculativeAuthenticate"` | document | Returned when speculative auth was requested |
| `"saslSupportedMechs"` | array of strings | SASL mechanisms supported for the user |

---

## Post-Handshake: Behaviour Based on Response

### Wire version negotiation

After receiving the server description, the driver stores `WireVersion` from `minWireVersion` /
`maxWireVersion`. **All subsequent operations check `conn.Description().WireVersion.Max`** against
required minimum versions:

| Feature | Required `maxWireVersion` |
|---------|--------------------------|
| Auto-encryption | `>= 8` |
| Read snapshots | `>= 13` |
| Pre-4.4 retry label behavior | `< 9` |
| Pool-clearing on error | `< 8` (wireVersion42 constant) |

A server should return `maxWireVersion >= 8` to avoid the driver attempting pre-4.2 legacy
behaviors.

### Connection compression

After a successful handshake, if `desc.Compression` is non-empty, the driver negotiates a
compressor in `topology/connection.go`:

```go
if len(c.desc.Compression) > 0 {
clientMethodLoop:
    for _, method := range c.config.compressors {
        for _, serverMethod := range c.desc.Compression {
            if method != serverMethod { continue }
            switch strings.ToLower(method) {
            case "snappy": c.compressor = wiremessage.CompressorSnappy
            case "zlib":   c.compressor = wiremessage.CompressorZLib; ...
            case "zstd":   c.compressor = wiremessage.CompressorZstd; ...
            }
            break clientMethodLoop
        }
    }
}
```

If the server returns `"compression": []` (empty) or the intersection of client and server
compressor lists is empty, no compression is used. The driver will then send all subsequent wire
messages uncompressed.

### Load balancing validation

If the client was configured with `loadBalanced=true`:
- The server's response **MUST** include `"serviceId"` (an ObjectID)
- If `"serviceId"` is absent, the connection fails with `errLoadBalancedStateMismatch`

```go
if c.config.loadBalanced && c.desc.ServiceID == nil {
    return ConnectionError{Wrapped: errLoadBalancedStateMismatch, init: true}
}
```

---

## Complete Minimal Valid Handshake Response

For a standalone server (no authentication, no sessions, no compression), the absolute minimum
response document that will satisfy the driver is:

```bson
{
  "ok": 1
}
```

This will parse as `Kind = Standalone` with no wire version set (`WireVersion = &{0, 0}`). This
will likely trigger a compatibility error because `maxWireVersion = 0 < MinWireVersion (8)`.

### Recommended minimal standalone response

```bson
{
  "ok": 1,
  "ismaster": true,
  "minWireVersion": 8,
  "maxWireVersion": 25,
  "maxBsonObjectSize": 16777216,
  "maxMessageSizeBytes": 48000000,
  "maxWriteBatchSize": 100000,
  "logicalSessionTimeoutMinutes": 30
}
```

This produces `Kind = Standalone`, `WireVersion = {8, 25}`, full capacity fields, session support.

### Recommended replica set primary response

```bson
{
  "ok": 1,
  "isWritablePrimary": true,
  "setName": "rs0",
  "minWireVersion": 8,
  "maxWireVersion": 25,
  "maxBsonObjectSize": 16777216,
  "maxMessageSizeBytes": 48000000,
  "maxWriteBatchSize": 100000,
  "logicalSessionTimeoutMinutes": 30,
  "hosts": ["localhost:27017"],
  "primary": "localhost:27017",
  "me": "localhost:27017",
  "helloOk": true,
  "topologyVersion": { "processId": <ObjectID>, "counter": 0 }
}
```

---

## Handshake Sequence Diagram

```
Client                                       Server
  |                                            |
  |--- TCP connect --------------------------->|
  |                                            |
  |--- OP_QUERY (or OP_MSG) ----------------->|
  |    Target: admin.$cmd                      |
  |    {                                       |
  |      isMaster: 1,     (or "hello": 1)     |
  |      helloOk: true,                        |
  |      backpressure: true,                   |
  |      compression: [...],                   |
  |      client: {                             |
  |        driver: {name, version},            |
  |        os: {type, architecture},           |
  |        platform: "go1.x.x",               |
  |        application: {name: "..."}          |
  |      }                                     |
  |    }                                       |
  |                                            |
  |<-- OP_REPLY (or OP_MSG) ------------------|
  |    {                                       |
  |      ok: 1,                                |
  |      ismaster: true,                       |
  |      minWireVersion: 8,                    |
  |      maxWireVersion: 25,                   |
  |      maxBsonObjectSize: 16777216,          |
  |      maxMessageSizeBytes: 48000000,        |
  |      maxWriteBatchSize: 100000,            |
  |      logicalSessionTimeoutMinutes: 30,     |
  |      connectionId: <int32>                 |
  |    }                                       |
  |                                            |
  |  [if auth configured: SASL exchange]       |
  |                                            |
  |--- first application command ------------->|
```

---

## Key Constants and Their Sources

| Constant | Value | Source file |
|----------|-------|-------------|
| `LegacyHello` | `"isMaster"` | `internal/handshake/handshake.go` |
| `LegacyHelloLowercase` | `"ismaster"` | `internal/handshake/handshake.go` |
| `maxClientMetadataSize` | `512` | `x/mongo/driver/operation/hello.go` |
| `defaultMaxMessageSize` | `48000000` | `x/mongo/driver/topology/connection.go` |
| `MinWireVersion` | `8` | `internal/driverutil/description.go` |
| `MaxWireVersion` | `25` | `internal/driverutil/description.go` |
| `wireVersion42` | `8` | `x/mongo/driver/topology/server.go` |
| `cryptMinWireVersion` | `8` | `x/mongo/driver/operation.go` |
| `readSnapshotMinWireVersion` | `13` | `x/mongo/driver/operation.go` |

---

## BSON Encoding Rules for the Response

From `internal/driverutil/description.go`, the parser uses `element.Value().AsInt64OK()` for
integer fields. This means:

- Integer fields (`maxBsonObjectSize`, `maxMessageSizeBytes`, etc.) may be BSON `int32` OR
  `int64`. The driver uses `AsInt64OK()` which accepts both.
- Boolean fields (`ismaster`, `helloOk`, etc.) must be BSON boolean type. The driver uses
  `element.Value().BooleanOK()`.
- String fields (`setName`, `msg`, etc.) must be BSON UTF-8 string.
- `"ok"` uses `element.Value().AsInt64OK()` — can be BSON int32, int64, or double coercible to
  int64. Value must equal `1` or the description records an error.
- `"connectionId"` is read with `.AsInt64OK()` — can be BSON int32 or int64.

---

## Heartbeat vs. Initial Handshake Differences

| Aspect | Initial Handshake | Heartbeat |
|--------|-------------------|-----------|
| `"client"` metadata | Yes (first connection only) | No |
| `"saslSupportedMechs"` | Yes, if auth username present | No |
| `"speculativeAuthenticate"` | Yes, if speculative auth | No |
| `"compression"` | Yes, always | No (not in heartbeat `command()`) |
| `"topologyVersion"` | No (unknown on first connect) | Yes, if streaming |
| `"maxAwaitTimeMS"` | No | Yes, if streaming |
| Wire message | `GetHandshakeInformation()` path | `check()`/`doHandshake()` path |
| Command called | `handshakeCommand()` | `command()` only |

The server-side heartbeat connection uses `createBaseOperation()` which creates a `Hello` operation
with no extra options, and calls `Execute()` directly (not `GetHandshakeInformation()`). The
heartbeat path calls only `command()`, not `handshakeCommand()`.

---

## Error Handling Behavior

If the handshake response has `ok != 1`, or if any required field cannot be decoded (wrong BSON
type), `NewServerDescription()` sets `desc.LastError` and returns immediately. The connection is
not necessarily closed at this point — the description will have `Kind = Standalone` with a
`LastError`. The topology layer will handle this as an unknown/unusable server.

If the actual wire-level `connect()` fails (i.e., sending or receiving the OP_QUERY message
fails), the error is wrapped as a `ConnectionError` with `init: true` and returned through
`ProcessHandshakeError()`, which may clear the connection pool.

The driver imposes a **48 MB default maximum message size** for incoming responses. Any response
exceeding this triggers `errResponseTooLarge` and closes the connection. After a successful
handshake, if `maxMessageSizeBytes` was returned by the server, that value is used instead:

```go
maxMessageSize := c.desc.MaxMessageSize
if maxMessageSize == 0 {
    maxMessageSize = defaultMaxMessageSize
}
if uint32(size) > maxMessageSize {
    return 0, errResponseTooLarge
}
```
