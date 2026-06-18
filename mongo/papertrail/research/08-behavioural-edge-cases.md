# Behavioural Edge Cases — Implementation Specification

```yaml
provenance:
  urls_fetched_in_order:
    - https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/errors.go
    - https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/operation.go
    - https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/operation_test.go
    - https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/topology/server.go
    - https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/topology/server_test.go
    - https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/topology/connection.go
    - https://bsonspec.org/spec.html
```

---

## Overview

This document specifies three categories of edge-case behaviour observed in the
Go driver v2.6.1 test suite and source code:

1. **Unknown commands** — what the driver expects when it sends a command the
   server does not recognise.
2. **Malformed BSON** — how the driver handles structurally invalid responses.
3. **Reconnection** — what happens when a connection is lost, including the
   heartbeat-based monitoring loop, pool clearing, and the exact conditions
   under which re-connections are attempted.

Prior documents define the wire protocol (`wire-protocol-messages.md`),
handshake (`04-connection-handshake.md`), command payloads
(`05-core-command-specifications.md`), error structure
(`06-error-response-structure.md`), and session/cursor state
(`07-session-cursor-state.md`). Definitions from those documents are not
repeated here.

---

## 1. Unknown Commands

### 1.1 What the driver expects

The driver has no special handling for "unknown command" responses. It treats
them identically to any other `ok: 0` command error. From the driver's
perspective, the response from an unrecognised command is simply an error
document that should be processed by `ExtractErrorFromServerResponse`.

A server that receives a command it does not understand **MUST** respond with
a standard `ok: 0` error document. The idiomatic MongoDB response is:

```bson
{
  "ok":       { $numberInt: "0" },
  "errmsg":   "no such command: '<commandName>'",
  "code":     { $numberInt: "59" },
  "codeName": "CommandNotFound"
}
```

Key facts:

- **Error code 59** is `CommandNotFound`. It is not in the driver's
  `retryableCodes` list, so the driver will **not** retry the operation.
- The driver does not have a dedicated `CommandNotFound` predicate; the
  response is surfaced to the caller as a `driver.Error` with `Code = 59`.
- The `errmsg` field is required but its content is not checked by the driver.
  It is surfaced as `Error.Message`.

### 1.2 Dispatch path in the driver

The full dispatch path when a command fails:

```
Operation.Execute
  → roundTrip (write + read)
  → readWireMessage
    → decodeResult(opcode, rem)
      → ExtractErrorFromServerResponse(doc)
        → returns driver.Error{Code: 59, ...}
  → checkError switch
    case Error:
      → err.RetryableWrite / err.RetryableRead
        → code 59 not in retryableCodes → retryableErr = false
      → return driver.Error{Code: 59, ...}
```

The caller receives a `driver.Error` value. The `ProcessError` method on the
server is called with this error:

```go
if ep, ok := srvr.(ErrorProcessor); ok {
    _ = ep.ProcessError(err, conn)
}
```

`ProcessError` evaluates the error:

```go
if cerr, ok := err.(driver.Error); ok && (cerr.NodeIsRecovering() || cerr.NotPrimary()) {
    // ... topology invalidation ...
}
```

For code 59, neither `NodeIsRecovering()` nor `NotPrimary()` is true. The next
check is:

```go
wrappedConnErr := unwrapConnectionError(err)
if wrappedConnErr == nil {
    return driver.NoChange
}
```

A `driver.Error` from the server is not a `ConnectionError`, so
`unwrapConnectionError` returns `nil`. The result is `driver.NoChange`:
**the server description is not updated and the connection pool is not cleared.**

### 1.3 Behavioural summary

| Aspect | Behaviour |
|---|---|
| Expected response shape | Standard `ok: 0` document (see §1.1) |
| Error type returned to caller | `driver.Error` with `Code = 59` |
| Operation retried | No (code 59 not retryable) |
| Server description changed | No (`driver.NoChange`) |
| Connection pool cleared | No |
| Connection returned to pool | Yes (no connection error occurred) |

### 1.4 Null / empty command name

If the command document contains an empty string as the first key (the command
name), the driver will still send the request and process the `ok: 0` response
using the same path above. The driver never validates the command name before
sending.

---

## 2. Malformed BSON

This section covers four distinct failure modes: malformed wire messages,
malformed BSON documents, type mismatches on well-known fields, and malformed
BSON within cursor responses.

### 2.1 Malformed wire message length

The first 4 bytes of any wire message encode the total message length as a
little-endian `int32`. The driver reads this in `parseWmSizeBytes`:

```go
func (c *connection) parseWmSizeBytes(wmSizeBytes [4]byte) (int32, error) {
    size := int32(binary.LittleEndian.Uint32(wmSizeBytes[:]))
    if size < 4 {
        return 0, fmt.Errorf("malformed message length: %d", size)
    }
    maxMessageSize := c.desc.MaxMessageSize
    if maxMessageSize == 0 {
        maxMessageSize = defaultMaxMessageSize  // 48,000,000 bytes
    }
    if uint32(size) > maxMessageSize {
        return 0, errResponseTooLarge
    }
    return size, nil
}
```

Rules derived from source:

| Condition | Error | Connection closed |
|---|---|---|
| `size < 4` | `"malformed message length: N"` | Yes |
| `size > maxMessageSize` | `errResponseTooLarge` | Yes (unless CSOT timeout case) |
| Size valid but body unreadable (e.g. connection closed mid-read) | `"incomplete read of full message"` | Yes (unless CSOT) |
| First 4 bytes unreadable | `"incomplete read of message header"` | Yes (unless CSOT) |

**`errResponseTooLarge`** is the sentinel `errors.New("length of read message too large")`.

**CSOT (Client-Side Operations Timeout) exception**: When a timeout error
occurs during `io.ReadFull`, the driver may mark the connection as
`awaitRemainingBytes` rather than closing it immediately, allowing the pool to
drain the unread response before returning the connection to the pool. This
only applies to timeout errors (`netErr.Timeout() == true`), not to other
errors.

### 2.2 Malformed OP_REPLY: zero-length document loop guard

Test `TestDecodeOpReply/malformatted_wiremessage_with_length_of_0`
(GODRIVER-2869) confirms that a zero-length document field in an OP_REPLY body
must not cause an infinite loop:

```go
// From operation_test.go:
t.Run("malformatted wiremessage with length of 0", func(t *testing.T) {
    var wm []byte
    wm = wiremessage.AppendReplyFlags(wm, 0)
    wm = wiremessage.AppendReplyCursorID(wm, int64(0))
    wm = wiremessage.AppendReplyStartingFrom(wm, 0)
    wm = wiremessage.AppendReplyNumberReturned(wm, 0)
    idx, wm := bsoncore.ReserveLength(wm)
    wm = bsoncore.UpdateLength(wm, idx, 0)  // length = 0
    reply := Operation{}.decodeOpReply(wm)
    assert.Equal(t, []bsoncore.Document(nil), reply.documents)
})
```

The driver's `decodeOpReply` handles a zero-length BSON document by producing
`nil` documents — it does not loop or panic. **A server implementation must
never send a BSON document with a length field of 0** (minimum valid length is
5: 4-byte length + 1-byte terminator `0x00`). A document whose length field
equals 0 will cause `decodeOpReply` to return no documents; the operation will
see `ErrNoDocCommandResponse`.

### 2.3 Malformed OP_MSG header

In `readWireMessage` (operation.go):

```go
length, _, _, opcode, rem, ok := wiremessage.ReadHeader(wm)
if !ok || len(wm) < int(length) {
    return nil, errors.New("malformed wire message: insufficient bytes")
}
```

- If the header cannot be parsed (`!ok`): the driver returns `"malformed wire
  message: insufficient bytes"` as a plain `error`.
- If the message body is shorter than the declared `length`: same error.

Neither of these paths is wrapped in `driver.Error`, so `ProcessError` will
treat them as non-server errors. The caller sees a plain Go error; the server
description is **not** changed (because these errors do not pass through the
`driver.Error` SDAM path).

However, since these errors come from `readWireMessage`, which is called in the
context of `roundTrip`, they propagate through `networkError`:

```go
func (op Operation) roundTrip(ctx context.Context, conn *mnet.Connection, wm []byte) ([]byte, error) {
    err := conn.Write(ctx, wm)
    if err != nil {
        return nil, op.networkError(err)
    }
    return op.readWireMessage(ctx, conn)
}
```

Wait — `networkError` is only called for write errors. For read errors from
`conn.Read`, the wrapping happens in `readWireMessage` itself, which calls
`op.networkError(err)`:

```go
func (op Operation) readWireMessage(ctx context.Context, conn *mnet.Connection) (result []byte, err error) {
    wm, err := conn.Read(ctx)
    if err != nil {
        return nil, op.networkError(err)
    }
    // ...
    length, _, _, opcode, rem, ok := wiremessage.ReadHeader(wm)
    if !ok || len(wm) < int(length) {
        return nil, errors.New("malformed wire message: insufficient bytes")
    }
    // ...
}
```

The malformed-header error path **bypasses** `networkError`. It returns a plain
`error`, not a `driver.Error` with `NetworkError` label. This means:

- The error is **not** labelled `RetryableWriteError` by the driver.
- `ProcessError` will call `unwrapConnectionError`, which returns `nil` for a
  plain `error`, resulting in `driver.NoChange`.
- The operation is **not** retried.

### 2.4 Malformed BSON document body

If the wire message is structurally valid but the BSON document embedded in it
is malformed, `ExtractErrorFromServerResponse` is called. The function begins
with:

```go
elems, err := doc.Elements()
if err != nil {
    return err
}
```

`bsoncore.Document.Elements()` will return an error if:
- The declared document length does not match the actual byte slice length.
- A type byte references a type whose encoding length is known but the
  remaining bytes are insufficient (e.g. a declared `int32` field with only 2
  bytes available).
- The document is not null-terminated (missing `0x00` terminator).

The error from `doc.Elements()` is returned directly as a Go `error` (not a
`driver.Error`). The caller receives this raw error through the same path as
§2.3, meaning **no retry, no server-description change**.

### 2.5 Type mismatches on well-known fields

When a BSON document is structurally valid but a field has an unexpected BSON
type, the driver silently ignores the field and uses a default value. Confirmed
examples (from `ExtractErrorFromServerResponse`):

| Field | Expected type | What happens on wrong type |
|---|---|---|
| `ok` | Int32/Int64/Double/Boolean | Type not matched → `ok` stays `false` → treated as error |
| `errmsg` | String | `StringValueOK()` returns false → `errmsg` stays `""` → defaults to `"command failed"` |
| `code` | Int32 only | `Int32OK()` returns false → `code` stays 0 |
| `codeName` | String | `StringValueOK()` returns false → `codeName` stays `""` |
| `errorLabels` | Array | `ArrayOK()` returns false → labels stay nil |
| `writeErrors` | Array | `ArrayOK()` returns false → no write errors decoded |
| `writeConcernError` | Document | `DocumentOK()` returns false → no WCE decoded |
| `topologyVersion` | Document | `DocumentOK()` returns false → `tv` stays nil |

**Critical**: if `ok` has a BSON type that is none of Int32, Int64, Double, or
Boolean, the response is treated as `ok: 0` (failure). For example, if `ok` is
a BSON String `"1"`, the response is treated as an error.

For cursor responses (`NewCursorResponse`), the cursor ID field has **strict**
type checking (no coercion):

```go
case "id":
    id, ok := elem.Value().Int64OK()
    if !ok {
        return CursorResponse{},
            fmt.Errorf("id should be an int64 but it is a BSON %s", elem.Value().Type)
    }
```

A cursor ID encoded as `int32` will cause `NewCursorResponse` to return an
error, aborting the operation.

### 2.6 Compressed message with malformed content

From `decompressWireMessage` (operation.go):

```go
func (Operation) decompressWireMessage(wm []byte) (wiremessage.OpCode, []byte, error) {
    opcode, rem, ok := wiremessage.ReadCompressedOriginalOpCode(wm)
    if !ok {
        return 0, nil, errors.New("malformed OP_COMPRESSED: missing original opcode")
    }
    uncompressedSize, rem, ok := wiremessage.ReadCompressedUncompressedSize(rem)
    if !ok {
        return 0, nil, errors.New("malformed OP_COMPRESSED: missing uncompressed size")
    }
    compressorID, rem, ok := wiremessage.ReadCompressedCompressorID(rem)
    if !ok {
        return 0, nil, errors.New("malformed OP_COMPRESSED: missing compressor ID")
    }
    // ...
    uncompressed, err := DecompressPayload(rem, opts)
    if err != nil {
        return 0, nil, err
    }
    return opcode, uncompressed, nil
}
```

Each of these errors is returned as a plain `error` from `readWireMessage`.
They follow the §2.3 path: no retry, no SDAM update.

### 2.7 BSON spec constraints (from bsonspec.org)

Per the BSON specification, a compliant implementation must enforce:

| Rule | What happens if violated in a driver response |
|---|---|
| Document `int32` length must equal the total byte count (including the length itself and the terminating `0x00`) | `doc.Elements()` error → §2.4 path |
| Minimum document length is 5 bytes (`int32(5)` + `0x00`) | Effectively `size < 4` in wire length check (§2.1) |
| All cstrings must be null-terminated | Parse error in `bsoncore` element iteration |
| String fields must include the correct `int32` byte count | Overrun detected by `bsoncore` parser |
| Type bytes must be one of the defined values (0x01–0x13, 0xFF, 0x7F) | Unknown type causes `bsoncore` to return a parse error from `Elements()` |

The driver uses `bsoncore` for all BSON parsing. `bsoncore` validates length
fields and type bytes as it iterates. Any violation returns an error from
`Elements()` or the individual `Value()` accessors.

---

## 3. Reconnection Behaviour

### 3.1 Architecture: the monitoring goroutine

Each `Server` object in the topology layer runs a single background goroutine
(`update()`) that continuously monitors the server's health. This goroutine is
started by `Connect()` (unless `monitoringDisabled` or `loadBalanced` is set)
and stopped by `Disconnect()`.

```go
func (s *Server) Connect(updateCallback updateTopologyCallback) error {
    // ...
    if !s.cfg.monitoringDisabled && !s.cfg.loadBalanced {
        s.closewg.Add(1)
        go s.update()
    }
    return s.pool.ready()
}
```

The monitoring goroutine uses a **dedicated** `*connection` (`s.conn`) that is
separate from the application connection pool. This dedicated connection is
only used for `hello`/`isMaster` heartbeats.

### 3.2 Heartbeat timing

From `server.go`:

```go
const (
    minHeartbeatInterval = 500 * time.Millisecond
    // default heartbeatInterval (from configuration) is typically 10 seconds
)
```

The `update()` loop calls `waitUntilNextCheck()` between heartbeats:

```go
func waitUntilNextCheck() {
    select {
    case <-heartbeatTicker.C:     // fires every heartbeatInterval (default 10s)
    case <-checkNow:              // immediate check requested by operation after error
    case <-done:                  // server disconnecting
        return
    }
    // Enforce minimum heartbeat interval:
    select {
    case <-rateLimiter.C:         // fires every 500ms
    case <-done:
        return
    }
}
```

There are therefore two timers:

1. `heartbeatTicker`: fires every `heartbeatInterval` (configurable, default
   10 s). This drives periodic health checks.
2. `rateLimiter` (`minHeartbeatInterval = 500 ms`): rate-limits check
   frequency; even if `checkNow` is signalled repeatedly, the driver waits at
   least 500 ms between consecutive checks.

`RequestImmediateCheck()` sends to `checkNow` (non-blocking):

```go
func (s *Server) RequestImmediateCheck() {
    select {
    case s.checkNow <- struct{}{}:
    default:
    }
}
```

An immediate check is requested by `ProcessError` when the server receives a
state-changing error (e.g. `NotPrimary`, `NodeIsRecovering`).

### 3.3 When a new connection is established

The `check()` function decides whether to use the existing `s.conn` or create
a new one:

```go
func (s *Server) check(ctx context.Context) (description.Server, error) {
    // Create new connection if:
    //   1. s.conn == nil (first check)
    //   2. s.conn.closed() (previous connection was closed due to error)
    //   3. s.conn.previousCanceled() (previous check was cancelled)
    if s.conn == nil || s.conn.closed() || previousCanceled {
        err = s.setupHeartbeatConnection(ctx)
        // setupHeartbeatConnection calls s.createConnection() then conn.connect()
        // which performs the full TLS + hello handshake
        if err == nil {
            s.rttMonitor.addSample(s.conn.helloRTT)
            descPtr = &s.conn.desc
        }
    } else {
        // Use existing connection
        tempDesc, err = doHandshake(ctx, s)
    }
}
```

A new TCP connection is established (and the full hello handshake re-run)
whenever the previous connection was marked closed or cancelled. See §3.4 for
the conditions that close the connection.

### 3.4 Conditions that close the heartbeat connection

The heartbeat connection (`s.conn`) is closed in the following cases:

| Cause | Code path |
|---|---|
| Any error returned by `doHandshake` | `s.conn.close()` called in `check()` |
| `ProcessError` receives a non-timeout network error | `s.heartbeatListener.StopListening()` → cancels `checkServerWithSignal` context → goroutine closes `s.conn` |
| `ProcessHandshakeError` (connection pool handshake error) | `s.heartbeatListener.StopListening()` |
| `Disconnect()` called | `s.heartbeatListener.StopListening()` → `pool.close()` |

The `heartbeatListener` is a `contextListener`. Calling `StopListening()` on it
causes the context passed to `checkServerWithSignal` to be cancelled, which in
turn triggers:

```go
go func(conn *connection) {
    defer cancel()
    var aborted bool
    listener.Listen(ctx, func() {
        aborted = true
    })
    if !aborted {
        conn.closeConnectContext()
        conn.wait()
        conn.prevCanceled.Store(true)
        _ = conn.close()
    }
}(conn)
```

After `s.conn.close()` is called, `s.conn.closed()` returns `true`, so the
next iteration of the `update()` loop calls `setupHeartbeatConnection()` to
dial a fresh TCP connection.

### 3.5 Connection pool clearing on error

The application connection pool (`s.pool`) is managed separately from the
monitoring connection. The pool is cleared in the following cases:

| Cause | Code path | Pool action |
|---|---|---|
| Non-timeout network error on app connection | `ProcessError` → `s.pool.clear(err, serviceID)` | **Cleared** (generation incremented) |
| `NodeIsShuttingDown` error (codes 91, 11600) | `ProcessError` | **Cleared** |
| `NotPrimary` / `NodeIsRecovering` on server with wire version < 8 (pre-4.2) | `ProcessError` | **Cleared** |
| `NotPrimary` / `NodeIsRecovering` on server with wire version ≥ 8 | `ProcessError` | **Not cleared** (only server description updated) |
| First timeout in FAAS pause (GODRIVER-2577) | `update()` loop | Not cleared immediately; retry first |
| Second consecutive timeout | `update()` loop | `pool.clearAll(err, nil)` |
| `ProcessHandshakeError` (connection-init error) | `ProcessHandshakeError` | **Cleared** |

The pool is NOT cleared for:

- Command errors (`ok: 0`) that do not involve `NodeIsRecovering` or `NotPrimary`.
- Transient timeout errors (single occurrence, `netErr.Timeout() == true`).
- `context.Canceled` or `context.DeadlineExceeded` wrapped in `ConnectionError`.
- Errors from stale connections (`describer.Stale() == true`).
- Errors with the `ErrSystemOverloadedError` label during handshake.

Pool generation number: clearing the pool increments the generation number.
Connections from previous generations are considered **stale**. Stale errors
(errors from connections where the pool has since been cleared) are silently
ignored by `ProcessError`:

```go
if describer.Stale() {
    return driver.NoChange
}
```

### 3.6 SDAM state transitions on error

When `ProcessError` determines a state change is needed, it calls
`updateDescription` with a new `Unknown` server description, then requests an
immediate check:

```go
s.updateDescription(newServerDescriptionFromError(s.address, err, cerr.TopologyVersion))
s.RequestImmediateCheck()
```

The `Unknown` description is propagated to the topology, which makes the server
unselectable for new operations. The immediate check triggers a reconnect
attempt after `minHeartbeatInterval` (500 ms).

The precise outcomes from `ProcessError`:

| Error category | `ProcessErrorResult` | Pool | Heartbeat |
|---|---|---|---|
| Nil error | `NoChange` | Unchanged | Not triggered |
| Stale connection error | `NoChange` | Unchanged | Not triggered |
| Non-state-change command error (e.g. code 59) | `NoChange` | Unchanged | Not triggered |
| `NotPrimary` / `NodeIsRecovering` with stale topology version | `NoChange` | Unchanged | Not triggered |
| `NotPrimary` / `NodeIsRecovering` with newer topology version, wire ≥ 8 | `ServerMarkedUnknown` | **Not cleared** | Triggered (immediate) |
| `NodeIsShuttingDown` (codes 91, 11600) or wire < 8 | `ConnectionPoolCleared` | **Cleared** | Triggered (immediate) |
| Non-timeout network error | `ConnectionPoolCleared` | **Cleared** | Cancelled + triggered |
| Timeout network error | `NoChange` | Unchanged | Not triggered |
| `context.Canceled` / `context.DeadlineExceeded` | `NoChange` | Unchanged | Not triggered |

### 3.7 Topology version staleness check

Before updating the server description on a `NotPrimary` or `NodeIsRecovering`
error, the driver compares topology versions to prevent stale errors from
causing spurious state changes:

```go
if driverutil.CompareTopologyVersions(topologyVersion, cerr.TopologyVersion) >= 0 {
    return driver.NoChange  // ignore stale error
}
```

`CompareTopologyVersions`:

```go
func CompareTopologyVersions(receiver, response *TopologyVersion) int {
    if receiver == nil || response == nil { return -1 }
    if receiver.ProcessID != response.ProcessID { return -1 }
    if receiver.Counter == response.Counter { return 0 }
    if receiver.Counter < response.Counter { return -1 }
    return 1
}
```

`topologyVersion` in this comparison is the **server's** current topology
version (from `s.desc.Load()`), not the connection's. A connection with a
newer `TopologyVersion` (from a recent heartbeat) overrides the server
description's version (GODRIVER-2841 workaround):

```go
if tv := connDesc.TopologyVersion; tv != nil &&
    driverutil.CompareTopologyVersions(topologyVersion, tv) < 0 {
    topologyVersion = tv
}
```

Result: if a connection's topology version is already at counter `N+1`, an
error from that connection with topology version `N` is treated as stale and
ignored.

### 3.8 Reconnection after a non-timeout network error

The sequence when a non-timeout network error occurs on an application
connection:

1. `conn.Read()` or `conn.Write()` fails → returns `ConnectionError`.
2. `operation.roundTrip()` wraps it: `op.networkError(err)` → `driver.Error`
   with label `["NetworkError"]`.
3. `srvr.ProcessError(err, conn)` is called.
4. `unwrapConnectionError(err)` finds the wrapped `ConnectionError`.
5. `netErr.Timeout() == false` → not a transient timeout.
6. `s.updateDescription(...)` → server becomes `Unknown`.
7. `s.pool.clear(err, serviceID)` → pool generation incremented.
8. `s.heartbeatListener.StopListening()` → cancels in-flight heartbeat, closes
   `s.conn`.
9. Next `update()` loop iteration → `s.conn.closed() == true` → calls
   `setupHeartbeatConnection()` → dials new TCP connection → performs full
   hello handshake.
10. After successful handshake → `s.updateDescription(desc)` → server becomes
    `Standalone`/`RSPrimary`/etc. → pool becomes `ready`.

The application connection pool is not automatically re-populated; connections
are created on demand when `server.Connection(ctx)` is called and the pool is
in the `ready` state.

### 3.9 Single-timeout retry (FAAS pause / GODRIVER-2577)

When the heartbeat connection receives a single timeout error, the driver does
**not** immediately clear the pool. This handles the case where a FaaS
(Function as a Service) environment pauses the process:

```go
// In update() loop:
if err := unwrapConnectionError(desc.LastError); err != nil && timeoutCnt < 1 {
    if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
        timeoutCnt++
        return true  // continue immediately (skip waitUntilNextCheck)
    }
    if err, ok := err.(net.Error); ok && err.Timeout() {
        timeoutCnt++
        return true  // continue immediately
    }
}
// If timeoutCnt >= 1 (second consecutive timeout):
if timeoutCnt > 0 {
    s.pool.clearAll(err, nil)
} else {
    s.pool.clear(err, nil)
}
timeoutCnt = 0
```

The `TestServerHeartbeatTimeout` test confirms:

```go
{
    desc:                "one single timeout should not clear the pool",
    ioErrors:            []error{nil, networkTimeoutError, nil, networkTimeoutError, nil},
    expectInterruptions: 0,   // 0 pool clears
},
{
    desc:                "continuous timeouts should clear the pool with interruption",
    ioErrors:            []error{nil, networkTimeoutError, networkTimeoutError, nil},
    expectInterruptions: 1,   // 1 pool clear
},
```

### 3.10 Load-balanced mode reconnection differences

In load-balanced mode (`loadBalanced == true`):

- **No monitoring goroutine**: `Connect()` does not start `update()`.
- Server kind is immediately set to `ServerKindLoadBalancer`.
- `updateDescription` is a no-op in LB mode:
  ```go
  func (s *Server) updateDescription(desc description.Server) {
      if s.cfg.loadBalanced {
          return  // no-op
      }
      // ...
  }
  ```
- `ProcessHandshakeError` ignores errors when the service ID is unknown (i.e.,
  dial errors and initial handshake errors before the service ID is known).
- Post-handshake errors (auth errors, etc.) **do** clear the pool in LB mode
  but do **not** update the server description.
- Connection pool generation increments happen per `serviceID` in LB mode
  (the `serviceID` returned by the server in the hello response).

### 3.11 Dial error handling (non-LB)

From `TestServerConnectionTimeout`:

| Dial error type | Pool cleared | Server description updated |
|---|---|---|
| Context timeout (connectTimeoutMS exceeded) | **No** (backpressure label applied) | No |
| Invalid address / DNS failure (`net.AddrError`) | **Yes** | Yes → Unknown |
| Any `net.Error` with `Timeout() == true` | **No** | No |

The backpressure exception: `wrapConnectionError` labels DNS and timeout
connection errors with `driver.ErrSystemOverloadedError`:

```go
func wrapConnectionError(connErr ConnectionError) error {
    var dnsErr *net.DNSError
    if errors.As(connErr.Wrapped, &dnsErr) {
        return connErr  // returned as-is, not wrapped as system overload
    }
    // ...
    return driver.Error{
        Labels:  []string{driver.ErrSystemOverloadedError, driver.ErrRetryableError, driver.NetworkError},
        Wrapped: connErr,
    }
}
```

Wait — DNS errors are returned as plain `ConnectionError`, not as
`driver.Error` with overload labels. `ProcessHandshakeError` checks:

```go
var de driver.Error
if errors.As(err, &de) && de.HasErrorLabel(driver.ErrSystemOverloadedError) {
    return  // do not clear pool
}
```

This check only fires for non-DNS errors that were wrapped with overload labels
by `wrapConnectionError`. For DNS errors (returned as plain `ConnectionError`),
the pool is cleared. The test confirms this:

```go
{"dial errors unrelated to context timeouts should clear the pool", ...
    expectPoolCleared: true},
```

---

## 4. Summary Tables

### 4.1 Unknown command

| Aspect | Value |
|---|---|
| Expected `code` in response | 59 (`CommandNotFound`) |
| Is response retried | No |
| Is pool cleared | No |
| Is server description changed | No |
| Error type returned to application | `driver.Error{Code: 59}` |

### 4.2 Malformed BSON — error classification

| Malformation | Error returned | Retried | Pool cleared |
|---|---|---|---|
| `length < 4` in wire message | Plain `error` (connection closed) | No | Via SDAM if non-timeout |
| `length > maxMessageSize` | Plain `error` (connection closed) | No | Via SDAM if non-timeout |
| Incomplete body read | Plain `error` (`ConnectionError`) | No | Via SDAM if non-timeout |
| Insufficient bytes in OP_MSG header | Plain `error` | No | No (not a connection error) |
| `doc.Elements()` parse failure | Plain `error` from `ExtractErrorFromServerResponse` | No | No |
| `ok` field with wrong type | Treated as `ok: 0` → `driver.Error{Code: 0}` | No | No |
| Cursor ID encoded as `int32` | Plain `error` from `NewCursorResponse` | No | No |
| OP_REPLY document length = 0 | Empty documents list; caller gets `ErrNoDocCommandResponse` | No | No |

### 4.3 Reconnection trigger matrix

| Trigger | Action |
|---|---|
| `NotPrimary` error, newer topology version, wire ≥ 8 | Mark server Unknown, request immediate heartbeat |
| `NodeIsShuttingDown` error (codes 91, 11600) | Mark server Unknown, clear pool, request immediate heartbeat |
| Non-timeout network error on app connection | Mark server Unknown, clear pool, cancel & restart heartbeat |
| Single timeout on heartbeat | Retry immediately (no pool clear) |
| Two consecutive timeouts on heartbeat | Clear pool, then retry |
| Successful heartbeat after Unknown state | Mark server with correct kind, pool becomes ready |
