# MongoDB Wire Protocol Message Structure — Complete Implementation Specification

```yaml
provenance:
  - url: "https://bsonspec.org/spec.html"
    fetched: true
    purpose: "BSON Specification v1.1 — encoding primitives used inside wire messages"
  - url: "https://github.com/mongodb/mongo-go-driver/tree/v2.6.1/x/mongo/driver/wiremessage"
    fetched: true
    purpose: "Directory listing of the wiremessage package at tag v2.6.1"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/wiremessage/wiremessage.go"
    fetched: true
    purpose: "Primary source — all opcode constants, flag constants, section-type constants,
              Append*/Read* functions that define every field's wire position and size"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/wiremessage/wiremessage_test.go"
    fetched: true
    purpose: "Behavioural test suite — byte-exact test vectors for header encoding,
              document-sequence parsing, int32/int64 little-endian encoding"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/drivertest/channel_conn.go"
    fetched: true
    purpose: "MakeReply helper (byte-exact OP_REPLY construction);
              GetCommandFromQueryWireMessage / GetCommandFromMsgWireMessage (parse order)"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/operation.go"
    fetched: true
    purpose: "createWireMessage, createMsgWireMessage, createLegacyHandshakeWireMessage,
              decompressWireMessage, roundTrip, moreToComeRoundTrip — full message lifecycle"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/operation_test.go"
    fetched: true
    purpose: "createExhaustServerResponse / assertExhaustAllowedSet (OP_MSG flag test vectors);
              TestDecodeOpReply (malformed length guard)"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/legacy.go"
    fetched: true
    purpose: "LegacyOperationKind constants — defines which operations still use OP_QUERY"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/compression.go"
    fetched: true
    purpose: "CompressPayload / DecompressPayload — exact compressor semantics for OP_COMPRESSED"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/compression_test.go"
    fetched: true
    purpose: "Compression round-trip tests, level range validation"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/mongo/driver/operation_exhaust.go"
    fetched: true
    purpose: "ExecuteExhaust — streaming / moreToCome read path"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/internal/binaryutil/binaryutil.go"
    fetched: true
    purpose: "Append32, Append64, ReadI32, ReadI64, ReadU32, ReadU64, ReadCString —
              confirms little-endian byte order for all multi-byte fields"
```

---

## 1. Overview

All communication between a MongoDB client and server uses a binary framing called the **wire protocol**. Every exchange consists of one or more **wire messages**. Each wire message has a fixed-size header followed by an opcode-specific body.

This document targets **wire protocol version 6** (the version introduced with MongoDB 3.6) and covers all opcodes present in the Go driver at tag `v2.6.1`.

### 1.1 Byte Order

**All multi-byte integer fields are little-endian.** This is confirmed by the `binaryutil` package:

```go
// Append32 (little-endian int32/uint32)
func Append32[T ~uint32 | ~int32](dst []byte, v T) []byte {
    return append(dst, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
}

// ReadI32 (little-endian int32)
value := int32(src[0]) | int32(src[1])<<8 | int32(src[2])<<16 | int32(src[3])<<24
```

The test vectors confirm: `int32(1)` encodes as `[0x01, 0x00, 0x00, 0x00]`; `int32(-1)` as `[0xFF, 0xFF, 0xFF, 0xFF]`; `int32(math.MaxInt32)` as `[0xFF, 0xFF, 0xFF, 0x7F]`.

### 1.2 Maximum Wire Message Size

The driver enforces a maximum of **16 MiB** (16,777,216 bytes) per wire message, as evidenced by the pool recycle guard in `operation.go`:

```go
// Recycle byte slices that are smaller than 16MiB and at least half occupied.
if c := cap(*wm); c < 16*1024*1024 && c/2 < len(*wm) {
```

### 1.3 Transport

Wire messages are sent over a TCP stream (or TLS). There is no framing layer below the wire message itself — the `messageLength` field in the header is the sole length delimiter.

---

## 2. MsgHeader — The Common Header

Every wire message begins with a fixed 16-byte header:

```
Offset  Size  Type    Field
------  ----  ------  -----------------------------------------
0       4     int32   messageLength   — total message length in bytes, including this header
4       4     int32   requestID       — client-assigned ID for this message (any int32)
8       4     int32   responseTo      — requestID of the message this is a reply to (0 for requests)
12      4     int32   opCode          — identifies the message type (see §3)
```

Total header size: **16 bytes**.

All fields are **little-endian signed 32-bit integers**. The `opCode` field is read as a signed `int32` and then cast to the `OpCode` type.

### 2.1 Field Semantics

**`messageLength`** — the total byte count of the entire wire message (header + body). Must be at least 16 (empty body). Implementations should reject messages whose `messageLength` exceeds the maximum negotiated size.

**`requestID`** — a monotonically increasing integer assigned by the sender. The driver uses an atomic counter:
```go
var globalRequestID int32
func NextRequestID() int32 { return atomic.AddInt32(&globalRequestID, 1) }
```
There is no specified starting value; zero is valid. The counter wraps at `math.MaxInt32`.

**`responseTo`** — for reply messages (OP_REPLY, OP_MSG responses), this field echoes the `requestID` of the message being responded to. For request messages sent by the client, this field is `0`.

**`opCode`** — a 32-bit integer identifying the message type. See §3 for all defined values.

### 2.2 Header Encoding (from source)

```go
// AppendHeaderStart reserves space for messageLength (filled in later),
// then writes requestID, responseTo, opCode.
func AppendHeaderStart(dst []byte, reqid, respto int32, opcode OpCode) (index int32, b []byte) {
    index, dst = bsoncore.ReserveLength(dst)   // writes 4 zero bytes, returns start index
    dst = binaryutil.Append32(dst, reqid)
    dst = binaryutil.Append32(dst, respto)
    dst = binaryutil.Append32(dst, opcode)     // OpCode is int32 underneath
    return index, dst
}
```

The `messageLength` is initially zeroed. After the body is fully written, it is back-filled:
```go
bsoncore.UpdateLength(dst, idx, int32(len(dst[idx:])))
```

### 2.3 Header Parsing (from source)

```go
func ReadHeader(src []byte) (length, requestID, responseTo int32, opcode OpCode, rem []byte, ok bool) {
    if len(src) < 16 {
        return 0, 0, 0, 0, src, false
    }
    length, _, _   = binaryutil.ReadI32(src)
    requestID, _, _ = binaryutil.ReadI32(src[4:])
    responseTo, _, _ = binaryutil.ReadI32(src[8:])
    opcodeVal, _, _ := binaryutil.ReadI32(src[12:])
    opcode = OpCode(opcodeVal)
    return length, requestID, responseTo, opcode, src[16:], true
}
```

**Minimum valid input**: 16 bytes. Fewer bytes → returns `ok=false`, original slice unchanged.

### 2.4 Byte-Level Test Vector

From `wiremessage_test.go`, `AppendHeaderStart` with `reqid=2, respto=1, opcode=OpMsg (2013)`:

```
Offset  Hex          Decimal  Field
------  -----------  -------  ----------------
0–3     00 00 00 00  0        messageLength (placeholder, filled later)
4–7     02 00 00 00  2        requestID
8–11    01 00 00 00  1        responseTo
12–15   DD 07 00 00  2013     opCode (OpMsg)
```

With `opcode=OpQuery (2004)`:
```
12–15   D4 07 00 00  2004     opCode (OpQuery)
```

---

## 3. OpCode Registry

All opcodes in the driver as of v2.6.1:

```go
const (
    OpReply        OpCode = 1     // server → client (legacy)
    _              OpCode = 1001  // (reserved/skipped)
    OpUpdate       OpCode = 2001  // client → server (deprecated, not actively used)
    OpInsert       OpCode = 2002  // client → server (deprecated, not actively used)
    _              OpCode = 2003  // (reserved/skipped — was OP_RESERVED)
    OpQuery        OpCode = 2004  // client → server (legacy, see §6)
    OpGetMore      OpCode = 2005  // client → server (legacy cursor fetch)
    OpDelete       OpCode = 2006  // client → server (deprecated, not actively used)
    OpKillCursors  OpCode = 2007  // client → server (legacy cursor cleanup)
    OpCommand      OpCode = 2010  // (internal / Atlas Proxy, not used by standard driver)
    OpCommandReply OpCode = 2011  // (internal / Atlas Proxy, not used by standard driver)
    OpCompressed   OpCode = 2012  // bidirectional (wraps another opcode with compression)
    OpMsg          OpCode = 2013  // bidirectional (primary modern opcode, MongoDB 3.6+)
)
```

The driver marks `OpQuery` as deprecated with the comment "Use OpMsg instead."

**Active opcodes in a MongoDB 3.6+ deployment:**
- `OpMsg` (2013) — all commands and responses
- `OpCompressed` (2012) — optionally wrapping any other opcode
- `OpReply` (1) — server replies during legacy handshake
- `OpQuery` (2004) — only for the initial `isMaster`/`hello` handshake when wire version is unknown (see §6)

---

## 4. OP_MSG (opCode = 2013) — Primary Modern Message

OP_MSG is the sole message type used for all commands and their responses in MongoDB 3.6+. It supports both a single-document body and multi-document sequences (for bulk operations).

### 4.1 Full Wire Layout

```
[MsgHeader: 16 bytes]          — see §2
[flagBits:   4 bytes, uint32]  — OP_MSG-specific flags
[sections:   variable]         — one or more sections (see §4.3)
[checksum:   0 or 4 bytes]     — present only if ChecksumPresent flag is set
```

### 4.2 flagBits Field

`flagBits` is a **uint32** (4 bytes, little-endian) immediately following the header. It is read as an unsigned integer internally stored as `MsgFlag`:

```go
type MsgFlag uint32

const (
    ChecksumPresent MsgFlag = 1 << 0   // bit 0 — checksum follows the last section
    MoreToCome      MsgFlag = 1 << 1   // bit 1 — more messages will follow without a reply

    ExhaustAllowed  MsgFlag = 1 << 16  // bit 16 — client allows server to send extra messages
)
```

**`ChecksumPresent` (bit 0)**: When set, a 4-byte CRC-32C checksum is appended after the last section. See §4.5.

**`MoreToCome` (bit 1)**: When set on a **client request**, the client will not wait for a response and will send additional messages. When set on a **server response**, the server will push additional responses without waiting for another client request (streaming protocol). A server response with `MoreToCome` set must not be replied to — the driver sets the connection to a streaming state:
```go
if streamer := conn.Streamer; streamer != nil {
    streamer.SetStreaming(wiremessage.IsMsgMoreToCome(wm))
}
```

**`ExhaustAllowed` (bit 16)**: Set by the **client** to signal that the connection supports streaming. The server may then respond with `MoreToCome` set. Only written when `conn.Streamer != nil && streamer.SupportsStreaming()`:
```go
if streamer := conn.Streamer; streamer != nil && streamer.SupportsStreaming() {
    flags = wiremessage.ExhaustAllowed
}
dst = wiremessage.AppendMsgFlags(dst, flags)
```

**All other bits are reserved and MUST be zero.**

Flag encoding: `AppendMsgFlags` calls `binaryutil.Append32(dst, flags)` — same little-endian int32/uint32 serialisation.

### 4.3 Sections

After `flagBits`, one or more sections follow sequentially until the checksum (if present) or the end of the message. Each section begins with a 1-byte **sectionType** tag:

```go
type SectionType uint8

const (
    SingleDocument   SectionType = 0   // Kind 0
    DocumentSequence SectionType = 1   // Kind 1
)
```

#### 4.3.1 Kind 0 — Single Document (Body Section)

```
[sectionType: 1 byte = 0x00]
[document:    variable BSON document]
```

The body section contains exactly one BSON document. In a request, this document contains the command name as its first key and all command parameters. It also carries protocol-level fields such as `$db`, `$readPreference`, `lsid` (logical session ID), `$clusterTime`, and `txnNumber`.

There is exactly **one** body section (Kind 0) per OP_MSG message.

From `createMsgWireMessage`:
```go
dst = wiremessage.AppendMsgSectionType(dst, wiremessage.SingleDocument)
idx, dst := bsoncore.AppendDocumentStart(dst)
// ... command fields, $db, $readPreference ...
dst, _ = bsoncore.AppendDocumentEnd(dst, idx)
```

The BSON document is written directly; its own `int32` length prefix (as per BSON spec) accounts for its total size.

#### 4.3.2 Kind 1 — Document Sequence

```
[sectionType: 1 byte = 0x01]
[size:        4 bytes, int32]  — total byte count of this section including these 4 bytes
[identifier:  cstring]         — null-terminated UTF-8 string naming the sequence
[documents:   repeated BSON documents until size is exhausted]
```

The `size` field counts from itself (the first byte of `size`) through the last byte of the last document. Minimum valid `size` is 4 (a section with no documents, just the length itself — though in practice there is always at least the identifier cstring).

**Parse logic from source:**
```go
func ReadMsgSectionRawDocumentSequence(src []byte) (identifier string, data []byte, rem []byte, ok bool) {
    length, rem, ok := binaryutil.ReadI32(src)
    if !ok || int(length) > len(src) || length < 4 {
        return "", nil, src, false
    }
    // rem[:length-4] is identifier + document bytes;
    // rem[length-4:] is remaining message data after this section.
    rem, rest := rem[:length-4], rem[length-4:]
    identifier, rem, ok = binaryutil.ReadCString(rem)
    ...
    return identifier, rem, rest, true
}
```

The document sequence section appears **zero or more times** per message. It is used for bulk write operations where individual write documents (e.g., the `documents` array for `insert`) are sent as a sequence rather than embedded in the command document. The `identifier` string names the field in the command document that the sequence corresponds to (e.g., `"documents"`, `"updates"`, `"deletes"`).

**Test vector from wiremessage_test.go** (valid document sequence):
```
Bytes: [11 00 00 00] [69 64 00] [05 00 00 00 00] [05 00 00 00 00]
         size=17        "id"\0    empty BSON doc    empty BSON doc
```
- `size = 17` means this section spans bytes 0–16 (17 bytes total from start of `size` field).
- identifier = `"id"` (2 bytes + 1 null = 3 bytes).
- Two empty BSON documents (5 bytes each: `\x05\x00\x00\x00\x00`).
- `17 - 4 = 13` bytes consumed for identifier + docs after reading `size`.

### 4.4 Command Document Fields (Kind 0 Body)

The command document inside the Kind 0 section has the following structure for client requests. Only fields relevant to wire framing are listed here; application-level fields vary per command.

| Field              | Type           | Notes |
|--------------------|----------------|-------|
| `<commandName>`    | first key      | Identifies the operation, e.g. `"find"`, `"insert"`, `"ping"` |
| `$db`              | UTF-8 string   | Database name, always present in OP_MSG |
| `$readPreference`  | BSON document  | Present only when needed (non-primary, non-trivial topology) |
| `lsid`             | BSON document  | Logical session ID, when sessions are in use |
| `$clusterTime`     | BSON document  | Gossiped cluster time |
| `txnNumber`        | int64          | Transaction number (retryable writes / multi-document transactions) |
| `maxTimeMS`        | int64          | Server-side operation timeout (omitted when 0) |
| `readConcern`      | BSON document  | Read concern level |
| `writeConcern`     | BSON document  | Write concern (w, j, wtimeout) |

### 4.5 Checksum

When `flagBits & ChecksumPresent != 0`, the last 4 bytes of the message are a **CRC-32C checksum**:

```go
func ReadMsgChecksum(src []byte) (checksum uint32, rem []byte, ok bool) {
    i32, rem, ok := binaryutil.ReadI32(src)
    return uint32(i32), rem, ok
}
```

The checksum is read as a `uint32` (via casting from `int32`). The algorithm is CRC-32C (Castagnoli). The checksum covers all bytes of the message from the start of `messageLength` through the last byte of the last section (excluding the checksum itself).

**Note:** The driver does not appear to write checksums in outgoing messages (no `AppendMsgChecksum` function exists; `ChecksumPresent` is never set in `createMsgWireMessage`). It only reads them. Checksums are therefore a receiver-side concern.

### 4.6 MoreToCome Detection

The `IsMsgMoreToCome` helper:
```go
func IsMsgMoreToCome(wm []byte) bool {
    if len(wm) < 20 {
        return false
    }
    opcode, _, _ := binaryutil.ReadI32(wm[12:16])
    flag, _, _ := binaryutil.ReadI32(wm[16:20])
    return OpCode(opcode) == OpMsg && MsgFlag(flag)&MoreToCome == MoreToCome
}
```

This reads the opcode from bytes 12–15 and the flags from bytes 16–19. It requires at least 20 bytes (16-byte header + 4-byte flags). If true, the receiving connection enters streaming mode.

### 4.7 Unacknowledged Writes (moreToCome on Requests)

When a client sends a write with `writeConcern: {w: 0}` (unacknowledged), the `MoreToCome` flag is set on the **outgoing** message. The driver then returns `ErrUnacknowledgedWrite` immediately without reading a server response:
```go
if moreToCome {
    return ErrUnacknowledgedWrite
}
```

### 4.8 Minimum Valid OP_MSG

The smallest valid OP_MSG has:
- 16-byte header
- 4-byte flagBits (= 0x00000000)
- 1-byte sectionType (= 0x00)
- 5-byte minimal BSON document (`\x05\x00\x00\x00\x00`)

Total: **26 bytes**.

---

## 5. OP_COMPRESSED (opCode = 2012)

OP_COMPRESSED wraps any other wire message (after its header) with compression. The original message's header is **not** included in the compressed payload — only the body bytes following the header.

### 5.1 Wire Layout

```
[MsgHeader: 16 bytes]             — opCode = 2012
[originalOpcode: 4 bytes, int32]  — opCode of the wrapped message
[uncompressedSize: 4 bytes, int32]— byte count of the uncompressed body
[compressorID: 1 byte, uint8]     — identifies the compression algorithm
[compressedMessage: variable]     — the compressed body bytes
```

Total header + fixed fields: 16 + 4 + 4 + 1 = **25 bytes** before compressed data.

### 5.2 Field Definitions

**`originalOpcode`** (int32): The opCode that the wrapped message would have used without compression. Typically `OpMsg` (2013) or `OpQuery` (2004).

**`uncompressedSize`** (int32): The byte count of the uncompressed body. Used to pre-allocate the decompression buffer. For snappy, the driver cross-checks the decoded length:
```go
l, err := snappy.DecodedLen(in)
if int32(l) != opts.UncompressedSize {
    return nil, fmt.Errorf("unexpected decompression size, expected %v but got %v", ...)
}
```

**`compressorID`** (uint8):
```go
const (
    CompressorNoOp   CompressorID = 0  // no compression (identity)
    CompressorSnappy CompressorID = 1  // Google Snappy
    CompressorZLib   CompressorID = 2  // RFC 1950 zlib / DEFLATE
    CompressorZstd   CompressorID = 3  // Facebook Zstandard
)
```

**`compressedMessage`**: The compressed body bytes. These decompress to the body of the wrapped message (everything after the 16-byte header of the original message).

### 5.3 Decompression Flow (from source)

```go
func (Operation) decompressWireMessage(wm []byte) (wiremessage.OpCode, []byte, error) {
    // wm here is the raw size after the header (header already stripped)
    opcode, rem, ok := wiremessage.ReadCompressedOriginalOpCode(wm)
    uncompressedSize, rem, ok := wiremessage.ReadCompressedUncompressedSize(rem)
    compressorID, rem, ok := wiremessage.ReadCompressedCompressorID(rem)
    opts := CompressionOpts{
        Compressor:       compressorID,
        UncompressedSize: uncompressedSize,
    }
    uncompressed, err := DecompressPayload(rem, opts)
    return opcode, uncompressed, err
}
```

Called from the read path:
```go
if opcode == wiremessage.OpCompressed {
    rawsize := length - 16 // remove header size
    opcode, rem, err = op.decompressWireMessage(rem[:rawsize])
}
```

### 5.4 Compression Algorithms

All algorithms operate on the **body bytes** (post-header) of the wrapped message.

| ID | Name     | Library (Go)                         | Default Level | Notes |
|----|----------|--------------------------------------|---------------|-------|
| 0  | NoOp     | none (passthrough)                   | —             | Identity compressor; payload returned as-is |
| 1  | Snappy   | `github.com/klauspost/compress/snappy` | —           | Framing format: raw Snappy block |
| 2  | ZLib     | `compress/zlib` (stdlib)             | 6             | RFC 1950 zlib wrapper around DEFLATE; level range `HuffmanOnly(-2)` to `BestCompression(9)` |
| 3  | Zstd     | `github.com/klauspost/compress/zstd` | 6             | Window size 8 MiB; level range `SpeedFastest(1)` to `SpeedBestCompression(4)` |

Default levels:
```go
const DefaultZlibLevel = 6
const DefaultZstdLevel = 6
```

### 5.5 Compression Negotiation

Compression is negotiated during the connection handshake (the `hello`/`isMaster` command includes a `compression` array listing client-supported compressors). The server responds with its supported list. The driver stores the negotiated compressor on the connection:
```go
if compressor := conn.Compressor; compressor != nil && op.canCompress(startedInfo.cmdName) {
    *b, err = compressor.CompressWireMessage(*wm, (*b)[:0])
}
```

Certain commands cannot be compressed (e.g., `isMaster`, `hello`, `authenticate`, `saslStart`, `saslContinue`, `getnonce`, `createUser`, `updateUser`, `copydbgetnonce`, `copydbsaslstart`, `copydb`) — the `canCompress` method implements this exclusion list.

---

## 6. OP_QUERY (opCode = 2004) — Legacy Request

OP_QUERY is deprecated. In a MongoDB 3.6+ driver it is only used for the **initial connection handshake** when the wire version of the server is not yet known (`WireVersion == nil || WireVersion.Max == 0`), specifically for the `isMaster`/`hello` command via `LegacyHandshake`.

### 6.1 Wire Layout

```
[MsgHeader: 16 bytes]               — opCode = 2004
[flags: 4 bytes, int32]             — QueryFlag bitmask
[fullCollectionName: cstring]        — "database.$cmd" — null-terminated
[numberToSkip: 4 bytes, int32]      — number of documents to skip (0 for commands)
[numberToReturn: 4 bytes, int32]    — max documents to return (-1 = unlimited)
[query: BSON document]              — the command document
[returnFieldsSelector: BSON doc]    — optional projection document (may be absent)
```

### 6.2 QueryFlag Bitmask

```go
type QueryFlag int32

const (
    _ QueryFlag = 1 << iota         // bit 0: Reserved (must be 0)
    TailableCursor                   // bit 1: 0x02
    SecondaryOK                      // bit 2: 0x04 — allow query against replica set secondary
    OplogReplay                      // bit 3: 0x08 — internal replication use
    NoCursorTimeout                  // bit 4: 0x10 — prevent cursor timeout
    AwaitData                        // bit 5: 0x20 — used with TailableCursor
    Exhaust                          // bit 6: 0x40 — stream all data
    Partial                          // bit 7: 0x80 — partial results on shard failure
)
```

For legacy handshake commands, only `SecondaryOK` (0x04) may be set (when targeting a replica set secondary or a single-node topology):
```go
func (op Operation) secondaryOK(desc description.SelectedServer) QueryFlag {
    // sets SecondaryOK if topology is Single+Secondary or if ReadPreference is Secondary
}
```

### 6.3 Field Encoding

```go
// from createLegacyHandshakeWireMessage:
dst = wiremessage.AppendQueryFlags(dst, flags)              // int32, LE
dst = append(dst, op.Database...)                           // bytes of db name
dst = append(dst, dollarCmd[:]...)                          // ".$cmd" literally
dst = append(dst, 0x00)                                     // null terminator → fullCollectionName
dst = wiremessage.AppendQueryNumberToSkip(dst, 0)           // int32, LE = 0
dst = wiremessage.AppendQueryNumberToReturn(dst, -1)        // int32, LE = -1 (unlimited)
// then appends the BSON command document
```

The `fullCollectionName` is always `"<dbname>.$cmd"` for command use. Both `numberToSkip` (0) and `numberToReturn` (-1) are fixed for command execution.

---

## 7. OP_REPLY (opCode = 1) — Legacy Response

OP_REPLY is sent by the server in response to OP_QUERY. It is also what the driver constructs in test helpers when simulating server responses to legacy clients.

### 7.1 Wire Layout

```
[MsgHeader: 16 bytes]              — opCode = 1; responseTo = client requestID
[responseFlags: 4 bytes, int32]    — ReplyFlag bitmask
[cursorID: 8 bytes, int64]         — cursor ID (0 if exhausted or not a cursor)
[startingFrom: 4 bytes, int32]     — index in cursor of first document
[numberReturned: 4 bytes, int32]   — number of documents in this reply
[documents: variable]              — numberReturned BSON documents, concatenated
```

Fixed fields after header: 4 + 8 + 4 + 4 = **20 bytes**.

### 7.2 ReplyFlag Bitmask

```go
type ReplyFlag int32

const (
    CursorNotFound   ReplyFlag = 1 << iota   // bit 0: cursor not found (stale/timed out)
    QueryFailure                              // bit 1: query failed (error in first document)
    ShardConfigStale                          // bit 2: (internal)
    AwaitCapable                              // bit 3: server supports AwaitData
)
```

When `QueryFailure` is set, the first document in `documents` is an error document.

### 7.3 Document Parsing

```go
func ReadReplyDocuments(src []byte) (docs []bsoncore.Document, rem []byte, ok bool) {
    rem = src
    for {
        var doc bsoncore.Document
        doc, rem, ok = bsoncore.ReadDocument(rem)
        if !ok { break }
        docs = append(docs, doc)
    }
    return docs, rem, true
}
```

Documents are read sequentially until the buffer is exhausted or a document read fails. The driver validates that the actual count matches `numberReturned` — a mismatch produces `ErrReplyDocumentMismatch`.

**Guard against infinite loop** (from `TestDecodeOpReply`): A document with `length = 0` is explicitly handled — `bsoncore.ReadDocument` will return `ok=false` when the length field is 0, preventing infinite loops.

### 7.4 Test Construction (from drivertest)

```go
func MakeReply(doc bsoncore.Document) []byte {
    var dst []byte
    idx, dst := wiremessage.AppendHeaderStart(dst, 10, 9, wiremessage.OpReply)
    dst = wiremessage.AppendReplyFlags(dst, 0)         // 4 bytes: flags = 0
    dst = wiremessage.AppendReplyCursorID(dst, 0)      // 8 bytes: cursorID = 0
    dst = wiremessage.AppendReplyStartingFrom(dst, 0)  // 4 bytes: startingFrom = 0
    dst = wiremessage.AppendReplyNumberReturned(dst, 1)// 4 bytes: numberReturned = 1
    dst = append(dst, doc...)                          // BSON document bytes
    return bsoncore.UpdateLength(dst, idx, int32(len(dst[idx:])))
}
```

---

## 8. Other Opcodes (Reference)

These opcodes are defined in the driver's `OpCode` constants but are not generated by the current driver for normal use. They represent the historical evolution of the wire protocol.

### 8.1 OP_UPDATE (2001)

Client-to-server document update. **Deprecated** — replaced by the `update` command over OP_MSG.

```
[MsgHeader]
[ZERO: 4 bytes]             — reserved, must be 0
[fullCollectionName: cstring]
[flags: 4 bytes, int32]
[selector: BSON document]   — query selector
[update: BSON document]     — update document
```

### 8.2 OP_INSERT (2002)

Client-to-server document insert. **Deprecated** — replaced by `insert` command.

```
[MsgHeader]
[flags: 4 bytes, int32]
[fullCollectionName: cstring]
[documents: one or more BSON documents]
```

### 8.3 OP_GET_MORE (2005)

Fetches more documents from an open cursor. **Deprecated**:

```
[MsgHeader]
[ZERO: 4 bytes]
[fullCollectionName: cstring]
[numberToReturn: 4 bytes, int32]
[cursorID: 8 bytes, int64]
```

From the driver's append functions:
```go
func AppendGetMoreZero(dst []byte) []byte          { return binaryutil.Append32(dst, 0) }
func AppendGetMoreFullCollectionName(...)
func AppendGetMoreNumberToReturn(dst []byte, n int32) []byte { return binaryutil.Append32(dst, n) }
func AppendGetMoreCursorID(dst []byte, id int64) []byte     { return binaryutil.Append64(dst, id) }
```

### 8.4 OP_DELETE (2006)

Client-to-server document delete. **Deprecated** — replaced by `delete` command.

```
[MsgHeader]
[ZERO: 4 bytes]
[fullCollectionName: cstring]
[flags: 4 bytes, int32]
[selector: BSON document]
```

### 8.5 OP_KILL_CURSORS (2007)

Closes open server-side cursors. **Deprecated** — replaced by `killCursors` command.

```
[MsgHeader]
[ZERO: 4 bytes]
[numberOfCursorIDs: 4 bytes, int32]
[cursorIDs: 8 * numberOfCursorIDs bytes, each int64]
```

From the driver:
```go
func AppendKillCursorsZero(dst []byte) []byte                { return binaryutil.Append32(dst, 0) }
func AppendKillCursorsNumberIDs(dst []byte, n int32) []byte  { return binaryutil.Append32(dst, n) }
func AppendKillCursorsCursorIDs(dst []byte, ids []int64) []byte {
    for _, id := range ids { dst = binaryutil.Append64(dst, id) }
    return dst
}
```

Reading cursor IDs:
```go
func ReadKillCursorsCursorIDs(src []byte, numIDs int32) (cursorIDs []int64, rem []byte, ok bool) {
    for i := int32(0); i < numIDs; i++ {
        id, src, ok = binaryutil.ReadI64(src)
        ...
    }
}
```

---

## 9. Message Lifecycle

### 9.1 Client Request Flow

```
1. Allocate buffer from pool (initially 1KB, max 16MB)
2. AppendHeaderStart(buf, requestID, 0, opCode)  → save index
3. Write opcode-specific body
4. bsoncore.UpdateLength(buf, index, totalLen)   → back-fill messageLength
5. If compression negotiated and command is compressible:
   a. Compress body bytes (everything after header)
   b. Construct new OP_COMPRESSED message wrapping original opCode
6. conn.Write(ctx, wireMessage)
7. If !moreToCome: conn.Read(ctx) → response wire message
```

### 9.2 Server Response Read Flow

```
1. conn.Read(ctx) → raw []byte
2. ReadHeader → length, requestID, responseTo, opCode, rem
3. Validate: len(raw) >= length
4. If opCode == OpCompressed:
   a. Strip 16-byte header
   b. decompressWireMessage(rem[:length-16])  → opCode, body
5. Dispatch on opCode:
   - OpMsg   → decodeOpMsg
   - OpReply → decodeOpReply
6. Update cluster time, operation time, recovery tokens
7. If conn.Streamer: SetStreaming(IsMsgMoreToCome(raw))
```

### 9.3 Wire Message Framing for Reading

The `messageLength` field governs framing. The TCP stream contains back-to-back wire messages. To read one message:
1. Read exactly 4 bytes → parse `messageLength` (little-endian int32).
2. Read `messageLength - 4` additional bytes.
3. The full message is the concatenation.

The driver's `conn.Read` handles this at the transport layer.

---

## 10. Streaming (moreToCome) Protocol

When the server sets `MoreToCome` on a response, it will continue sending OP_MSG messages without the client sending new requests. This is used for change streams and monitoring commands.

```
Client → Server: OP_MSG with ExhaustAllowed set
Server → Client: OP_MSG with MoreToCome=1, ExhaustAllowed=0  (response 1)
Server → Client: OP_MSG with MoreToCome=1, ExhaustAllowed=0  (response 2)
...
Server → Client: OP_MSG with MoreToCome=0, ExhaustAllowed=0  (final response)
```

The client reads subsequent responses via `ExecuteExhaust`:
```go
func (op Operation) ExecuteExhaust(ctx context.Context, conn *mnet.Connection) error {
    if !conn.CurrentlyStreaming() {
        return errors.New("exhaust read must be done with a connection that is currently streaming")
    }
    res, err := op.readWireMessage(ctx, conn)
    ...
}
```

A connection in streaming mode must not be returned to the pool until streaming ends.

---

## 11. Size Constraints

| Limit | Value | Source |
|-------|-------|--------|
| Maximum wire message | 16 MiB (16,777,216 bytes) | Pool recycle guard in `operation.go` |
| Minimum wire message | 16 bytes (header only) | `ReadHeader` requires 16 bytes |
| Minimum valid OP_MSG | 26 bytes | 16 header + 4 flags + 1 stype + 5 BSON |
| Minimum valid OP_REPLY | 36 bytes | 16 header + 4 flags + 8 cursorID + 4 startingFrom + 4 numReturned |
| Maximum BSON document (encryption) | 2 MiB (2,097,152 bytes) | `cryptMaxBsonObjectSize` constant |
| MsgHeader size | 16 bytes | Fixed: 4×int32 |
| OP_COMPRESSED fixed overhead | 9 bytes | 4 (originalOpCode) + 4 (uncompressedSize) + 1 (compressorID) |
| Document sequence `size` minimum | 4 | `length < 4` treated as error |

---

## 12. Numeric Encoding Quick Reference

| Type | Size | Encoding | Notes |
|------|------|----------|-------|
| int32 | 4 bytes | Little-endian signed | Used for: messageLength, requestID, responseTo, opCode, flags, numberToSkip, numberToReturn, startingFrom, numberReturned, doc-sequence size |
| uint32 | 4 bytes | Little-endian unsigned | Used for: MsgFlag, checksum |
| int64 | 8 bytes | Little-endian signed | Used for: cursorID |
| uint8 | 1 byte | — | Used for: sectionType, compressorID |
| cstring | variable | UTF-8 bytes + `0x00` | Used for: fullCollectionName, doc-sequence identifier |
| BSON document | variable | As per BSON spec | int32 length + e_list + `0x00` |
