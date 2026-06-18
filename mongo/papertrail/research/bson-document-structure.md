# BSON Document Structure — Complete Implementation Specification

```yaml
provenance:
  - url: "https://bsonspec.org/spec.html"
    fetched: true
    purpose: "Primary BSON wire format grammar (version 1.1)"
  - url: "https://github.com/mongodb/mongo-go-driver/tree/v2.6.1/bson"
    fetched: true
    purpose: "Directory listing of Go driver bson package at tag v2.6.1"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/bson/raw.go"
    fetched: true
    purpose: "Raw document type and validation"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/bson/types.go"
    fetched: true
    purpose: "BSON type byte constants and binary subtype constants"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/bson/value_reader.go"
    fetched: true
    purpose: "Low-level parsing logic for every BSON value type"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/bson/primitive.go"
    fetched: true
    purpose: "Go primitive types for each BSON type"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/bson/bson_corpus_spec_test.go"
    fetched: true
    purpose: "Canonical round-trip test methodology"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/bson/objectid.go"
    fetched: true
    purpose: "ObjectID 12-byte layout and generation"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/bson/writer.go"
    fetched: true
    purpose: "ValueWriter interface — complete list of writable value types"
  - url: "https://github.com/mongodb/mongo-go-driver/tree/v2.6.1/x/bsonx/bsoncore"
    fetched: true
    purpose: "Directory listing of low-level bsoncore package"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/bsonx/bsoncore/bsoncore.go"
    fetched: true
    purpose: "Core read/write/append functions for all BSON types"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/bsonx/bsoncore/document.go"
    fetched: true
    purpose: "Document type, Validate(), LookupErr(), Elements()"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/bsonx/bsoncore/element.go"
    fetched: true
    purpose: "Element type, key/value extraction"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/v2.6.1/x/bsonx/bsoncore/value.go"
    fetched: true
    purpose: "Value type, per-type accessor methods, full size table"
```

---

## 1. Overview

BSON (Binary JSON) version 1.1 is a binary encoding format for ordered key/value maps called
*documents*. All integer and floating-point values are encoded in **little-endian** byte order.
A document is a self-delimiting byte sequence: its first four bytes give the total byte length of
the entire encoding (including those four bytes and the final terminating null byte).

The minimum valid document is 5 bytes:

```
05 00 00 00   00
└── int32 ──┘ └─ terminating null
```

This empty document has `int32` length = 5, zero elements, and ends with `0x00`.

---

## 2. Top-level Grammar (BSON Specification v1.1)

The authoritative grammar, transcribed verbatim from bsonspec.org:

```
document  ::= int32 e_list unsigned_byte(0)

e_list    ::= element e_list
           |  ""                           (empty)

element   ::= <type-byte> e_name <value-encoding>

e_name    ::= cstring

string    ::= int32 (byte*) unsigned_byte(0)
              -- int32 = (number of content bytes) + 1 (for the NUL terminator)

cstring   ::= (byte*) unsigned_byte(0)
              -- MUST NOT contain an interior 0x00 byte

binary    ::= int32 subtype (byte*)
              -- int32 = number of bytes in (byte*) for subtypes != 0x02
              -- For subtype 0x02 see section 9.6

code_w_s  ::= int32 string document
              -- int32 = total byte length of the entire code_w_s value
```

### Constraint: `int32` length field

The `int32` at the start of a `document` counts **every byte** in the document encoding,
including:
- the 4-byte `int32` length field itself
- all element bytes
- the final `unsigned_byte(0)` terminator

Therefore `length >= 5` always. A `length < 5` is invalid. A `length` that exceeds the
available bytes is invalid.

---

## 3. Byte-Level Document Layout

```
Offset  Size   Field
------  -----  -----
0       4      int32   Total document length (little-endian, includes self and terminator)
4       var    element* Zero or more elements (each starts with a 1-byte type tag)
?       1      0x00    Terminating null byte
```

The final byte at `offset = length - 1` MUST be `0x00`. Validation checks both that
`length` equals `len(bytes)` and that `bytes[length-1] == 0x00`
(source: `bsoncore/document.go` `Validate()`, `bsoncore/document.go` `newBufferFromReader()`).

---

## 4. Element Layout

Each element inside `e_list` has the layout:

```
Offset  Size   Field
------  -----  -----
0       1      type byte   (see section 5)
1       var    key         (cstring: zero or more bytes, terminated by 0x00)
?       var    value       (encoding depends on type byte)
```

### 4.1 Key (e_name / cstring)

- Zero or more bytes of UTF-8 text, followed by a single `0x00` terminator.
- The key **MUST NOT** contain an interior `0x00` byte — any such byte would be
  misinterpreted as the terminator (enforced in `bsoncore.go` `AppendHeader()` and
  `AppendKey()` via `isValidCString()`; the implementation panics with the message
  `"BSON element keys cannot contain null bytes"`).
- The key is **not** required to be unique within a document by the wire format itself,
  but consuming code typically treats the first matching key as canonical.

### 4.2 Reading an Element Sequentially

Algorithm (from `bsoncore.go` `ReadElement()`):

1. Read 1 byte → `type`.
2. Scan forward from offset 1 until a `0x00` byte is found → this is the end of the key.
3. The value starts immediately after that `0x00`.
4. Determine value byte length using `valueLength(src, type)` (section 7).
5. The element occupies bytes `[0, 1 + keyLen + 1 + valueLen)`.

---

## 5. Type Byte Constants

All type bytes are 1-octet unsigned values. The Go driver's `bson/types.go` and
`x/bsonx/bsoncore/type.go` define these constants:

| Hex    | Dec  | Name                    | Notes                             |
|--------|------|-------------------------|-----------------------------------|
| `0x01` |  1   | Double                  | 64-bit IEEE 754 float             |
| `0x02` |  2   | String                  | UTF-8                             |
| `0x03` |  3   | Embedded Document       | Recursive document                |
| `0x04` |  4   | Array                   | Document with integer keys        |
| `0x05` |  5   | Binary                  | Opaque byte array + subtype       |
| `0x06` |  6   | Undefined               | **Deprecated**                    |
| `0x07` |  7   | ObjectID                | 12 bytes                          |
| `0x08` |  8   | Boolean                 | 1 byte: 0x00=false, 0x01=true     |
| `0x09` |  9   | DateTime                | int64 UTC milliseconds since epoch|
| `0x0A` | 10   | Null                    | 0 bytes                           |
| `0x0B` | 11   | Regex                   | Two cstrings                      |
| `0x0C` | 12   | DBPointer               | **Deprecated**                    |
| `0x0D` | 13   | JavaScript              | UTF-8 string                      |
| `0x0E` | 14   | Symbol                  | **Deprecated**                    |
| `0x0F` | 15   | Code With Scope         | **Deprecated**                    |
| `0x10` | 16   | Int32                   | 4 bytes                           |
| `0x11` | 17   | Timestamp               | 8 bytes: uint32 increment + uint32 timestamp |
| `0x12` | 18   | Int64                   | 8 bytes                           |
| `0x13` | 19   | Decimal128              | 16 bytes IEEE 754-2008 decimal    |
| `0x7F` | 127  | MaxKey                  | 0 bytes                           |
| `0xFF` | 255  | MinKey (signed: -1)     | 0 bytes                           |

The Go driver represents `MinKey` as `signed_byte(-1)` = `0xFF` and `MaxKey` as
`signed_byte(127)` = `0x7F`, matching the spec.

---

## 6. Value Encodings by Type

All multi-byte integers are **little-endian**.

### 6.1 Double (0x01) — 8 bytes

IEEE 754-2008 64-bit binary floating point, stored as an unsigned 64-bit integer in
little-endian order, then reinterpreted as float64.

```
[f0][f1][f2][f3][f4][f5][f6][f7]
 ↑ LSB                        ↑ MSB
```

Read: `math.Float64frombits(binary.LittleEndian.Uint64(b[0:8]))`.
Write: `binary.LittleEndian.PutUint64(dst, math.Float64bits(f))`.

Special values `+Infinity`, `-Infinity`, and `NaN` are representable as standard
IEEE 754 bit patterns.

### 6.2 String (0x02) — variable

```
Offset  Size   Field
------  -----  -----
0       4      int32   byte count of content + 1 (for the NUL terminator)
4       n      byte*   UTF-8 encoded characters (n = int32 value - 1)
4+n     1      0x00    NUL terminator
```

Total size on wire: `4 + int32_value` bytes.

- `int32_value` must be >= 1 (for at least the NUL terminator); a value of 0 is invalid.
- The NUL terminator at offset `4+n` MUST be present and MUST be `0x00`.
- Content bytes MAY include any valid UTF-8 sequence; validation of UTF-8 correctness is
  driver-dependent (the Go driver validates in strict mode but permits invalid UTF-8 in
  some decode-error test paths).

Source: `bsoncore.go` `appendstring()` / `readstring()`:
```go
func appendstring(dst []byte, s string) []byte {
    l := int32(len(s) + 1)
    dst = appendLength(dst, l)         // write int32 = len+1
    dst = append(dst, s...)            // write content bytes
    return append(dst, 0x00)           // write NUL terminator
}
```

### 6.3 Embedded Document (0x03) — variable

The value is itself a complete, fully-formed BSON document (see section 3). The document's
own 4-byte `int32` length field governs how many bytes it occupies on the wire. The outer
document's length field already accounts for this nested document's bytes.

```
[doc_length: int32][elements...][0x00]
```

Reading: `ReadDocument(src)` calls `readLengthBytes(src)` which reads the inner int32 and
returns `src[0:inner_length]` as the document.

Recursion can be arbitrarily deep; there is no depth limit in the spec.

### 6.4 Array (0x04) — variable

Encoded identically to an Embedded Document. The keys of the elements MUST be the decimal
string representations of their sequential zero-based indices: `"0"`, `"1"`, `"2"`, etc.

```
// Go driver BuildArray() implementation:
idx, dst := ReserveLength(dst)
for pos, val := range values {
    dst = AppendValueElement(dst, strconv.Itoa(pos), val)   // key = "0", "1", ...
}
dst = append(dst, 0x00)
dst = UpdateLength(dst, idx, int32(len(dst[idx:])))
```

Example: The array `['red', 'blue']` encodes as the document `{'0': 'red', '1': 'blue'}`.

The Go driver's `bson.Raw` type (a `[]byte`) and `bsoncore.Array` type (also `[]byte`) are
treated identically at the byte level; only the semantic meaning differs.

### 6.5 Binary (0x05) — variable

```
Offset  Size   Field
------  -----  -----
0       4      int32    byte count of binary data (see subtype 0x02 exception)
4       1      subtype  one of the subtype constants (section 6.5.1)
5       n      byte*    binary data, n = int32 value
```

Total size on wire: `4 + 1 + int32_value` bytes.

#### Subtype 0x02 exception (Binary Old — deprecated)

For subtype `0x02`, the outer `int32` includes an extra 4-byte inner length, so the
actual payload byte count is `outer_int32 - 4`. The layout is:

```
Offset  Size   Field
------  -----  -----
0       4      int32    outer length = (payload byte count) + 4
4       1      0x02     subtype
5       4      int32    inner length = payload byte count
9       n      byte*    binary data
```

Source: `bsoncore.go` `appendBinarySubtype2()` and `ReadBinary()`.

#### 6.5.1 Binary Subtypes

| Hex    | Dec  | Name                     | Notes                              |
|--------|------|--------------------------|------------------------------------|
| `0x00` |  0   | Generic binary           | Default subtype                    |
| `0x01` |  1   | Function                 |                                    |
| `0x02` |  2   | Binary (old)             | **Deprecated**; has inner length   |
| `0x03` |  3   | UUID (old)               | **Deprecated**                     |
| `0x04` |  4   | UUID                     | RFC 4122 UUID, exactly 16 bytes    |
| `0x05` |  5   | MD5                      | 16 bytes                           |
| `0x06` |  6   | Encrypted BSON value     |                                    |
| `0x07` |  7   | Compressed BSON column   |                                    |
| `0x08` |  8   | Sensitive                |                                    |
| `0x09` |  9   | Vector                   | Dense numeric array                |
| `0x80`–`0xFF` | 128–255 | User defined    |                                    |

### 6.6 Undefined (0x06) — 0 bytes

**Deprecated.** No value bytes follow the key. Wire encoding is just the type byte + key cstring.

### 6.7 ObjectID (0x07) — 12 bytes

A 12-byte opaque identifier. The byte layout (from `bson/objectid.go`):

```
Offset  Size   Field
------  -----  -----
0       4      uint32  Unix timestamp seconds (big-endian — NOTE: ObjectID uses big-endian here)
4       5      byte*   Process-unique random bytes (generated once at startup)
9       3      byte*   Incrementing counter, big-endian 3-byte unsigned integer
```

Important: The ObjectID itself uses **big-endian** for its timestamp and counter fields
internally, but in BSON wire format the 12-byte ObjectID is simply written as-is (no
additional byte-swapping by the BSON layer):

```go
func AppendObjectID(dst []byte, oid objectID) []byte { return append(dst, oid[:]...) }
```

### 6.8 Boolean (0x08) — 1 byte

- `0x00` → false
- `0x01` → true
- Any other byte value is invalid (enforced by `value_reader.go`
  `ReadBoolean()`: `if b > 1 { return error }`).

### 6.9 DateTime (0x09) — 8 bytes

A signed 64-bit integer (int64) representing UTC milliseconds since the Unix epoch
(1970-01-01T00:00:00.000Z). Negative values represent dates before the epoch.

```
[ms0][ms1][ms2][ms3][ms4][ms5][ms6][ms7]
 ↑ LSB                                ↑ MSB  (little-endian int64)
```

Conversion to/from `time.Time` (from `bsoncore.go` `AppendTime()` and `ReadTime()`):
```
milliseconds = unix_seconds * 1000 + nanoseconds / 1_000_000
time.Time    = time.Unix(ms / 1000, (ms % 1000) * 1_000_000)
```

### 6.10 Null (0x0A) — 0 bytes

No value bytes. Wire encoding: type byte + key cstring only.

### 6.11 Regex (0x0B) — variable

Two back-to-back cstrings (each zero or more bytes followed by `0x00`):

```
[pattern bytes][0x00][options bytes][0x00]
```

- First cstring: regex pattern. MUST NOT contain `0x00`.
- Second cstring: regex options. MUST NOT contain `0x00`. Options MUST be stored in
  ascending alphabetical order.

Supported option characters (from bsonspec.org):
- `i` — case-insensitive matching
- `m` — multiline matching
- `s` — dotall mode (`.` matches everything including newlines)
- `x` — verbose mode
- `u` — enables `\w`, `\W`, etc. to match Unicode

Source validation: `bsoncore.go` `AppendRegex()` panics if either cstring contains `0x00`.
The Go driver's `sortStringAlphebeticAscending()` in `value.go` sorts options before writing.

Size computation (from `bsoncore.go` `valueLength()`):
```go
case TypeRegex:
    regex := bytes.IndexByte(src, 0x00)     // end of pattern
    pattern := bytes.IndexByte(src[regex+1:], 0x00)  // end of options
    length = int32(regex + 1 + pattern + 1)
```

### 6.12 DBPointer (0x0C) — variable — **Deprecated**

```
Offset  Size   Field
------  -----  -----
0       4+n+1  string   namespace (int32 length + bytes + NUL)
?       12     byte*    ObjectID (12 bytes)
```

Total size: `4 + string_int32_value + 12` bytes.

Size computation: `valueLength()` reads the string's `int32` and adds `4 + 12`.

### 6.13 JavaScript (0x0D) — variable

Encoded identically to a String (section 6.2): `int32` length followed by UTF-8 bytes
followed by `0x00`.

### 6.14 Symbol (0x0E) — variable — **Deprecated**

Encoded identically to a String (section 6.2).

### 6.15 Code With Scope (0x0F) — variable — **Deprecated**

```
Offset  Size   Field
------  -----  -----
0       4      int32     total byte length of the entire code_w_s value (including this int32)
4       4+n+1  string    JavaScript code: int32 + bytes + NUL
?       var    document  scope document (a fully valid BSON document)
```

Total `int32` at offset 0 = `4 + (4 + code_len + 1) + scope_document_length`
                           = `4 + string_wire_size + scope_document_length`.

Source: `bsoncore.go` `AppendCodeWithScope()`:
```go
length := int32(4 + 4 + len(code) + 1 + len(scope))
// 4 = length field itself, 4 = code string length field, len(code) = code bytes,
// 1 = NUL terminator of code string, len(scope) = full scope document bytes
```

Validation in `value_reader.go` `ReadCodeWithScope()` checks:
```
totalLength == 4 + strLength + 4 + scopeDocumentLength
```
(where `strLength` includes the NUL terminator, i.e. `len(codeString) + 1`).

### 6.16 Int32 (0x10) — 4 bytes

Signed 32-bit two's complement integer, little-endian.

```
[b0][b1][b2][b3]
 ↑ LSB        ↑ MSB
```

### 6.17 Timestamp (0x11) — 8 bytes

A special internal type used by MongoDB replication and sharding. **Not for general use.**

The 8 bytes consist of two unsigned 32-bit integers, both little-endian:

```
Offset  Size   Field
------  -----  -----
0       4      uint32   increment (ordinal within a second)
4       4      uint32   seconds since Unix epoch
```

Note: the **increment** is stored in the **lower** 4 bytes and the **seconds** in the
**upper** 4 bytes. This matches the Go driver's `AppendTimestamp()`:
```go
func AppendTimestamp(dst []byte, t, i uint32) []byte {
    return binaryutil.Append32(binaryutil.Append32(dst, i), t)
    // i (increment) first, then t (seconds)
}
```
And `ReadTimestamp()`:
```go
i, rem, ok = binaryutil.ReadU32(src)   // increment first
t, rem, ok = binaryutil.ReadU32(rem)   // seconds second
```

The Go primitive type:
```go
type Timestamp struct {
    T uint32  // seconds
    I uint32  // increment (ordinal)
}
```

Comparison: `(T1, I1) > (T2, I2)` if `T1 > T2`, or `T1 == T2 && I1 > I2`.

### 6.18 Int64 (0x12) — 8 bytes

Signed 64-bit two's complement integer, little-endian.

### 6.19 Decimal128 (0x13) — 16 bytes

128-bit IEEE 754-2008 decimal floating point, stored as two unsigned 64-bit integers
in little-endian order:

```
Offset  Size   Field
------  -----  -----
0       8      uint64   low 64 bits (LSB portion)
8       8      uint64   high 64 bits (MSB portion)
```

Source: `bsoncore.go` `AppendDecimal128()`:
```go
func AppendDecimal128(dst []byte, high, low uint64) []byte {
    return binaryutil.Append64(binaryutil.Append64(dst, low), high)
    // low first, then high
}
```

And `ReadDecimal128()`:
```go
low, rem, ok = binaryutil.ReadU64(src)
high, rem, ok = binaryutil.ReadU64(rem)
```

Reading in `value_reader.go` `ReadDecimal128()`:
```go
l := binary.LittleEndian.Uint64(b[0:8])   // low
h := binary.LittleEndian.Uint64(b[8:16])  // high
return NewDecimal128(h, l), nil
```

### 6.20 MinKey (0xFF) — 0 bytes

No value bytes. Wire encoding: type byte `0xFF` + key cstring only.
Compares lower than all other BSON values.

### 6.21 MaxKey (0x7F) — 0 bytes

No value bytes. Wire encoding: type byte `0x7F` + key cstring only.
Compares higher than all other BSON values.

---

## 7. Value Size Table

This table is used during sequential parsing to know how many bytes to consume after the
key cstring. Source: `bsoncore.go` `valueLength()` and `value_reader.go`
`peekNextValueSize()`.

| Type byte | Size rule                                                                   |
|-----------|-----------------------------------------------------------------------------|
| `0x01`    | 8 (fixed)                                                                   |
| `0x02`    | `ReadInt32(src) + 4` (string: int32 field + content + NUL)                 |
| `0x03`    | `ReadInt32(src)` (document's own int32 counts all its bytes)                |
| `0x04`    | `ReadInt32(src)` (same as document)                                         |
| `0x05`    | `ReadInt32(src) + 4 + 1` (binary: int32 + subtype byte)                    |
| `0x06`    | 0 (undefined, deprecated)                                                   |
| `0x07`    | 12 (fixed)                                                                  |
| `0x08`    | 1 (boolean)                                                                 |
| `0x09`    | 8 (fixed)                                                                   |
| `0x0A`    | 0 (null)                                                                    |
| `0x0B`    | Length of two cstrings including their NUL terminators (scan for two 0x00s) |
| `0x0C`    | `ReadInt32(src) + 4 + 12` (string + OID)                                   |
| `0x0D`    | `ReadInt32(src) + 4` (same as string)                                       |
| `0x0E`    | `ReadInt32(src) + 4` (same as string, deprecated)                           |
| `0x0F`    | `ReadInt32(src)` (code_w_s: int32 counts all its bytes)                     |
| `0x10`    | 4 (fixed)                                                                   |
| `0x11`    | 8 (fixed)                                                                   |
| `0x12`    | 8 (fixed)                                                                   |
| `0x13`    | 16 (fixed)                                                                  |
| `0x7F`    | 0 (MaxKey)                                                                  |
| `0xFF`    | 0 (MinKey)                                                                  |

---

## 8. Embedded Documents and Arrays — Recursive Structure

### 8.1 Embedded Document

An embedded document value (type `0x03`) is a complete, self-contained BSON document.
Its bytes begin at the value position within the parent element and end at
`value_start + embedded_document_int32`. The embedded document includes its own 4-byte
length prefix, its own elements, and its own terminating `0x00`.

```
Parent document:
  [doc_length: int32]
  ...
  [0x03]                           <- type: embedded document
  [key bytes][0x00]                <- e_name cstring
  [sub_doc_length: int32]          <- start of embedded document value
  [sub_elements...]
  [0x00]                           <- end of embedded document value
  ...
  [0x00]                           <- end of parent document
```

The parent's `doc_length` must account for the full byte length of the sub-document,
including the sub-document's own length prefix and terminator.

### 8.2 Array

An array value (type `0x04`) is encoded identically to an embedded document. Its element
keys must be the decimal string representations of consecutive zero-based indices:
`"0"`, `"1"`, `"2"`, etc.

```
Array encoding of ['red', 'blue']:
  0d 00 00 00             <- int32 length = 13
  02                      <- type: string
  30 00                   <- key: "0\x00"
  04 00 00 00             <- string length = 4 (3 chars + NUL)
  72 65 64 00             <- "red\x00"
  02                      <- type: string
  31 00                   <- key: "1\x00"
  05 00 00 00             <- string length = 5 (4 chars + NUL)
  62 6c 75 65 00          <- "blue\x00"
  00                      <- array terminator
```

There is no separate "array type" at the wire level — just the convention of integer keys.
The Go driver's `bson.A` type (`[]any`) encodes this way.

### 8.3 Arbitrary Nesting

Documents and arrays can be nested to arbitrary depth. Each level is
self-delimiting via its own `int32` length prefix, so a parser can skip a nested
structure without recursing into it by reading the `int32` and advancing by that many bytes.

---

## 9. Concrete Byte Examples

### 9.1 Empty Document

```
05 00 00 00 00
```
- Bytes `[00 00 00 05]` LE = `int32(5)`.
- No elements.
- Final byte `0x00`.

### 9.2 Document with a Single Int32 Field `{"x": 1}`

```
0C 00 00 00      <- int32(12): total doc length
10               <- type 0x10 = Int32
78 00            <- key "x" + NUL (0x78 = 'x')
01 00 00 00      <- int32(1) value
00               <- document terminator
```

Total: 12 bytes = 4 (length) + 1 (type) + 2 (key+NUL) + 4 (int32 value) + 1 (terminator).

### 9.3 Document with a String `{"hello": "world"}`

```
16 00 00 00      <- int32(22)
02               <- type 0x02 = String
68 65 6C 6C 6F 00   <- key "hello\x00"
06 00 00 00      <- string int32 = 6 (5 bytes + NUL)
77 6F 72 6C 64 00   <- "world\x00"
00               <- document terminator
```

Total: 22 bytes.

### 9.4 Document with Embedded Document `{"a": {"b": 1}}`

```
12 00 00 00      <- outer int32(18)
03               <- type 0x03 = Embedded Document
61 00            <- key "a\x00"
0C 00 00 00      <- inner int32(12): sub-document length
10               <- type 0x10 = Int32
62 00            <- key "b\x00"
01 00 00 00      <- int32(1)
00               <- sub-document terminator
00               <- outer document terminator
```

Outer length = 4 + 1 + 2 + 12 (entire sub-doc) + 1 = 20?  
Re-count: type(1) + key "a\0"(2) + subdoc(12) = 15 element bytes.
Total = 4 (length) + 15 + 1 (terminator) = 20. So outer int32 = 20.

Corrected:
```
14 00 00 00      <- int32(20)
03               <- type: embedded doc
61 00            <- key "a\x00"
0C 00 00 00      <- sub-doc int32(12)
10               <- type: Int32
62 00            <- key "b\x00"
01 00 00 00      <- value: 1
00               <- sub-doc terminator
00               <- outer terminator
```

### 9.5 Boolean Values

```
// false
08  62 00  00
// type | "b\0" | false
```

```
// true
08  62 00  01
// type | "b\0" | true
```

---

## 10. Validation Rules

A conforming parser MUST enforce all of the following. These are derived from
`bsoncore/document.go` `Validate()` and `newBufferFromReader()`, plus `value_reader.go`.

1. **Length prefix validity**: The 4-byte `int32` at offset 0 must be >= 5. A value < 5
   is invalid (`ErrInvalidLength`).

2. **Length vs buffer**: The declared `int32` length must not exceed the number of bytes
   actually available.

3. **Terminating null**: `bytes[length - 1]` must be `0x00` (`ErrMissingNull`).

4. **Element type validity**: Each element's type byte must be a recognized type. An
   unrecognized type causes parsing to fail.

5. **Key null constraint**: Element keys must not contain interior `0x00` bytes.

6. **Value length validity**: Each value's encoded length must not exceed the remaining
   bytes in the document.

7. **String NUL terminator**: For types `0x02`, `0x0D`, `0x0E`, the final byte of the
   content (at offset `4 + (int32-1)`) must be `0x00`.

8. **String length > 0**: The `int32` in a string, javascript, or symbol value must be
   >= 1 (at minimum there must be the NUL terminator byte).

9. **Boolean byte value**: A boolean value byte must be exactly `0x00` or `0x01`.

10. **CodeWithScope consistency**: The outer `int32` must equal
    `4 + (string_int32 + 4) + scope_document_length`.

11. **Nested documents and arrays**: Each embedded document or array value must itself
    satisfy all the above constraints recursively.

12. **Document end marker position**: After consuming all elements, the next byte
    (at position `length - 1` from document start) must be `0x00`. If position does not
    match declared `length - 1`, the document is invalid. In the Go driver:
    ```go
    if vr.src.pos() != vr.stack[vr.frame].end {
        return "", nil, vr.invalidDocumentLengthError()
    }
    ```

---

## 11. Minimum Document Size Constant

```go
// From bsoncore.go:
const EmptyDocumentLength = 5
```

An empty document `[05 00 00 00 00]` is 5 bytes: 4-byte length + 1-byte terminator.

---

## 12. Reading a Document from a Stream

When reading from an `io.Reader` (source: `bsoncore/document.go` `NewDocumentFromReader()`):

```
1. Read exactly 4 bytes → length_bytes
2. Decode int32 from length_bytes (little-endian)
3. If int32 < 0: error (ErrInvalidLength)
4. Allocate buffer[int32]
5. Copy length_bytes into buffer[0:4]
6. Read exactly (int32 - 4) more bytes into buffer[4:]
7. Verify buffer[int32 - 1] == 0x00 (ErrMissingNull)
8. Return buffer as Document
```

---

## 13. Constants Summary (Go Driver)

```go
// Type bytes (bson/types.go and x/bsonx/bsoncore/type.go)
TypeDouble           = 0x01
TypeString           = 0x02
TypeEmbeddedDocument = 0x03
TypeArray            = 0x04
TypeBinary           = 0x05
TypeUndefined        = 0x06  // deprecated
TypeObjectID         = 0x07
TypeBoolean          = 0x08
TypeDateTime         = 0x09
TypeNull             = 0x0A
TypeRegex            = 0x0B
TypeDBPointer        = 0x0C  // deprecated
TypeJavaScript       = 0x0D
TypeSymbol           = 0x0E  // deprecated
TypeCodeWithScope    = 0x0F  // deprecated
TypeInt32            = 0x10
TypeTimestamp        = 0x11
TypeInt64            = 0x12
TypeDecimal128       = 0x13
TypeMaxKey           = 0x7F
TypeMinKey           = 0xFF

// Binary subtypes (bson/types.go)
TypeBinaryGeneric     = 0x00
TypeBinaryFunction    = 0x01
TypeBinaryBinaryOld   = 0x02  // deprecated; has inner length field
TypeBinaryUUIDOld     = 0x03  // deprecated
TypeBinaryUUID        = 0x04
TypeBinaryMD5         = 0x05
TypeBinaryEncrypted   = 0x06
TypeBinaryColumn      = 0x07
TypeBinarySensitive   = 0x08
TypeBinaryVector      = 0x09
TypeBinaryUserDefined = 0x80  // 0x80–0xFF user defined

// Minimum document length
EmptyDocumentLength = 5
```

---

## 14. Error Sentinel Values (bsoncore/document.go)

```go
ErrMissingNull  ValidationError = "document or array end is missing null byte"
ErrInvalidLength ValidationError = "document or array length is invalid"
ErrNilReader    error            = "nil reader"
ErrEmptyKey     error            = "empty key provided"
ErrElementNotFound error         = "element not found"
ErrOutOfBounds  error            = "out of bounds"
```

`ValidationError` is a `string` type that implements the `error` interface. It is used
to distinguish structural validation failures from other error types.

`InsufficientBytesError` is returned when there are not enough bytes to read the next
component. It carries both the source slice and the remaining slice at the point of failure.

---

## 15. Test Corpus Methodology

The Go driver uses a JSON-described test corpus (`bson-corpus` spec tests) where each
test case specifies:

- `canonical_bson` — hex-encoded canonical wire bytes
- `canonical_extjson` — canonical Extended JSON representation
- `relaxed_extjson` (optional) — relaxed Extended JSON
- `degenerate_bson` (optional) — alternative valid wire bytes that should round-trip to
  canonical form
- `decode_errors` — hex-encoded byte sequences that must fail to decode

Round-trip invariants tested:
1. `native_to_bson(bson_to_native(cB)) == cB`
2. `native_to_canonical_extended_json(bson_to_native(cB)) == cEJ`
3. `native_to_bson(json_to_native(cEJ)) == cB` (unless `lossy: true`)
4. `native_to_bson(bson_to_native(dB)) == cB` (degenerate BSON normalizes to canonical)

These invariants define the behavioral contract for any conforming BSON implementation.
