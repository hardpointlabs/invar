# BSON Type System — Complete Implementation Specification

---

## Provenance

```yaml
sources_fetched_in_order:
  - url: "https://bsonspec.org/spec.html"
    description: "BSON Specification v1.1 (official grammar)"
  - url: "https://github.com/mongodb/mongo-go-driver/tree/685cf13847cf82b628769502fabf130e5aa84d2f/bson"
    description: "MongoDB Go driver bson package directory listing (commit 685cf13)"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/685cf13847cf82b628769502fabf130e5aa84d2f/bson/types.go"
    description: "Type constants and binary subtype constants"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/685cf13847cf82b628769502fabf130e5aa84d2f/bson/primitive.go"
    description: "Go primitive types for all BSON types"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/685cf13847cf82b628769502fabf130e5aa84d2f/bson/raw_value.go"
    description: "RawValue type and per-type accessor methods"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/685cf13847cf82b628769502fabf130e5aa84d2f/bson/value_reader.go"
    description: "Wire decoding for all types (binary.LittleEndian calls, exact byte layouts)"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/685cf13847cf82b628769502fabf130e5aa84d2f/bson/objectid.go"
    description: "ObjectID structure and generation"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/685cf13847cf82b628769502fabf130e5aa84d2f/bson/decimal.go"
    description: "Decimal128 structure, bit layout, parse/format"
  - url: "https://github.com/mongodb/mongo-go-driver/tree/685cf13847cf82b628769502fabf130e5aa84d2f/x/bsonx/bsoncore"
    description: "bsoncore package directory listing"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/685cf13847cf82b628769502fabf130e5aa84d2f/x/bsonx/bsoncore/bsoncore.go"
    description: "Low-level Append*/Read* functions — definitive wire layout"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/685cf13847cf82b628769502fabf130e5aa84d2f/x/bsonx/bsoncore/value.go"
    description: "bsoncore.Value accessors, String (Extended JSON) representations"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/685cf13847cf82b628769502fabf130e5aa84d2f/x/bsonx/bsoncore/type.go"
    description: "bsoncore.Type constants and String() names"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/685cf13847cf82b628769502fabf130e5aa84d2f/bson/vector.go"
    description: "Vector (Binary subtype 9) encoding/decoding"
  - url: "https://raw.githubusercontent.com/mongodb/mongo-go-driver/685cf13847cf82b628769502fabf130e5aa84d2f/bson/writer.go"
    description: "ValueWriter interface — complete list of writable types"
commit: "685cf13847cf82b628769502fabf130e5aa84d2f"
tag: "v2.6.1"
spec_version: "BSON 1.1"
```

---

## 1. Global Encoding Rules

- **All multi-byte integers are little-endian** (least-significant byte first). This applies to
  `int32`, `int64`, `uint64`, `double`, and `decimal128`.
- **Strings** are length-prefixed: a 4-byte LE `int32` giving the number of bytes including the
  mandatory trailing `NUL` byte (`0x00`), followed by the UTF-8 byte sequence, followed by `0x00`.
  The length field therefore equals `strlen + 1`.
- **C-strings** (`cstring`) are raw byte sequences terminated by a single `0x00` byte. They must
  NOT contain internal `0x00` bytes. No length prefix.
- **Documents and arrays** are self-delimiting: the first 4 bytes are a LE `int32` giving the total
  byte length of the document (including those 4 bytes and the trailing `0x00` terminator). The
  minimum valid document is `\x05\x00\x00\x00\x00` (5 bytes, empty).
- An **element** inside a document is:
  `[type byte (1)] [key as cstring (variable)] [value bytes (variable)]`
- A document ends with a single `0x00` byte (the "end-of-document" marker, also called the null
  terminator).

---

## 2. Document Top-Level Grammar (BNF from Spec)

```
document  ::= int32 e_list uint8(0)
e_list    ::= element e_list | ""
element   ::= type_byte e_name value_bytes
e_name    ::= cstring
string    ::= int32 (byte*) uint8(0)     ; int32 = len(byte*) + 1
cstring   ::= (byte*) uint8(0)           ; no 0x00 inside byte*
binary    ::= int32 subtype (byte*)      ; int32 = len(byte*)
subtype   ::= uint8
```

The `int32` at the start of a document includes itself (4 bytes) plus all element bytes plus the
final `0x00`.  
**Minimum document:** 5 bytes = `\x05\x00\x00\x00\x00`.

---

## 3. BSON Type Table

| Name              | Type Byte | Go Constant (`bson.Type`)      | Wire Size (value only) | Status      |
|-------------------|-----------|-------------------------------|------------------------|-------------|
| Double            | `0x01`    | `TypeDouble`                  | 8 bytes                | Current     |
| String            | `0x02`    | `TypeString`                  | 4 + n + 1 bytes        | Current     |
| Embedded Document | `0x03`    | `TypeEmbeddedDocument`        | variable (self-length) | Current     |
| Array             | `0x04`    | `TypeArray`                   | variable (self-length) | Current     |
| Binary            | `0x05`    | `TypeBinary`                  | 4 + 1 + n bytes        | Current     |
| Undefined         | `0x06`    | `TypeUndefined`               | 0 bytes                | Deprecated  |
| ObjectID          | `0x07`    | `TypeObjectID`                | 12 bytes               | Current     |
| Boolean           | `0x08`    | `TypeBoolean`                 | 1 byte                 | Current     |
| UTC DateTime      | `0x09`    | `TypeDateTime`                | 8 bytes                | Current     |
| Null              | `0x0A`    | `TypeNull`                    | 0 bytes                | Current     |
| Regex             | `0x0B`    | `TypeRegex`                   | variable (2 cstrings)  | Current     |
| DBPointer         | `0x0C`    | `TypeDBPointer`               | 4 + n + 1 + 12 bytes   | Deprecated  |
| JavaScript        | `0x0D`    | `TypeJavaScript`              | 4 + n + 1 bytes        | Current     |
| Symbol            | `0x0E`    | `TypeSymbol`                  | 4 + n + 1 bytes        | Deprecated  |
| Code With Scope   | `0x0F`    | `TypeCodeWithScope`           | variable               | Deprecated  |
| Int32             | `0x10`    | `TypeInt32`                   | 4 bytes                | Current     |
| Timestamp         | `0x11`    | `TypeTimestamp`               | 8 bytes                | Current     |
| Int64             | `0x12`    | `TypeInt64`                   | 8 bytes                | Current     |
| Decimal128        | `0x13`    | `TypeDecimal128`              | 16 bytes               | Current     |
| Min Key           | `0xFF`    | `TypeMinKey`                  | 0 bytes                | Current     |
| Max Key           | `0x7F`    | `TypeMaxKey`                  | 0 bytes                | Current     |

> **Note on signed\_byte vs byte:** The spec grammar uses `signed_byte(n)` for type bytes. Because
> the type byte is a single octet, the signed representation is only relevant for `MinKey` whose
> value is `0xFF` = -1 when treated as a signed byte. In Go the `Type` alias is `byte` (unsigned).
> `TypeMinKey = 0xFF` and `TypeMaxKey = 0x7F`.

---

## 4. Per-Type Wire Encoding

### 4.1 Double — `0x01`

**Wire layout:**
```
[8 bytes: IEEE 754-2008 binary64, little-endian]
```

- Encoded as a 64-bit floating-point number.
- In Go: `math.Float64bits(f)` produces a `uint64`; written with
  `binary.LittleEndian.PutUint64`.
- Decoded with `binary.LittleEndian.Uint64` then `math.Float64frombits`.
- Special values: `NaN`, `+Infinity`, `-Infinity` are valid per IEEE 754 and must be preserved.

**Go mapping:**
```go
// bson package
TypeDouble Type = 0x01
// primitive: Go float64 (no wrapper type needed)
// bsoncore append:
func AppendDouble(dst []byte, f float64) []byte  // uses binaryutil.Append64(dst, math.Float64bits(f))
// bsoncore read:
func ReadDouble(src []byte) (float64, []byte, bool)
```

**Example:** `float64(1.0)` → `\x3F\xF0\x00\x00\x00\x00\x00\x00` (big-endian notation) =
`\x00\x00\x00\x00\x00\x00\xF0\x3F` on the wire (little-endian).

---

### 4.2 String — `0x02`

**Wire layout:**
```
[int32: total byte count including NUL, LE] [UTF-8 bytes] [0x00]
```

- `int32` length = `len(utf8_bytes) + 1` (the +1 accounts for the `0x00` terminator).
- The `(byte*)` content is zero or more UTF-8 encoded characters.
- The trailing `0x00` is mandatory and must be present. It is NOT part of the string value.
- Length must be ≥ 1 (minimum valid string is a single `0x00` byte, representing the empty string,
  with `int32 = 1`).

**Go mapping:**
```go
TypeString Type = 0x02
// primitive: Go string
// bsoncore append:
func AppendString(dst []byte, s string) []byte  // calls appendstring internally
// appendstring implementation:
//   l := int32(len(s) + 1)
//   dst = appendLength(dst, l)    // 4-byte LE int32
//   dst = append(dst, s...)
//   return append(dst, 0x00)
// bsoncore read:
func ReadString(src []byte) (string, []byte, bool)
// readstring implementation:
//   l, rem, ok = ReadLength(src)  // reads 4-byte LE int32
//   return string(rem[:l-1])      // strips trailing NUL
```

---

### 4.3 Embedded Document — `0x03`

**Wire layout:**
```
[int32: total document byte count, LE] [e_list] [0x00]
```

- Identical structure to a top-level BSON document.
- The `int32` includes itself, all elements, and the terminal `0x00`.
- Minimum value: `\x05\x00\x00\x00\x00` (5-byte empty document).

**Go mapping:**
```go
TypeEmbeddedDocument Type = 0x03
// primitives: bson.D (ordered), bson.M (unordered map), bson.Raw ([]byte)
// D is []E where E = {Key string, Value any}
// bsoncore: Document type is []byte; length is self-contained
// bsoncore append:
func AppendDocumentElement(dst []byte, key string, doc []byte) []byte
func BuildDocument(dst []byte, elems ...[]byte) []byte
// bsoncore read:
func ReadDocument(src []byte) (doc Document, rem []byte, ok bool)
//   calls readLengthBytes: reads int32, returns src[:l], src[l:]
```

---

### 4.4 Array — `0x04`

**Wire layout:**
```
[int32: total array byte count, LE] [e_list] [0x00]
```

- Encoded exactly like an embedded document (same grammar production).
- Keys are the decimal string representations of 0-based indices: `"0"`, `"1"`, `"2"`, etc.
  These keys are stored as cstrings.
- The array `['red', 'blue']` encodes as the document `{'0': 'red', '1': 'blue'}`.

**Go mapping:**
```go
TypeArray Type = 0x04
// primitive: bson.A ([]any) or bson.RawArray ([]byte)
// bsoncore: Array type is []byte (identical representation to Document)
// bsoncore append:
func AppendArrayElement(dst []byte, key string, arr []byte) []byte
func BuildArray(dst []byte, values ...Value) []byte
//   uses strconv.Itoa(pos) for sequential integer keys
// bsoncore read:
func ReadArray(src []byte) (arr Array, rem []byte, ok bool)
```

---

### 4.5 Binary — `0x05`

**Wire layout:**
```
[int32: byte count of data (n), LE] [subtype: 1 byte] [data: n bytes]
```

- The `int32` is the number of bytes in the `data` portion **only** (does not include itself or the
  subtype byte).
- The subtype byte immediately follows the length.
- The raw data bytes follow the subtype.

**Special case — Subtype 0x02 (Binary Old):**  
The data portion itself starts with another `int32` (LE) giving the length of the real data, then
that many data bytes. So the outer `int32` = inner `int32 + 4`.
```
[outer int32 = n+4, LE] [0x02] [inner int32 = n, LE] [data: n bytes]
```

**Go mapping:**
```go
TypeBinary Type = 0x05
// primitive:
type Binary struct {
    Subtype byte
    Data    []byte
}
// bsoncore append (all subtypes except 0x02):
//   dst = appendLength(dst, int32(len(b)))  // 4-byte LE
//   dst = append(dst, subtype)
//   return append(dst, b...)
// bsoncore append (subtype 0x02):
//   dst = appendLength(dst, int32(len(b)+4))  // outer length
//   dst = append(dst, 0x02)
//   dst = appendLength(dst, int32(len(b)))    // inner length
//   return append(dst, b...)
// bsoncore read:
func ReadBinary(src []byte) (subtype byte, bin []byte, rem []byte, ok bool)
//   length = ReadLength(src)          // reads outer int32
//   subtype = rem[0]
//   if subtype == 0x02: length = ReadLength(rem) // reads inner int32
//   return subtype, rem[:length], rem[length:]
```

**Binary subtypes:**

| Subtype Byte | Go Constant             | Meaning                         |
|--------------|-------------------------|---------------------------------|
| `0x00`       | `TypeBinaryGeneric`     | Generic / default               |
| `0x01`       | `TypeBinaryFunction`    | Function                        |
| `0x02`       | `TypeBinaryBinaryOld`   | Binary (old) — deprecated       |
| `0x03`       | `TypeBinaryUUIDOld`     | UUID (old) — deprecated         |
| `0x04`       | `TypeBinaryUUID`        | UUID (current)                  |
| `0x05`       | `TypeBinaryMD5`         | MD5 hash                        |
| `0x06`       | `TypeBinaryEncrypted`   | Encrypted BSON value            |
| `0x07`       | `TypeBinaryColumn`      | Compressed BSON column          |
| `0x08`       | `TypeBinarySensitive`   | Sensitive data                  |
| `0x09`       | `TypeBinaryVector`      | Dense numeric vector (see §4.5.1) |
| `0x80`–`0xFF`| `TypeBinaryUserDefined` | User-defined range              |

#### 4.5.1 Binary Subtype 9 — Vector

Binary subtype `0x09` encodes a dense homogeneous array of numerics. After the standard binary
header (`int32` length + `0x09` subtype byte), the data payload is:
```
[dtype: 1 byte] [padding: 1 byte] [elements...]
```

The first byte (`dtype`) specifies the element type:

| dtype byte | Go constant       | Element encoding                        |
|------------|-------------------|-----------------------------------------|
| `0x03`     | `Int8Vector`      | 1 byte per element, signed two's complement |
| `0x10`     | `PackedBitVector` | Packed bits, MSB first; `padding` (0–7) specifies how many low bits in the last byte to ignore |
| `0x27`     | `Float32Vector`   | 4 bytes per element, IEEE 754 single-precision, **little-endian** |

The `padding` byte must be `0` for `Int8Vector` and `Float32Vector`. For `PackedBitVector`,
`padding` is 0–7 and indicates the number of unused trailing bits in the final byte. `padding > 0`
is invalid if there are zero data bytes.

**Encoding details:**
- `Int8Vector`: `[0x03][0x00][int8_0][int8_1]...` — each int8 stored as a single raw byte.
- `Float32Vector`: `[0x27][0x00][f0_byte0][f0_byte1][f0_byte2][f0_byte3]...` — each float32
  written as `binary.LittleEndian.PutUint32(a[:], math.Float32bits(e))`.
- `PackedBitVector`: `[0x10][padding][byte0][byte1]...` — bits stored MSB-first within each byte;
  the last byte may have `padding` low bits that must be ignored.

**Go mapping:**
```go
type Vector struct { /* unexported */ }
func NewVector[T int8 | float32](data []T) Vector
func NewPackedBitVector(bits []byte, padding uint8) (Vector, error)
func (v Vector) Binary() Binary   // marshals to Binary{Subtype: 0x09, Data: ...}
func NewVectorFromBinary(b Binary) (Vector, error)
```

---

### 4.6 Undefined — `0x06`

**Wire layout:** (no value bytes)

- Deprecated. Drivers must be able to read it but should not generate it.
- No bytes follow the element header (type + key).

**Go mapping:**
```go
TypeUndefined Type = 0x06
type Undefined struct{}
```

---

### 4.7 ObjectID — `0x07`

**Wire layout:**
```
[12 bytes: ObjectID]
```

A 12-byte value broken down as:
```
bytes[0:4]  — Unix timestamp seconds (big-endian uint32)
bytes[4:9]  — 5-byte random process-unique value
bytes[9:12] — 3-byte incrementing counter (big-endian, MSB first)
```

**Critical endianness detail:** The timestamp field uses **big-endian** encoding (unlike all other
BSON integer types). The Go driver confirms this:
```go
binary.BigEndian.PutUint32(b[0:4], uint32(timestamp.Unix()))
```
The counter is also written MSB-first via `putUint24`:
```go
func putUint24(b []byte, v uint32) {
    b[0] = byte(v >> 16)
    b[1] = byte(v >> 8)
    b[2] = byte(v)
}
```

The 12-byte value is stored **as-is** on the wire (no re-encoding of the structure).

**Go mapping:**
```go
TypeObjectID Type = 0x07
type ObjectID [12]byte
var NilObjectID ObjectID  // zero value
func NewObjectID() ObjectID
func NewObjectIDFromTimestamp(timestamp time.Time) ObjectID
func ObjectIDFromHex(s string) (ObjectID, error)  // s must be exactly 24 hex chars
func (id ObjectID) Hex() string   // 24-char lowercase hex
func (id ObjectID) Timestamp() time.Time
// bsoncore:
func AppendObjectID(dst []byte, oid [12]byte) []byte  // appends all 12 bytes raw
func ReadObjectID(src []byte) ([12]byte, []byte, bool)
```

---

### 4.8 Boolean — `0x08`

**Wire layout:**
```
[1 byte: 0x00 = false, 0x01 = true]
```

- Only `0x00` and `0x01` are valid. Any other value is an error.
- From the Go driver: `if b > 1 { return false, fmt.Errorf("invalid byte for boolean, %b", b) }`

**Go mapping:**
```go
TypeBoolean Type = 0x08
// primitive: Go bool
func AppendBoolean(dst []byte, b bool) []byte  // appends 0x01 or 0x00
func ReadBoolean(src []byte) (bool, []byte, bool)  // src[0] == 0x01
```

---

### 4.9 UTC DateTime — `0x09`

**Wire layout:**
```
[8 bytes: int64, LE, milliseconds since Unix epoch (Jan 1 1970 00:00:00 UTC)]
```

- Signed 64-bit integer.
- Negative values represent dates before the Unix epoch.
- Resolution is milliseconds (not microseconds or nanoseconds).

**Go mapping:**
```go
TypeDateTime Type = 0x09
type DateTime int64  // milliseconds since epoch
func NewDateTimeFromTime(t time.Time) DateTime
//   = DateTime(t.Unix()*1e3 + int64(t.Nanosecond())/1e6)
func (d DateTime) Time() time.Time
//   = time.Unix(int64(d)/1000, int64(d)%1000*1000000)
// bsoncore:
func AppendDateTime(dst []byte, dt int64) []byte  // binaryutil.Append64 (LE int64)
func ReadDateTime(src []byte) (int64, []byte, bool)  // binaryutil.ReadI64
```

---

### 4.10 Null — `0x0A`

**Wire layout:** (no value bytes)

- No bytes follow the element header.

**Go mapping:**
```go
TypeNull Type = 0x0A
type Null struct{}
```

---

### 4.11 Regular Expression — `0x0B`

**Wire layout:**
```
[pattern: cstring] [options: cstring]
```

- Both `pattern` and `options` are C-strings (raw bytes terminated by `0x00`).
- Neither field has a length prefix.
- `options` contains single-character flags in **alphabetical order** (enforced by the Go driver's
  `sortStringAlphebeticAscending` in the Extended JSON formatter).
- Supported option characters: `i` (case insensitive), `m` (multiline), `s` (dotall), `x`
  (verbose), `u` (Unicode).
- Neither the pattern nor the options may contain a `0x00` byte.

**Go mapping:**
```go
TypeRegex Type = 0x0B
type Regex struct {
    Pattern string
    Options string
}
// bsoncore:
func AppendRegex(dst []byte, pattern, options string) []byte
//   panics if pattern or options contain 0x00
//   = append(dst, pattern + "\x00" + options + "\x00"...)
func ReadRegex(src []byte) (pattern, options string, rem []byte, ok bool)
//   reads two consecutive cstrings
```

---

### 4.12 DBPointer — `0x0C`

**Wire layout:**
```
[string: namespace] [12 bytes: ObjectID]
```

- `string` uses the standard BSON string encoding: `int32` (LE) length prefix + UTF-8 bytes +
  `0x00` terminator.
- Followed immediately by a 12-byte ObjectID (raw, no prefix).
- Deprecated. Drivers must read it; should not generate it.

**Go mapping:**
```go
TypeDBPointer Type = 0x0C
type DBPointer struct {
    DB      string
    Pointer ObjectID
}
// bsoncore:
func AppendDBPointer(dst []byte, ns string, oid [12]byte) []byte
//   = appendstring(dst, ns) then append(dst, oid[:]...)
func ReadDBPointer(src []byte) (ns string, oid [12]byte, rem []byte, ok bool)
// value_reader.go ReadDBPointer:
//   ns, err := vr.readString()   // standard BSON string read
//   oidBytes, err := vr.readBytes(12)
```

---

### 4.13 JavaScript — `0x0D`

**Wire layout:**
```
[string: JavaScript code]
```

- Same encoding as BSON String (`0x02`): `int32` length (LE) + UTF-8 bytes + `0x00`.

**Go mapping:**
```go
TypeJavaScript Type = 0x0D
type JavaScript string
// bsoncore:
func AppendJavaScript(dst []byte, js string) []byte  // = appendstring(dst, js)
func ReadJavaScript(src []byte) (js string, rem []byte, ok bool)
```

---

### 4.14 Symbol — `0x0E`

**Wire layout:**
```
[string: symbol]
```

- Same encoding as BSON String: `int32` length (LE) + UTF-8 bytes + `0x00`.
- Deprecated.

**Go mapping:**
```go
TypeSymbol Type = 0x0E
type Symbol string
// bsoncore:
func AppendSymbol(dst []byte, symbol string) []byte  // = appendstring(dst, symbol)
func ReadSymbol(src []byte) (symbol string, rem []byte, ok bool)
```

---

### 4.15 Code With Scope — `0x0F`

**Wire layout:**
```
[int32: total length of entire code_w_s value, LE]
[string: JavaScript code]
[document: scope]
```

The outer `int32` is the total byte count of the entire `code_w_s` encoding, including the 4 bytes
for itself. Therefore:
```
total_length = 4 + (4 + len(code) + 1) + len(scope_document)
             = 4 + string_encoded_size + scope_document_size
```
Where `string_encoded_size = 4 + len(code_utf8) + 1` and `scope_document_size` is the full
self-describing document (starts with its own `int32` length).

- Deprecated.

**Go mapping:**
```go
TypeCodeWithScope Type = 0x0F
type CodeWithScope struct {
    Code  JavaScript
    Scope any
}
// bsoncore:
func AppendCodeWithScope(dst []byte, code string, scope []byte) []byte {
    length := int32(4 + 4 + len(code) + 1 + len(scope))
    dst = appendLength(dst, length)       // outer int32
    return append(appendstring(dst, code), scope...)
}
func ReadCodeWithScope(src []byte) (code string, scope []byte, rem []byte, ok bool)
//   length, rem = ReadLength(src)        // outer int32
//   code, rem = readstring(rem)          // BSON string
//   scope, rem = ReadDocument(rem)       // embedded document
// value_reader.go validates:
//   componentsLength = int64(4 + strLength + 4) + int64(scopeDocSize)
//   assert totalLength == componentsLength
```

---

### 4.16 Int32 — `0x10`

**Wire layout:**
```
[4 bytes: int32, little-endian, two's complement]
```

**Go mapping:**
```go
TypeInt32 Type = 0x10
// primitive: Go int32
// bsoncore:
func AppendInt32(dst []byte, i32 int32) []byte  // binaryutil.Append32(dst, i32)
func ReadInt32(src []byte) (int32, []byte, bool)  // binaryutil.ReadI32
```

---

### 4.17 Timestamp — `0x11`

**Wire layout:**
```
[4 bytes: increment (i), uint32, little-endian]
[4 bytes: seconds (t), uint32, little-endian]
```

**Important:** The wire order is `i` first (lower 4 bytes), `t` second (upper 4 bytes) — confirmed
by the Go driver:
```go
// AppendTimestamp:
func AppendTimestamp(dst []byte, t, i uint32) []byte {
    return binaryutil.Append32(binaryutil.Append32(dst, i), t)
    // i is the lower 4 bytes, t is the higher 4 bytes
}
// ReadTimestamp:
i, rem, ok = binaryutil.ReadU32(src)   // reads increment first
t, rem, ok = binaryutil.ReadU32(rem)   // reads seconds second
return t, i, rem, true
```

- This is an internal MongoDB type used by replication and sharding.
- `t` is a Unix timestamp in seconds; `i` is an incrementing ordinal for operations in the same
  second.
- Callers compare timestamps using `(t, i)` as a pair: larger `t` wins; ties broken by `i`.

**Go mapping:**
```go
TypeTimestamp Type = 0x11
type Timestamp struct {
    T uint32  // seconds (higher field semantically)
    I uint32  // ordinal increment (lower field semantically)
}
func (tp Timestamp) After(tp2 Timestamp) bool   // tp.T > tp2.T || (tp.T==tp2.T && tp.I>tp2.I)
func (tp Timestamp) Before(tp2 Timestamp) bool
func (tp Timestamp) Compare(tp2 Timestamp) int  // -1, 0, +1
```

---

### 4.18 Int64 — `0x12`

**Wire layout:**
```
[8 bytes: int64, little-endian, two's complement]
```

**Go mapping:**
```go
TypeInt64 Type = 0x12
// primitive: Go int64
// bsoncore:
func AppendInt64(dst []byte, i64 int64) []byte  // binaryutil.Append64(dst, i64)
func ReadInt64(src []byte) (int64, []byte, bool)  // binaryutil.ReadI64
```

---

### 4.19 Decimal128 — `0x13`

**Wire layout:**
```
[8 bytes: low uint64, little-endian]
[8 bytes: high uint64, little-endian]
```

Total: 16 bytes. The **low** word is stored first (bytes 0–7), the **high** word is stored second
(bytes 8–15). Both words are unsigned 64-bit integers in little-endian order.

This is confirmed by the Go driver:
```go
// AppendDecimal128:
func AppendDecimal128(dst []byte, high, low uint64) []byte {
    return binaryutil.Append64(binaryutil.Append64(dst, low), high)
    // low first, high second
}
// ReadDecimal128:
low, rem, ok = binaryutil.ReadU64(src)   // reads bytes 0-7
high, rem, ok = binaryutil.ReadU64(rem)  // reads bytes 8-15
// value_reader.go ReadDecimal128:
l := binary.LittleEndian.Uint64(b[0:8])   // low
h := binary.LittleEndian.Uint64(b[8:16])  // high
return NewDecimal128(h, l)
```

**Bit structure of the combined 128-bit value** (IEEE 754-2008 decimal128):

The 128 bits form a single value where the most-significant bit is bit 127 (in the `high` word at
bit 63 of `high`). Bit layout within `high`:

| Bits (in high word) | Field                        |
|---------------------|------------------------------|
| bit 63              | Sign bit (1 = negative)      |
| bits 61–62 = `11`   | Indicates special combination field |
| bits 47–60 (14 bits)| Exponent (when bits 61–62 == `11`) |
| bits 49–62 (14 bits)| Exponent (when bits 61–62 != `11`) |
| remaining bits      | Significand (coefficient)    |

- Exponent is stored as a biased value; actual exponent = stored_exponent + `MinDecimal128Exp`
  where `MinDecimal128Exp = -6176`.
- Maximum exponent: `MaxDecimal128Exp = 6111`.
- Maximum significand (34 decimal digits): `9999999999999999999999999999999999`.
- Special values:
  - `NaN`: `high >> 58 & 0x1F == 0x1F`
  - `+Infinity`: `high >> 58 & 0x1F == 0x1E`, sign bit = 0
  - `-Infinity`: `high >> 58 & 0x1F == 0x1E`, sign bit = 1

**Go mapping:**
```go
TypeDecimal128 Type = 0x13
type Decimal128 struct { h, l uint64 }  // h = high word, l = low word
func NewDecimal128(h, l uint64) Decimal128
func (d Decimal128) GetBytes() (uint64, uint64)  // returns h, l
func ParseDecimal128(s string) (Decimal128, error)  // parses decimal string
func (d Decimal128) String() string
func (d Decimal128) BigInt() (*big.Int, int, error)  // returns significand, exponent, error
func (d Decimal128) IsNaN() bool
func (d Decimal128) IsInf() int  // +1, 0, -1
const MaxDecimal128Exp = 6111
const MinDecimal128Exp = -6176
```

---

### 4.20 Min Key — `0xFF`

**Wire layout:** (no value bytes)

- A special sentinel that compares lower than all other BSON values.
- Type byte `0xFF` = 255 unsigned = -1 signed.

**Go mapping:**
```go
TypeMinKey Type = 0xFF
type MinKey struct{}
```

---

### 4.21 Max Key — `0x7F`

**Wire layout:** (no value bytes)

- A special sentinel that compares higher than all other BSON values.

**Go mapping:**
```go
TypeMaxKey Type = 0x7F
type MaxKey struct{}
```

---

## 5. Fixed-Size Value Reference Table

This table gives the exact number of value bytes that the wire reader must consume for each type.
Where the size is "variable", the reader must parse a length from the first bytes.

| Type Byte | Name              | Value Byte Count    | How Determined                          |
|-----------|-------------------|---------------------|-----------------------------------------|
| `0x01`    | Double            | **8**               | Fixed                                   |
| `0x02`    | String            | `4 + length`        | First 4 bytes = LE int32 length (incl. NUL) |
| `0x03`    | Embedded Document | first 4 bytes (LE)  | First 4 bytes = total document length   |
| `0x04`    | Array             | first 4 bytes (LE)  | Same as document                        |
| `0x05`    | Binary            | `4 + 1 + length`    | First 4 bytes = data length; +1 for subtype |
| `0x06`    | Undefined         | **0**               | Fixed (deprecated)                      |
| `0x07`    | ObjectID          | **12**              | Fixed                                   |
| `0x08`    | Boolean           | **1**               | Fixed                                   |
| `0x09`    | DateTime          | **8**               | Fixed                                   |
| `0x0A`    | Null              | **0**               | Fixed                                   |
| `0x0B`    | Regex             | variable            | Two consecutive NUL-terminated cstrings |
| `0x0C`    | DBPointer         | `4 + strLen + 12`   | First 4 bytes = string length           |
| `0x0D`    | JavaScript        | `4 + length`        | Same as String                          |
| `0x0E`    | Symbol            | `4 + length`        | Same as String                          |
| `0x0F`    | Code With Scope   | first 4 bytes (LE)  | First 4 bytes = total cws length        |
| `0x10`    | Int32             | **4**               | Fixed                                   |
| `0x11`    | Timestamp         | **8**               | Fixed                                   |
| `0x12`    | Int64             | **8**               | Fixed                                   |
| `0x13`    | Decimal128        | **16**              | Fixed                                   |
| `0x7F`    | Max Key           | **0**               | Fixed                                   |
| `0xFF`    | Min Key           | **0**               | Fixed                                   |

The `peekNextValueSize` function in `value_reader.go` implements this exact table and is the
authoritative reference for computing how many bytes to skip when skipping an element.

For Binary specifically:
```go
case TypeBinary:
    length, err = vr.peekLength()
    length += 4 + 1   // binary_data_length + subtype_byte
// so total binary value bytes = (4 bytes for length field) + 1 + data_length
```

For DBPointer specifically:
```go
case TypeDBPointer:
    length, err = vr.peekLength()
    length += 4 + 12  // string_length_field + ObjectID
// total = (4 bytes for string length field) + string_length + 12
```

---

## 6. Go Primitive Type Mapping Summary

| BSON Type          | Go Primitive Type         | Notes                                              |
|--------------------|---------------------------|----------------------------------------------------|
| Double             | `float64`                 | —                                                  |
| String             | `string`                  | —                                                  |
| Embedded Document  | `bson.D`, `bson.M`, `bson.Raw` | D = ordered `[]E`; M = `map[string]any`; Raw = `[]byte` |
| Array              | `bson.A`, `bson.RawArray` | A = `[]any`; RawArray = `[]byte`                   |
| Binary             | `bson.Binary`             | `struct{ Subtype byte; Data []byte }`              |
| Undefined          | `bson.Undefined`          | `struct{}`; deprecated                             |
| ObjectID           | `bson.ObjectID`           | `[12]byte`                                         |
| Boolean            | `bool`                    | —                                                  |
| DateTime           | `bson.DateTime`           | `int64` (ms since epoch); also `time.Time`         |
| Null               | `bson.Null`               | `struct{}` (or Go `nil`)                           |
| Regex              | `bson.Regex`              | `struct{ Pattern, Options string }`                |
| DBPointer          | `bson.DBPointer`          | `struct{ DB string; Pointer ObjectID }`; deprecated|
| JavaScript         | `bson.JavaScript`         | `string` typedef                                   |
| Symbol             | `bson.Symbol`             | `string` typedef; deprecated                       |
| Code With Scope    | `bson.CodeWithScope`      | `struct{ Code JavaScript; Scope any }`; deprecated |
| Int32              | `int32`                   | —                                                  |
| Timestamp          | `bson.Timestamp`          | `struct{ T, I uint32 }`                            |
| Int64              | `int64`                   | —                                                  |
| Decimal128         | `bson.Decimal128`         | `struct{ h, l uint64 }` (high, low)                |
| Min Key            | `bson.MinKey`             | `struct{}`                                         |
| Max Key            | `bson.MaxKey`             | `struct{}`                                         |
| Vector (sub 0x09)  | `bson.Vector`             | Wraps typed slice; marshals to `Binary{0x09, ...}` |

---

## 7. Key/Name Encoding Rules

- All element keys are encoded as **cstrings**: raw bytes + `0x00`.
- Keys **MUST NOT** contain a `0x00` byte. The Go driver panics with
  `"BSON element keys cannot contain null bytes"` if this constraint is violated.
- Keys are compared byte-by-byte; BSON is key-order preserving (the `bson.D` type preserves
  insertion order).
- Regex pattern and options also use cstring encoding and must not contain `0x00` (same panic
  `"BSON regex values cannot contain null bytes"`).

---

## 8. Document Traversal Algorithm

A correct decoder traverses a document as follows:

```
1. Read int32 at offset 0 → total_length (LE)
2. Compute end = start + total_length
3. Loop:
   a. Read 1 byte → type_byte
   b. If type_byte == 0x00:
      - Assert current position == end - 1 (or exactly at end after consuming terminator)
      - Done (ErrEOD)
   c. Read cstring → key (advance past 0x00)
   d. Read value bytes per §5 Fixed-Size table for type_byte
   e. Yield element (type, key, value_bytes)
   f. Goto a
```

The `value_reader.go` `ReadElement` method implements exactly this:
```go
t, err := vr.readByte()            // step a
if t == 0 { /* check end, return ErrEOD */ }  // step b
name, err := vr.readCString()     // step c
// value bytes are read lazily when the caller invokes the type-specific Read method
```

Array traversal is identical; the only difference is that keys are decimal integer strings and
`ReadValue` is called instead of `ReadElement`.

---

## 9. Extended JSON Representation (for reference)

The Go driver's `Value.String()` and `Value.StringN()` emit Canonical Extended JSON. An
implementor will encounter these representations in tests. Key mappings:

| BSON Type      | Extended JSON form                                                  |
|----------------|---------------------------------------------------------------------|
| Double         | `{"$numberDouble":"<value>"}` (special: `"Infinity"`, `"-Infinity"`, `"NaN"`) |
| String         | `"<escaped UTF-8>"`                                                 |
| Document       | `{...}` (recursive)                                                 |
| Array          | `[...]` (recursive)                                                 |
| Binary         | `{"$binary":{"base64":"<b64>","subType":"<hex2>"}}`                |
| Undefined      | `{"$undefined":true}`                                               |
| ObjectID       | `{"$oid":"<24hexchars>"}`                                           |
| Boolean        | `true` / `false`                                                    |
| DateTime       | `{"$date":{"$numberLong":"<ms>"}}`                                  |
| Null           | `null`                                                              |
| Regex          | `{"$regularExpression":{"pattern":"<p>","options":"<opts>"}}`      |
| DBPointer      | `{"$dbPointer":{"$ref":"<ns>","$id":{"$oid":"<hex>"}}}`            |
| JavaScript     | `{"$code":"<code>"}`                                                |
| Symbol         | `{"$symbol":"<sym>"}`                                               |
| Code W/ Scope  | `{"$code":"<code>","$scope":{...}}`                                 |
| Int32          | `{"$numberInt":"<value>"}`                                          |
| Timestamp      | `{"$timestamp":{"t":<secs>,"i":<inc>}}`                            |
| Int64          | `{"$numberLong":"<value>"}`                                         |
| Decimal128     | `{"$numberDecimal":"<string>"}`                                     |
| MinKey         | `{"$minKey":1}`                                                     |
| MaxKey         | `{"$maxKey":1}`                                                     |

---

## 10. Numeric Interoperability Notes

The Go driver provides cross-type numeric helpers:

- `Value.AsInt64()` / `AsInt64OK()`: works for `TypeDouble`, `TypeInt32`, `TypeInt64` (not
  `Decimal128`).
- `Value.AsInt32()` / `AsInt32OK()`: same three types; truncates.
- `Value.AsFloat64()` / `AsFloat64OK()`: same three types.
- `Value.IsNumber()` returns true for `TypeDouble`, `TypeInt32`, `TypeInt64`, `TypeDecimal128`.
- `Decimal128` is intentionally excluded from `AsInt64` and `AsFloat64` (panics with
  `ElementTypeError`).

---

## 11. Wire-Level Annotated Example

Empty document:
```
05 00 00 00   ← int32 total length = 5, LE
00            ← end-of-document terminator
```

Document `{"hello": "world"}`:
```
16 00 00 00         ← int32 total length = 22, LE
02                  ← type byte: String (0x02)
68 65 6C 6C 6F 00   ← key: "hello\0" (cstring)
06 00 00 00         ← string length = 6 (len("world")+1), LE
77 6F 72 6C 64 00   ← "world\0"
00                  ← end-of-document
```

Document `{"n": 42}` (Int32):
```
0C 00 00 00         ← total length = 12
10                  ← type: Int32 (0x10)
6E 00               ← key: "n\0"
2A 00 00 00         ← int32 value 42, LE
00                  ← end-of-document
```

Document `{"flag": true}` (Boolean):
```
0C 00 00 00         ← total length = 12
08                  ← type: Boolean (0x08)
66 6C 61 67 00      ← key: "flag\0"
01                  ← true
00                  ← end-of-document
```

---

## 12. Constraints and Error Conditions

| Condition                               | Behavior per Go driver                          |
|-----------------------------------------|--------------------------------------------------|
| Boolean byte not 0x00 or 0x01          | Error: `"invalid byte for boolean, %b"`         |
| String length ≤ 0                       | Error: `"invalid string length: %d"`            |
| Document length ≤ 4                     | Error: `"invalid string length: %d"` (document) |
| End byte position mismatch              | Error: `invalidDocumentLengthError()`           |
| Key contains `0x00`                     | Panic: `"BSON element keys cannot contain null bytes"` |
| Regex contains `0x00`                   | Panic: `"BSON regex values cannot contain null bytes"` |
| Binary length < 0                       | Error: `"invalid length: %d"`                   |
| CodeWithScope length mismatch           | Error with total vs components lengths          |
| Vector padding > 7                      | Error: `errVectorPaddingTooLarge`               |
| Vector padding > 0 with zero data bytes | Error: `errNonZeroVectorPadding`                |
| Int8/Float32 vector with non-zero padding | Error: `errNonZeroVectorPadding`              |
| Unknown type byte                       | Error: `"attempted to read bytes of unknown BSON type %v"` |
