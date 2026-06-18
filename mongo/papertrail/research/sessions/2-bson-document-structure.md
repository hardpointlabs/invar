You are the research agent in a clean-room implementation project. Your output will 
be used by an implementation agent that has NO access to any reference material 
other than what you produce. Your spec must therefore be complete enough to implement 
from scratch.

## Sources you MAY use (only these)

1. MongoDB Go driver source code and tests (Apache 2.0)
   - Take it from tag `v2.6.1`, commit `685cf13847cf82b628769502fabf130e5aa84d2f`
   - https://github.com/mongodb/mongo-go-driver/
   - Read: x/mongo/driver/, x/bson/, test/, drivertest/
   - Focus especially on test files — they are behavioural specifications in 
     runnable form
   - Do NOT follow external links or read any file that references mongodb.com docs

2. BSON specification and Go implementation
   - https://bsonspec.org/spec.html (the spec itself, not MongoDB's docs)
   - https://github.com/mongodb/mongo-go-driver/tree/master/x/bson

## Sources you MUST NOT use

- Any page on mongodb.com (CC-NC-SA licensed)
- Any page on docs.mongodb.com
- Any MongoDB manual, specification document, or reference page
- Any StackOverflow answer or blog post that quotes MongoDB documentation
- The official MongoDB wire protocol specification document
- Do not follow links to external documentation

## Output Requirements

Begin every response with a YAML provenance block listing every URL you fetched, in order. This is mandatory.


## What to produce

A single comprehensive markdown specification file (put it in the `./mongo/papertrail/research` directory) covering:

### 2. BSON document structure
The exact byte layout of a BSON document including the int32 length prefix, 
the element list, and the terminating null byte. Include the recursive 
structure for embedded documents and arrays.

---

Written to `./mongo/papertrail/research/bson-document-structure.md`. Here is a summary of what the spec covers:

---

**Sources fetched (14 URLs):**

1. `https://bsonspec.org/spec.html` — canonical grammar (v1.1)
2. `mongodb/mongo-go-driver` at `v2.6.1`:
   - `bson/types.go` — type byte and binary subtype constants
   - `bson/value_reader.go` — low-level parse logic for every type
   - `bson/primitive.go` — Go struct types for each BSON type
   - `bson/objectid.go` — ObjectID 12-byte layout
   - `bson/writer.go` — ValueWriter interface (authoritative list of writable types)
   - `bson/bson_corpus_spec_test.go` — round-trip test methodology
   - `x/bsonx/bsoncore/bsoncore.go` — all Append*/Read* functions
   - `x/bsonx/bsoncore/document.go` — Validate(), stream reader, LookupErr()
   - `x/bsonx/bsoncore/element.go` — Element type and key/value extraction
   - `x/bsonx/bsoncore/value.go` — Value type and per-type accessors

---

**Spec contents:**

| Section | What it specifies |
|---------|-------------------|
| §2 Grammar | Full BNF grammar from bsonspec.org v1.1 |
| §3 Document layout | Exact byte offsets: `int32` length, `e_list`, `0x00` terminator |
| §4 Element layout | Type byte + cstring key + value; key null-byte constraint |
| §5 Type byte table | All 20 valid type bytes with hex, decimal, and name |
| §6 Value encodings | Complete byte layout for all 21 types including edge cases (Binary subtype 0x02, Timestamp byte order, Decimal128 low/high word order, CodeWithScope length arithmetic) |
| §7 Value size table | How many bytes to consume for each type during sequential parsing |
| §8 Recursive structure | Embedded doc and array nesting; array key convention |
| §9 Byte examples | Empty doc, `{x:1}`, `{hello:"world"}`, `{a:{b:1}}`, booleans |
| §10 Validation rules | 12 enforced constraints with error names |
| §13 Constants | Complete Go constants block |
| §14 Error sentinels | All error types and values |
| §15 Test methodology | Round-trip invariants from the corpus test suite |