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

### 1. BSON type system
Every BSON type: its type byte value, its wire encoding (byte layout, 
endianness, length prefix conventions), and Go type mapping. Be precise 
about endianness — all BSON integers are little-endian.

---

Written to `./mongo/papertrail/research/bson-type-system.md`. Here is a summary of what the spec covers:

---

**Sources fetched (provenance block at top of file):**
- `https://bsonspec.org/spec.html` — BSON v1.1 formal grammar
- 13 raw source files from `mongodb/mongo-go-driver` at commit `685cf13847cf82b628769502fabf130e5aa84d2f` (`v2.6.1`)

**Spec contents:**

1. **Global encoding rules** — all multi-byte integers are little-endian; string encoding convention; document self-delimiting structure.

2. **Top-level grammar** — full BNF reproduced from the spec.

3. **Type table** — all 21 types with type byte, Go constant, wire size, and deprecation status.

4. **Per-type wire encoding** (§4.1–4.21) — for every type:
   - Exact byte layout with field sizes and endianness
   - Go type mapping and relevant `bsoncore` Append/Read functions
   - Special cases: Binary subtype 0x02 double-length prefix; ObjectID big-endian timestamp; Timestamp wire order (increment before seconds); Decimal128 low-then-high word order; Vector inner dtype/padding header.

5. **Fixed-size reference table** — how many value bytes each type consumes on the wire.

6. **Go primitive type mapping** — complete table of all BSON→Go type correspondences.

7. **Key/name encoding rules** — cstring constraints and no-NUL invariant.

8. **Document traversal algorithm** — step-by-step decoder loop.

9. **Extended JSON representation** — all types, useful for reading the driver's test output.

10. **Numeric interoperability** — AsInt64/AsFloat64 cross-type rules.

11. **Annotated wire examples** — empty doc, `{"hello":"world"}`, `{"n":42}`, `{"flag":true}`.

12. **Error conditions** — complete table of invalid-input behaviors from the Go driver source.