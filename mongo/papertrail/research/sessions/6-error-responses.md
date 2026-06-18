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

Avoid duplication of the content of the preceding markdown specs in the `./mongo/papertrail/research` folder as much as possible unless it affects readability.

## What to produce

A single comprehensive markdown specification file (put it in the `./mongo/papertrail/research` directory) covering:

### 6. Error response structure
The exact BSON document structure of error responses including the ok field, 
code field, codeName field, and errmsg field. Derive error code numeric values 
from the driver source.

---

The file has been written to `./mongo/papertrail/research/06-error-response-structure.md`.

Here is a summary of what was produced and the key findings from the source:

---

**`06-error-response-structure.md`** — sourced entirely from the driver's `errors.go`, `operation.go`, `driverutil/description.go`, `description/server.go`, and `topology/server.go`.

### Key findings an implementer must know

**`ok` field**
- Accepted as `1` (success) for BSON types: Int32, Int64, Double, or Boolean.
- Handshake responses use a stricter decoder (`AsInt64OK`) that only accepts Int32 and Int64.

**`code` field — critical asymmetry**
- At the **top level** of a command error: decoded with `Int32OK()` only. Must be BSON `int32`. A BSON `int64` here will be silently ignored (code defaults to 0).
- Inside `writeErrors[].code` and `writeConcernError.code`: decoded with `AsInt64OK()`, which accepts both Int32 and Int64.

**`topologyVersion` fields**
- `processId` must be BSON ObjectID (strict — `ObjectIDOK()`).
- `counter` must be BSON int64 (strict — `Int64OK()`, not `AsInt64OK()`).

**Error code table** — all 13 retryable codes, 5 node-is-recovering codes, 2 shutting-down codes, 3 not-primary codes, and 6 special-case codes (20, 26, 50, 79, 100, 391) with their exact numeric values extracted from the source.

**`writeErrors` vs `writeConcernError`** — both decoded from `ok: 1` responses; both contribute to the unified `Labels` slice; the exact field-by-field accessor calls are specified.