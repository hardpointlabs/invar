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

### 8. Behavioural edge cases
Derived from the driver test suite:
- What happens when an unknown command is received
- Behaviour on malformed BSON
- Reconnection behaviour (what the driver does when connection is lost)

---

Written to `mongo/papertrail/research/08-behavioural-edge-cases.md`. Here is a summary of what it covers:

---

### `08-behavioural-edge-cases.md` — contents

**Provenance block** lists all 7 URLs fetched in order.

**Section 1 — Unknown commands**
- The driver has no special handling; unknown commands produce a standard `ok: 0` response
- Expected: `code: 59` (`CommandNotFound`), `errmsg: "no such command: '...'"`, `codeName: "CommandNotFound"`
- Code 59 is not in `retryableCodes` → not retried
- `ProcessError` returns `driver.NoChange` — no pool clear, no server-description update
- The connection is returned to the pool normally

**Section 2 — Malformed BSON**
- Wire message length `< 4` or `> maxMessageSize` → connection closed, plain error
- OP_REPLY with document length = 0 must not infinite-loop (GODRIVER-2869); produces empty documents
- OP_MSG header with insufficient bytes → `"malformed wire message: insufficient bytes"` (bypasses `networkError`, no retry)
- `doc.Elements()` failure on structurally corrupt BSON → returned directly, no retry, no pool clear
- Type mismatches on `ok`, `errmsg`, `code` etc. → silently use defaults; wrong `ok` type → treated as error
- Cursor ID must be BSON `int64` strictly; `int32` causes an immediate parse error
- BSON spec constraints from bsonspec.org are tabulated

**Section 3 — Reconnection behaviour**
- Architecture: dedicated monitoring goroutine per `Server`, separate from application pool
- Heartbeat timing: `heartbeatInterval` (default 10 s) + `minHeartbeatInterval` (500 ms) rate limiter
- New TCP connections are established when `s.conn` is nil, closed, or was previously cancelled
- Exact conditions that clear the pool vs. only mark the server Unknown (the wire-version-42 boundary)
- Single-timeout FAAS-pause exemption (GODRIVER-2577): one timeout → immediate retry, no pool clear; two consecutive timeouts → `clearAll`
- Load-balanced mode differences: no monitoring goroutine, `updateDescription` is a no-op
- Dial error classification: context-timeout dial errors do NOT clear the pool (backpressure labels); non-timeout network errors DO