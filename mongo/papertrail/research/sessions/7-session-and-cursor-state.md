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

### 7. Session and cursor state
What server-side state the driver expects to exist between requests:
- Cursor lifecycle (what cursorID means, when it becomes invalid)
- Logical session IDs if present in the 3.6 driver
- Which commands require a cursor response vs inline results

---

Written to `/Users/chris/hardpoint/kv/mongo/papertrail/research/07-session-cursor-state.md`.

---

## What the spec covers

**Sources fetched (in order):**
1. `batch_cursor.go` — cursor lifecycle, `getMore`/`killCursors` dispatch, load-balanced pinning, batch-size arithmetic
2. `session/client_session.go` — `Client` struct, transaction FSM, causal consistency, snapshot time, cluster-time/operation-time advancement, recovery token
3. `session/server_session.go` — server session struct, UUID generation, dirtiness, expiry predicate
4. `session/session_pool.go` — LIFO pool, expiry on checkout/return, dirty-session discard
5. `session/session_pool_test.go` — confirms LIFO ordering and expiry removal
6. `session/cluster_clock.go` — global cluster clock, `MaxClusterTime` comparison
7. `session/client_session_test.go` — confirms transaction state machine transitions and causal consistency defaults
8. `session/server_session_test.go` — confirms expiry logic in normal and load-balanced modes
9. `operation.go` (key sections via grep/sed) — `addSession`, `addClusterTime`, `updateClusterTimes`, `updateOperationTime`, `sessionsSupported`, `retryWritesSupported`
10. `mongo/cursor.go` — implicit session lifecycle tied to cursor exhaustion
11. `bsonspec.org/spec.html` — BSON Timestamp type (`0x11`) confirmation

**Key findings:**
- `cursor.id` **must** be BSON `int64` — `Int64OK()` is called with no fallback
- `cursor.id == 0` means exhausted; driver never issues another `getMore`
- `lsid` = `{"id": BinData(4, 16-byte UUID v4)}` embedded document
- `sessionsSupported` is true as soon as wire version is known (`!= nil`)
- `retryWritesSupported` additionally requires `SessionTimeoutMinutes != nil` AND server is not standalone
- `$clusterTime` and `operationTime` are silently ignored if absent from responses
- Only `find`, `aggregate`, `listCollections`, `listIndexes` return cursor responses; everything else is inline
- The session pool is LIFO; dirty sessions are discarded not returned