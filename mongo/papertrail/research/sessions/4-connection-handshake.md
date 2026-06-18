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

### 4. Connection handshake
The exact sequence a driver performs on connect, derived from the driver's 
connection establishment code:
- Which command it sends first (isMaster or hello)
- The exact fields it sends in the request document
- The exact fields it requires in the response document
- Which fields determine wire protocol version negotiation
- What values the server must return for maxWireVersion, maxBsonObjectSize, 
  maxMessageSizeBytes, maxWriteBatchSize, localTime, logicalSessionTimeoutMinutes

The implementation agent will need to return exactly the right fields here 
or the driver will disconnect.

---

Spec written to `./mongo/papertrail/research/04-connection-handshake.md`. Here's a summary of the key findings derived directly from the driver source:

## What the driver sends (request)

**Default path (no serverAPI, not loadBalanced):** `OP_QUERY` to `admin.$cmd` with command name `"isMaster": 1` plus:
- `"helloOk": true`
- `"backpressure": true`
- `"compression": [...]` (array, possibly empty)
- `"client": {...}` — driver/OS/platform metadata, max 512 bytes, **only on first connection, never on heartbeats**

**API version / LB path:** `OP_MSG` with `"hello": 1` instead.

## What the driver requires back (response)

The only strictly mandatory field is `"ok": 1`. Everything else is optional but recommended:

| Field | Why it matters |
|-------|---------------|
| `"ok": 1` | Required — anything else marks server as broken |
| `"maxWireVersion": ≥ 8` | **Critical** — driver refuses servers with maxWireVersion < 8 (MongoDB 4.2 minimum) |
| `"minWireVersion": ≤ 25` | Must not exceed driver's max supported version |
| `"maxBsonObjectSize"` | Fallback is no limit; `16777216` is standard |
| `"maxMessageSizeBytes"` | Fallback is `48000000`; must match actual limit |
| `"maxWriteBatchSize"` | `100000` is standard |
| `"logicalSessionTimeoutMinutes"` | Required for session support; `30` is standard |
| `"localTime"` | **Not parsed at all** — driver ignores it |
| `"connectionId"` | Optional int32/int64 stored as the server connection ID |