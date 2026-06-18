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

### 3. Wire protocol message structure
Target: wire protocol version 6 (MongoDB 3.6). 

For every message type, specify:
- The MsgHeader structure (messageLength, requestID, responseTo, opCode) 
  with field sizes and endianness
- The opCode numeric value
- All opcodes present in the driver source: OP_MSG (2013), OP_COMPRESSED (2012),
  OP_QUERY (2004), OP_REPLY (1), and any others found in the driver

Primary focus should be OP_MSG as it is the modern opcode used by 3.6+ drivers.
Document OP_QUERY and OP_REPLY as legacy but include them since drivers may 
fall back to them.

For OP_MSG specifically: document the flagBits field, the Section structure 
(kind 0 and kind 1), and the checksum behaviour.

---

Written to `mongo/papertrail/research/wire-protocol-messages.md`. Here is a summary of what it covers:

---

### Provenance block
Lists 12 URLs fetched in order, with purpose annotations for each.

### Sections produced

**§1 Overview** — byte order (little-endian, confirmed with test vectors), 16 MiB max message size, TCP stream framing.

**§2 MsgHeader** — exact 16-byte layout with offsets (`messageLength` at 0, `requestID` at 4, `responseTo` at 8, `opCode` at 12), field semantics, `AppendHeaderStart`/`ReadHeader` source, and byte-exact test vector (`OpMsg=2013` → `DD 07 00 00`; `OpQuery=2004` → `D4 07 00 00`).

**§3 OpCode Registry** — all 13 constants from the driver with numeric values and deprecation status.

**§4 OP_MSG (2013)** — full wire layout; `flagBits` uint32 with all three bits (`ChecksumPresent=1`, `MoreToCome=2`, `ExhaustAllowed=65536`); Kind 0 (body) and Kind 1 (document sequence) sections with exact parse logic and test vector; checksum position and algorithm; `MoreToCome` detection; unacknowledged write semantics; minimum message size (26 bytes).

**§5 OP_COMPRESSED (2012)** — exact layout (originalOpcode+uncompressedSize+compressorID+data); all four compressor IDs (NoOp/Snappy/ZLib/Zstd) with libraries and default levels; decompression flow.

**§6 OP_QUERY (2004)** — layout; `QueryFlag` bitmask; exactly when the driver uses it (legacy handshake only when wire version unknown).

**§7 OP_REPLY (1)** — layout (cursorID is int64); `ReplyFlag` bitmask; document parse loop with infinite-loop guard; byte-exact `MakeReply` construction.

**§8 Other opcodes** — OP_UPDATE/INSERT/GET_MORE/DELETE/KILL_CURSORS layouts with field names and types.

**§9–10 Lifecycle + Streaming** — request/response flow pseudocode; `moreToCome` streaming protocol sequence.

**§11–12 Size constraints and encoding quick reference tables.**