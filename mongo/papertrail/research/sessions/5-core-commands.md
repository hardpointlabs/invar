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

### 5. Core command specifications
For each command: the request document structure, required fields, optional 
fields, response document structure, and error response structure. Derive 
these from the driver's operation implementations and test cases.

Commands to cover (at minimum):
- find (including filter, projection, sort, limit, skip, batchSize)
- getMore / killCursors
- insert
- update (including upsert behaviour)
- delete
- aggregate (pipeline structure)
- createIndex / listIndexes / dropIndexes
- listDatabases / listCollections
- drop / create
- ping
- buildInfo (drivers call this)

---

Written to `./mongo/papertrail/research/05-core-command-specifications.md`. Here is a summary of what was produced:

---

## What the spec covers

The file derives everything from the driver source at `v2.6.1` commit `685cf13`. It contains:

**Provenance block** — all 23 URLs fetched, in order.

**Per-command tables** for each required command derived from the `command()` builder and `buildXResult()` parser in the driver source:

| Command | Source file |
|---|---|
| `find` | `operation/find.go` |
| `getMore` / `killCursors` | `batch_cursor.go` |
| `insert` | `mongo/insert.go` |
| `update` (upsert included) | `operation/update.go` |
| `delete` | `operation/delete.go` |
| `aggregate` | `operation/aggregate.go` |
| `createIndexes` | `operation/create_indexes.go` |
| `listIndexes` | `operation/list_indexes.go` |
| `dropIndexes` | `operation/drop_indexes.go` |
| `listDatabases` | `operation/list_databases.go` |
| `listCollections` | `operation/list_collections.go` |
| `drop` (dropCollection) | `operation/drop_collection.go` |
| `dropDatabase` | `operation/drop_database.go` |
| `create` | `operation/create.go` |
| `ping` | generic `Command` operation |
| `buildInfo` | generic `Command` operation |
| `findAndModify` | `operation/find_and_modify.go` |

**Key behavioural details captured:**
- OP_MSG document sequences (identifier names `"documents"`, `"updates"`, `"deletes"`) vs body documents
- `cursor` sub-document structure (`firstBatch` vs `nextBatch` for getMore)
- Exact field names parsed (`showRecordId` not `showRecordID`; `timeseries` not `timeSeries`)
- Wire version gates for each optional field
- Batch splitting semantics for multi-document write commands
- Error response structure including `writeErrors`, `writeConcernError`, and `errorLabels`
- Retryable error codes and special codes (391 reauthentication, 50 MaxTimeMSExpired, etc.)
- Framework-appended fields (`$db`, `$readPreference`, session fields, `writeConcern`, `readConcern`, `maxTimeMS`)