# KV Project overview

Hardpoint KV is a data store that runs as a standalone daemon and supports the Redis wire protocol and a small but growing subset of Redis commands (the authoritative reference of Redis commands can be found on the official website at https://redis.io/docs/latest/commands/set/index.html.md). The project is 100% written in golang. You can see the currently supported list of commands in the integration test cases in `./test/redis-commands.json`.

## Overall structure

Directory layout follows golang packaging norms (it's module-based), save for the integration tests.

- `core` package: core types
- `redis` package: main implementation code for the Redis listener. Relies on github.com/tidwall/redcon for Redis wire command [de]serialization and BadgerDB (https://github.com/dgraph-io/badger) for the actual long term persistence. This package therefore destructures Redis command data into individual keys that are stored in BadgerDB, and then looked up & translated back into Redis responses. See the later section about 'redis key structure'.
- `mongo` package: experimental. ignore this for now
- `test`: a test suite where Deno boots a test script containing a Redis client, and runs through a set of Redis commands with known expected responses, and evaluates the correctness of what comes back to the client.

## Development

This is a normal Go project. To fetch modules:

`go mod download`

The modules should be periodically updated:

`go get -u ./...`

To build, simply `go build .`. At this time there are no non-standard build flags.

To run, simply invoke the resulting executable: `./kv` (which will spin up a daemon listening on `:6379`)

Before committing, run staticcheck to catch code quality regressions:

`./run-staticcheck.sh`

This compares current staticcheck output against the baseline in `.staticcheck.baseline`. Any new issues (not present in the baseline) will cause it to fail. To update the baseline (e.g. after cleaning up an existing issue), run:

`staticcheck ./... > .staticcheck.baseline`

## Test

* To run all unit tests, run `go test ./...`
* To run the integration tests, run `./run-tests.sh` from the project root

## Redis key structure

Redis has a couple of concepts that don't map naturally to BadgerDB's flat keyspace:

* What redis calls a `db`, which to all intents and purposes is a namespace
* Compound data structures: lists, sets, e.t.c which under the hood will be represented by multiple keys which contain a combination of user-provided data as well as internal references to other keys

The naming convention for keys is as follows:

For public keys (i.e. user-accessible keys):

"<current DB>:keyname"

For internal (i.e. non-user-accessible keys):

"-<current DB>:keyname:rest..."

For the semantics of how internal keys reference each other, see inline comments (e.g. for the linked list implementation in list.go)
