# Invar

Invar is a data store that runs as a standalone daemon and supports the Redis wire protocol and a small but growing subset of Redis commands (the authoritative reference of Redis commands can be found on the official website at https://redis.io/docs/latest/commands/set/index.html.md). The project is 100% written in golang. You can see the currently supported list of commands in the integration test cases in `./test/redis-commands.json`.

## Overall structure

Directory layout follows golang packaging norms (it's module-based), save for the integration tests.

- `kv`: vendor-neutral abstraction over LSM-tree-based key-value stores. Gives a single interface to build upon with common minimum consistency and isolation guarantees
- `redis` package: main implementation code for the Redis listener. Relies on github.com/tidwall/redcon for Redis wire command [de]serialization the `kv` module for the actual persistence. This package therefore destructures Redis command data into individual keys that are stored in the kv store, and then looked up & translated back into Redis responses. See the later section about 'redis key structure'.
- `redis/common`: Common utilities for redis connection management, redis-specific key prefixing, tx queuing and command boilerplate
- `mongo` package: experimental. ignore this for now
- `test`: a test suite where Deno boots a test script containing a Redis client, and runs through a set of Redis commands with known expected responses, and evaluates the correctness of what comes back to the client.

## Development

This is a normal Go project. To fetch modules:

`go mod download`

The modules should be periodically updated:

`go get -u ./...`

To build, simply `go build .`. At this time there are no non-standard build flags.

To run, the storage backend is a mandatory subcommand. For example, invoke the resulting executable as `./invar redis badger --data-dir /tmp/badger` to spin up a daemon with BadgerDB listening on `:6379`. The Redis and Mongo protocol subcommands each accept `badger` or `slatedb`:

### Optional SlateDB backend

`kv/slate.go` (and `kv/slate_test.go`) implement the kv interface on top of SlateDB via the `slatedb.io/slatedb-go` bindings. This requires the native `libslatedb_uniffi` shared library and is compiled only when the `slatedb` build tag is set (e.g. `go build -tags slatedb .`). The default build has no SlateDB references and works with the BadgerDB backend only.

The committed `go.mod` intentionally contains no SlateDB entries; the `require`/`replace` directives are injected by the Makefile. To set up the SlateDB prerequisites (clones SlateDB at a pinned tag into `.build/`, builds the release uniffi lib, and wires `go.mod`):

`make deps-slatedb`

Then build invar with the SlateDB backend, run the tagged tests, or run only the BadgerDB tests:

* `make build` — builds `./invar` with `-tags slatedb` and an embedded rpath (no `DYLD_LIBRARY_PATH` needed at runtime)
* `make test` — runs the unit tests with `-tags slatedb`
* `make test-badger` — runs the plain `go test ./...` without SlateDB
* `make clean` — removes `.build/` and drops the injected `go.mod` entries

`make` (default target) runs `deps-slatedb` followed by `build`. `make regen-bindings` regenerates the checked-in Go bindings when the SlateDB tag is bumped (requires `uniffi-bindgen-go`).

Note: `go mod tidy` evaluates all build-tag files, so it needs the SlateDB checkout present (run `make deps-slatedb` first) or it will fail trying to resolve `slatedb.io/slatedb-go`.

Before committing, first run staticcheck to catch code quality regressions:

`./run-staticcheck.sh`

This compares current staticcheck output against the baseline in `.staticcheck.baseline`. Any new issues (not present in the baseline) will cause it to fail. To update the baseline (e.g. after cleaning up an existing issue), run:

`staticcheck ./... > .staticcheck.baseline`

Then ensure all the tests pass as outlined below.

If you have implemented a new command(s), check the `COMPATIBILITY.md` table and update it accordingly.

## Branching strategy

Create a new branch based on latest master for new feature development. Create a PR with a clear description of changes made once you're ready and wait for the checks to pass before merging. Direct pushes to master are blocked.

## Test

* To run all unit tests, run `go test ./...`
* To run the integration tests, run `./run-tests.sh` from the project root

If the invar executable is hanging for some reason (e.g. resource contention, typo causing infinite loop, e.t.c) you can use the `pprof` tool that's built into go as outlined in the [`net/http/pprof`](https://pkg.go.dev/net/http/pprof) docs and the main [pprof](https://github.com/google/pprof) docs to pinpoint execution points in the program. The pprof HTTP handler listens on `localhost:6060`.

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
