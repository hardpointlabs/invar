# Invar

Invar is a lightweight document DB which supports the Redis wire protocol and a small but growing subset of Redis
commands. It uses LSM-trees for persistence and supports multiple implementations to facilitate local development (with
Fjall and real production deployment with SlateDB).

Invar's focus is on operational simplicity and durability, not outright performance. The choice of SlateDB was
deliberate: offer a document DB that Redis clients can speak to, but has well-defined transactional guarantees without
the operational complexity of managing a normal stateful workload.

Invar only supports single-writers per key-value store; it's designed to be light enough to be scaled horizontally,
with many instances + key-value stores running in parallel as discrete datasets.

## Global requirements

* Correctness comes before everything: this is not an in-memory data store; the loss of unintended modification of data is never acceptable
* Minimise code churn: avoid refactorings unrelated to the task at hand, and if significant amount of adjacent code is being churned, ask the user what to do
* Do not speculate on public API behavior: if some functionality is not clear, ask the user rather than guessing.

## Tech stack

The project is a Cargo-based Rust project. A legacy golang implementation is present and can be used for reference; it
is scheduled for removal. While the Rust project diverges in idiomatic language/framework-specific ways such as
threading model, it preserves the key layout of the original implementation.

## Overall structure

Directory layout follows Cargo packaging norms, save for the integration tests, which use Deno/TypeScript and some shell
scripting.

- `./src`: implementation source
- `./src/kv`: vendor-neutral abstraction over LSM-tree-based key-value stores. Gives a single interface to build upon with common minimum consistency and isolation guarantees
- `./src/redis` package: main implementation code for the Redis listener. Relies on github.com/tidwall/redcon for Redis wire command [de]serialization the `kv` module for the actual persistence. This package therefore destructures Redis command data into individual keys that are stored in the kv store, and then looked up & translated back into Redis responses. See the later section about 'redis key structure'.
- `test`: a test suite where Deno boots a test script containing a Redis client, and runs through a set of Redis commands with known expected responses, and evaluates the correctness of what comes back to the client.

## Development

This is a normal Rust project. To build:

`cargo build`

To run, the storage backend is a mandatory subcommand. For example, invoke the resulting executable as `./target/debug/invar --backend fjall --path /tmp/invar --redis` to spin up a daemon with BadgerDB listening on `:6379`. The Redis and Mongo protocol subcommands each accept `badger` or `slatedb`:

## Branching strategy

Create a new branch based on latest master for new feature development. Create a PR with a clear description of changes made once you're ready and wait for the checks to pass before merging. Direct pushes to master are blocked.

## Test

* To run all Rust-based unit tests, run `cargo test --workspace`
* To run al the unit tests + the integration tests, run `./run-tests.sh` from the project root

If the invar executable is hanging for some reason (e.g. resource contention, typo causing infinite loop, e.t.c) you can use the `pprof` tool that's built into go as outlined in the [`net/http/pprof`](https://pkg.go.dev/net/http/pprof) docs and the main [pprof](https://github.com/google/pprof) docs to pinpoint execution points in the program. The pprof HTTP handler listens on `localhost:6060`.

## Redis key structure

Redis has a couple of concepts that don't map naturally to an LSM tree's flat keyspace, such as:

* What redis calls a `db`, which to all intents and purposes is a namespace
* Compound data structures: lists, sets, e.t.c which under the hood will be represented by multiple keys which contain a combination of user-provided data as well as internal references to other keys

The layout for keys is as follows:

For public keys (i.e. user-accessible keys):

`<current DB>:<keyname>`

For internal (i.e. non-user-accessible keys):

`-<current DB>:<keyname>:<rest...>`

For the semantics of how internal keys reference each other, see inline comments (e.g. for the linked list implementation in `./src/redis/list.rs`)
