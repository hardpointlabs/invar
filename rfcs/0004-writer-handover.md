# Writer handover for rolling updates

Status: Draft

Authors:

* Chris Mowforth


# Summary

Invar allows one writer per store. Upgrading it today means stopping the writer and starting the new release in its
place, so the store is unavailable until the new process has opened SlateDB, and a release that cannot start leaves
nothing serving the store at all. This RFC proposes a *writer handover*: a second Invar process, running the new
release, starts beside the writer as a read-only follower, proves that it can open and read the store, and then takes
over as writer in a short, coordinated switch. SlateDB's writer fencing already guarantees that at most one process
can commit at any moment. This RFC adds what is needed around it: from the moment a handover starts, a write is only
acknowledged once it is durable, so no acknowledged write can be lost in the switch; a restarted process can always
tell whether it is still the writer; and a failed handover leaves the old writer in charge. The same durable
acknowledgements apply during a graceful shutdown, so a plain restart for an upgrade gets the same protection.

Invar provides the building blocks: process roles, a small admin HTTP interface and a boot rule. Deciding when to
start a handover, starting and stopping processes, and moving client traffic belong to whatever runs Invar, here
called the *orchestrator*. That can be a deploy script, a Kubernetes operator or any other tool. Nothing in this RFC
depends on a particular one.

# Motivation

* **Bad releases should fail before they take anything down.** When a stopped writer is replaced in place, a release
  that cannot open the store (a wrong flag, a crash on startup, an incompatible format) is only discovered after the
  working process has gone. With a handover, the new release has to open the store as a reader before the old writer
  is asked to do anything.
* **Shorter write outages.** An in-place replacement is unavailable for a full restart: scheduling, image pull,
  process start and opening SlateDB. A handover is unavailable for a flush, a WAL catch-up and opening the writer in a
  process that is already running.
* **Works with rollout tools that already exist.** Rolling update mechanisms generally start the new version, wait
  for it to report ready, then stop the old one. A writer-eligible node reports ready only while it is the writer, so
  such a mechanism can follow a handover instead of racing it.
* **No lost acknowledgements in planned events.** `DbTransaction::commit` in SlateDB 0.18 returns before the write is
  durable in object storage; durability follows on the next WAL flush (`SLATEDB_FLUSH_INTERVAL`). In normal operation
  that is a deliberate trade-off, and `WAIT`/`WAITAOF` exist for clients that need more. A handover or an upgrade is
  a planned event, though, and a planned event should never lose a write a client was told had succeeded. So while a
  handover or a graceful shutdown is in progress, every commit waits until it is durable before it is acknowledged,
  using the mechanism `WAIT` already uses. Writes get slower for that window; none are acknowledged and then lost.
  This lives in the `kv` crate, below any wire protocol, so every protocol built on `kv` gets it without having to
  manage acknowledgements itself.

## What this RFC does not change

* **Durability of acknowledged writes in normal operation.** Outside a handover or shutdown, commits are acknowledged
  as they are today, and a crash can still lose writes acknowledged within the last flush interval. Hardware failure
  is out of scope.
* **Unplanned failover.** If the writer's host fails, recovery is whatever it is today: restart the process. This
  RFC only covers a planned switch between two healthy processes. The boot rule it introduces does make restarts
  safer (see *Boot rule*).
* **Read replicas.** The follower serves no client traffic. Read replicas (nodes that stay read-only and serve
  reads indefinitely) are expected to follow in a separate RFC. This RFC does not add them, but it defines readiness,
  the boot rule and write rejection so that they can be added without changing any of those (see *Read replicas*).
* **Moving client traffic.** Invar does not route clients. It reports its role through `/readyz` and closes
  connections at the right moment; the deployment's load balancer, service discovery or proxy does the rest.

# Guide Implementation

## Concepts

**Node.** A running Invar process that can take part in a handover, identified by a **node ID**: any name that is
stable across restarts of that process and unique among the processes serving one store (a pod name, a host name, a
VM ID). Handover support is enabled by giving Invar a node ID. Without one, Invar behaves exactly as it does today.

**Writer record.** A reserved key, `!invar:writer`, holding the node ID of the process that most recently opened the
store as writer. A writer writes it, durably, immediately after opening. Because it lives in the store, it is
protected by the same fencing as the data: a process that has been fenced cannot update it.

**Handover marker.** A reserved key, `!invar:handover`, written durably by the old writer as its last write. Its
value is the handover ID. When the follower can read the marker, it can read every write the old writer
acknowledged.

**Handover ID.** An opaque string chosen by the orchestrator for one attempt. It ties the drain, the marker and the
promotion together, so that a request from an abandoned attempt cannot affect a later one.

**Acknowledgement level.** A store-wide setting in the `kv` crate that decides when a commit returns. *Buffered* (the
default, and today's only behaviour) returns once the write is committed in memory. *Durable* returns only once the
write would survive the process dying. A writer switches to Durable when a handover starts or a graceful shutdown
begins.

Neither reserved key is visible to Redis clients. User keys start with a database number and internal keys with `-`
or `+`, so `!` cannot collide, and commands that enumerate keys (`KEYS`, `DBSIZE`) iterate only the current database's
prefix. Any future command that enumerates keys must also stay within a database prefix.

## Roles

Each node is in exactly one role.

| Role | Store opened as | Clients | Moves to |
|---|---|---|---|
| **Writer** | `Db` | served normally | Draining |
| **Draining** | `Db` | write transactions rejected; reads served | HandedOff, or Writer on abort |
| **HandedOff** | `Db` (fenced, or about to be) | all connections closed, listeners stopped | Writer on abort |
| **Follower** | `DbReader` | none (listeners stopped) | Writer, by promotion |
| **Standby** | `DbReader` | none (listeners stopped) | none: waits to be stopped |
| **Replica** *(future RFC)* | `DbReader` | reads served; writes rejected | none: never promoted |

`/livez` is 200 in every role. `/readyz` is 200 when the node is serving clients **in the role it was started
for**. A writer-eligible node (every node in this RFC) is ready only as Writer; a replica will be ready while it is
serving reads. Readiness therefore stays a single yes or no per process, which is what load balancers and probes
consume, and deployments that run both kinds of node route them as separate groups.

## A handover, end to end

Node A is the writer. The orchestrator wants node B, running the new release, to take over.

1. **Prepare A**: `POST /handover/prepare?id=H` on A. A stays the Writer and keeps serving everything, but switches
   to Durable acknowledgements: from now on, a client is only told a write succeeded once it is in object storage.
   Writes get slower (each waits for the next WAL flush); nothing else changes.
2. **Start B as a follower**, with its own node ID and `--handover-id H`. B opens a `DbReader`, reads the writer
   record (which names A) and becomes a Follower. If the new release cannot open or read the store, it fails here,
   and A has not been touched. The orchestrator waits for `GET /node` on B to report `follower`.
3. **Drain A**: `POST /handover/drain?id=H` on A. A rejects new write transactions with `-READONLY` and lets in-flight
   ones finish; each is acknowledged only once durable. It then writes the handover marker, flushes, and moves to
   HandedOff, closing every client connection and refusing new ones. Clients reconnect and retry.
4. **Promote B**: `POST /handover/promote?id=H` on B. Once B's reader can see the marker, which proves B can read A's
   last write, B closes the reader and opens the store as writer. This fences A in SlateDB. B then writes the writer
   record durably and moves to Writer, with Buffered acknowledgements. Its `/readyz` turns 200.
5. **Move traffic to B** and stop A. In a deployment that routes only to ready instances, this happens on its own:
   A's readiness failed at step 3 and B's succeeded at step 4.

Prepare is a separate step so that the slower, durable phase can begin before anything else happens, and so that it
covers the time spent starting B. Drain also switches to Durable acknowledgements, so an orchestrator that skips
prepare loses the earlier coverage, not the guarantee during the switch. The orchestrator may call promote before
drain; promote simply waits for the marker.

Clients of the writer see slower writes from step 1, and a short period of errors and reconnects between steps 3
and 5, while nothing serves them. They never see a stale read: A stops serving reads before B can accept a write.
Readers that are not taking part in the handover, such as future replicas, are unaffected: they follow the store,
not a particular writer, and keep serving reads through the write outage.

## When things go wrong

* **B fails before step 3** (bad release, bad configuration, cannot open the store): A is still the writer. The
  orchestrator stops B and calls `POST /handover/abort?id=H` on A, which returns A to Buffered acknowledgements.
* **B fails between steps 3 and 4**: A is HandedOff and nobody is writing. The orchestrator stops B, makes sure it
  cannot start again, then calls `POST /handover/abort?id=H` on A. A reopens the store as writer, rewrites the writer
  record and resumes with Buffered acknowledgements. Writes were unavailable until the abort.
* **A is fenced unexpectedly while Durable** (for example, something opens the store as writer outside this
  protocol): any write A has not yet acknowledged fails with an error instead of being acknowledged and lost. The
  client cannot tell whether it was applied, which is the usual outcome of a failed commit, but it is never told
  "OK" for a write that is gone.
* **A restarts after step 4**: it reads the writer record, finds B, and comes up as Standby instead of reopening as
  writer and fencing B.
* **B restarts after step 4**: it finds itself in the writer record and reopens as Writer.
* **The orchestrator disappears after prepare**: A stays Durable, which is slower but safe, until a later abort or
  handover.

## Boot rule

A node decides its role at startup from the writer record, never by default. The first clause takes precedence over
the rest:

* the node was started as a replica → **Replica**, whatever the record says, including when there is none. A replica
  never opens the store as writer and never writes the writer record.
* the record names this node → **Writer**
* the record names another node, and `--handover-id` is set → **Follower**
* the record names another node, and `--handover-id` is not set → **Standby**
* there is no record (a new store, or one last written by an older release) → **Writer**, unless `--handover-id` is
  set, in which case **Follower**

The last rule allows handover support to be adopted with one ordinary restart: the upgraded writer finds no record,
opens as writer and writes one.

## Example: Kubernetes

As an illustration only, not something this RFC ships: run Invar with the pod name as its node ID through the downward
API, and a Service that selects on readiness. A rolling update with one extra pod and no unavailable pods starts the
new pod, which comes up as a Follower and is not ready. Kubernetes waits for it to become ready, which only happens
after promotion. Something still has to call prepare, drain and promote, and that component is not part of Kubernetes
or of Invar. It could be an operator, a job run by the deploy pipeline, or a container in the new pod that drives
steps 3 and 4 (with prepare called by whatever starts the rollout). The handover ID must reach the new pod, for
example through a pod annotation or the rollout's revision.

# Reference Implementation

## Configuration

| Flag | Env | Purpose |
|---|---|---|
| `--node-id` | `INVAR_NODE_ID` | Enables the roles, boot rule and writer record. Unset: today's behaviour. |
| `--handover-id` | `INVAR_HANDOVER_ID` | Start as a handover target for this attempt. Requires `--node-id`. |
| `--admin-addr` | `INVAR_ADMIN_ADDR` | Listen address for the admin endpoints. Unset: no admin listener. |

Handover requires the SlateDB backend. With Fjall or the in-memory store, `--node-id` and `--handover-id` are
rejected at startup.

## Opening the store

`SlateDb` gains a reader variant. Both variants are built with the same Foyer hybrid cache and `db_cache_id`. SlateDB
0.18's `DbReaderBuilder::with_db_cache` accepts the cache `Db` already uses, so a promotion keeps what is in it.
Two *processes* must not share an on-disk cache directory: the follower is a separate process, so it is given its own
`--cache-path`.

The follower's `DbReaderOptions::manifest_poll_interval` is short (for example 250ms) and independent of the writer's
`SLATEDB_MANIFEST_POLL_INTERVAL`. The reader polls the WAL at this interval, so it bounds how long promotion takes to
see the marker, and therefore the length of the outage.

The reader uses `DbReaderMode::ManagedCheckpoint` (SlateDB's default), so the writer's garbage collector cannot delete
files the reader is still using.

## Admin endpoints

These are served on `--admin-addr`. Every mutating endpoint is idempotent for a given handover ID: repeating a request
returns the result of the first. The handover endpoints are refused with 409 on a replica.

* `GET /node` → `{role, node_id, handover_id, writer, acks}`. The name is deliberately not handover-specific, so that
  it can report any role, including a replica's. `acks` is `buffered` or `durable`.
* `POST /handover/prepare?id=H` (Writer, role unchanged). Switches the store to Durable acknowledgements and records
  H as the handover in progress. Responds once every commit returning from then on is guaranteed to wait for
  durability. A prepare with a different ID while one is in progress is refused with 409.
* `POST /handover/drain?id=H` (Writer → Draining → HandedOff). Switches to Durable acknowledgements if prepare was
  skipped, rejects new write transactions, waits for in-flight ones, writes `!invar:handover = H`, calls `Db::flush`,
  waits until `durable_seq` covers the marker, closes all client connections and stops the client listeners. Responds
  once HandedOff. A drain with a different ID while one is in progress or completed is refused with 409.
* `POST /handover/promote?id=H` (Follower → Writer). Responds 202 at once; progress is read from `GET /node`.
  Waits until the reader reads `!invar:handover = H`, closes the reader, opens `Db`, writes
  `!invar:writer = <node id>`, waits for it to be durable, then starts the client listeners. Refused with 409 if this
  node was started with a different handover ID.
* `POST /handover/abort?id=H`. On a prepared Writer that has not drained, it returns the store to Buffered
  acknowledgements and clears the handover; nothing else changes. On a Draining or HandedOff node it closes and
  reopens `Db` (which re-fences), rewrites the writer record durably, clears the handover, returns to Buffered
  acknowledgements and restarts the client listeners. **The second case is only safe once the orchestrator knows the
  handover target cannot open the store**; see *Abort*.

## Durable acknowledgements

The acknowledgement level belongs to the `kv` crate, not to any wire protocol, so that every front end built on `kv`
gets it without managing acknowledgements itself.

* **Interface.** `KeyValueStore` gains `set_ack_level(AckLevel)` and `ack_level()`, with `AckLevel::Buffered` (the
  default) and `AckLevel::Durable`. When the level is Durable, `Tx::commit`, and `update` through it, return only once
  the write is durable. The level is read after the backend's commit has succeeded and just before returning, so
  every commit that returns after `set_ack_level(Durable)` has completed waits, including commits that started
  before it. A front end that offers per-request durability (as `WAIT` does) can ask for more than the store's level,
  never less.
* **SlateDB.** The wait is the one `await_until` already performs for `WAIT`: subscribe to `DbStatus` and wait until
  `durable_seq` reaches the commit's sequence number. The two share one helper, which also wakes when
  `close_reason` is set and returns an error rather than waiting on. That fixes an existing problem: when a `Db` is
  fenced, SlateDB keeps the status channel open and only sets `close_reason`, so today `WAIT 0` on a fenced writer
  waits forever. A Durable commit on a fenced writer instead fails with the new `Error::Fenced`, and the client gets
  an error instead of an acknowledgement for a write that will never be durable.
* **Latency.** A Durable commit waits for the next WAL flush, up to `SLATEDB_FLUSH_INTERVAL`. To keep that bounded
  without changing SlateDB's settings at runtime, Invar calls `Db::flush` on a short timer (for example every 50ms)
  while the level is Durable and writes are pending. That writes more, smaller WAL objects for the length of the
  handover.
* **Fjall.** Durable means the commit's journal entry has been synced to disk before `commit` returns. Fjall takes no
  part in handover, but graceful shutdown uses the level with every backend.

## Graceful shutdown

Shutdown is the other planned event, and the one every in-place upgrade goes through:

1. **SIGTERM is handled like SIGINT.** Today Invar only listens for `ctrl_c` (SIGINT), but process supervisors and
   container runtimes stop processes with SIGTERM, which currently kills Invar without closing the store. Any write
   acknowledged within the last flush interval is lost on every ordinary restart.
2. **The first thing shutdown does is switch to Durable acknowledgements.** Commits still in flight then return only
   once durable.
3. The listeners stop and the store closes, as today. `Db::close` flushes, so writes acknowledged before the switch
   are persisted too, unless closing exceeds `--shutdown-timeout`, in which case Invar logs an error saying so.

This applies with or without a node ID, so it covers deployments that never use handover.

## Rejecting writes while draining

The drain gate lives in the SlateDB store, in two places:

* **`begin` with `mutating = true`** returns a new `Error::ReadOnly`. Commands already declare whether they write
  (`is_mutating` on each op, `true` for `EVAL`), so most writes are refused before doing any work.
* **`commit` of a transaction that wrote anything** also returns `Error::ReadOnly`. This backstop is what makes the
  guarantee hold: `SlateDb::begin` ignores the `mutating` flag today, so a transaction opened as non-mutating can
  still put or delete. `SlateTx` records whether it has written, and a commit with writes is refused while draining.
  `destroy` and `drop_prefix` (`FLUSHALL`, `FLUSHDB`), which open their own transactions, are gated the same way.

The RESP layer maps `Error::ReadOnly` to `-READONLY You can't write against a read only replica.`, the error Redis
replicas return. The same error will be what a replica returns for every write, so clients see one behaviour for "this
node does not accept writes", whatever the reason. Clients can be configured to reconnect on it (ioredis documents
exactly this case for `reconnectOnError`). The HTTP listener (RFC 0003), when enabled, returns the same error in its
JSON error body and closes keep-alive connections when HandedOff.

## Why HandedOff closes connections instead of serving reads

Once the marker is durable, the follower may promote and start accepting writes at any moment, and the old writer
has no cheap way to know when. Any read it served after that point could be stale. Closing connections at HandedOff
is the simplest rule that rules out stale reads. It also means anything relaying bytes between a client and Invar
needs no knowledge of the protocol: when Invar closes its end, the relay closes the client's.

## Fencing

SlateDB reports fencing as `ErrorKind::Closed(CloseReason::Fenced)`, which `map_slate_error` currently folds into
`Error::Undefined`. It gets its own variant. A node that sees `Fenced` in any role moves to Standby: it closes client
connections and stops the client listeners. It does not exit, because a process supervisor would restart it, and it
must not reopen as writer. After a restart, the boot rule decides.

## Reserved keys and `FLUSHALL`

`FLUSHALL` calls `KeyValueStore::destroy`, which deletes every key in the store, including the writer record.
`destroy` skips the `!invar:` prefix. `FLUSHDB` uses `drop_prefix` on database prefixes and is unaffected.

## Abort

After a drain, `abort` reopens the store rather than simply lifting the drain gate. Once back at Buffered
acknowledgements, `commit` acknowledges writes before they are durable, so a writer that resumed while another process
had opened the store would acknowledge writes and then lose them when its next WAL flush was fenced. Reopening fences
any such process first, so every write acknowledged after an abort reaches a store owned by the aborting node.

Even so, the orchestrator must only abort once the target is stopped and cannot restart. Otherwise the target could
come back, and the two nodes would take turns fencing each other until one of them was stopped.

## Readers across a change of writer

A `DbReader` must keep reading correctly when the process writing the store changes: the new writer opens with a
higher writer epoch and continues the WAL and the manifest from where the old one left off. This RFC relies on it
only briefly (the follower reads until it promotes), but future replicas will depend on it for their whole life. The
implementation includes a test in which a reader keeps reading while one writer hands over to another, and checks that
it sees every write made before and after the switch.

## Read replicas

Replicas are left to their own RFC. This one fixes the rules they must fit into, so that adding them changes none of
the behaviour defined here:

* **Startup:** a replica takes the first clause of the boot rule. It never opens the store as writer and never writes
  the writer record, even for a store that has none. Otherwise a replica started against a new store would become
  its writer.
* **Readiness:** ready while serving reads, under the per-role definition in *Roles*.
* **Writes:** rejected with `Error::ReadOnly`, as on a draining writer.
* **Handover:** `prepare`, `drain`, `promote` and `abort` are refused. A replica is never a handover target, and a
  handover does not interrupt it.

The replica RFC also has to settle a question this one does not need to: consistent multi-key reads. Every Invar read
runs in a snapshot transaction today, so a command that reads several keys (a hash sentinel and its fields, a list's
nodes, `MULTI`, Lua) sees one point in time. In SlateDB 0.18.1 only `Db` offers snapshots: `DbReader` has `get` and
`scan`, and `ReadOptions` cannot pin a read to a sequence number, so a multi-key command on a replica could straddle a
refresh and see a partly applied write. The follower in this RFC reads only the two reserved keys, so it is not
affected.

## Security

The admin endpoints can stop a store's writer. Like the RESP listener and the HTTP listener of RFC 0003, they are
unauthenticated: `--admin-addr` is unset by default, and when set it should be bound to loopback or reachable only
by the orchestrator. Invar logs a warning at startup when it is bound to a non-loopback address.

The metrics and probe listener stays `GET`/`HEAD` only; handover is never controlled through it.

# Drawbacks

* **Not zero downtime.** Clients see a short outage: the drain, the follower's WAL poll, opening the writer, and the
  deployment noticing the change in readiness. The gain over an in-place restart is a shorter outage and a release
  that is proven to start before anything is stopped, not continuous availability.
* **Slower writes during a handover and at shutdown.** Every write waits for durability, a flush away. This is a
  deliberate trade: a temporary slowdown in exchange for never losing an acknowledged write in a planned event.
* **Needs an orchestrator.** Invar cannot hand over on its own, and most rollout tools will not call prepare, drain
  and promote without extra glue.
* **More moving parts in Invar.** Two reserved keys, a new listener, five roles, and a second way of opening the
  store.
* **A failed handover costs write availability** until the orchestrator aborts, because the old writer cannot safely
  resume while the target might still open the store.
* **Two processes for the length of a handover**, each with its own cache.

# Rationale / Alternatives

* **Rely on SlateDB fencing alone** (start the new process as writer). It would fence the old writer at once, before
  the new release had proven it could serve, and writes the old writer had acknowledged but not flushed would be lost.
* **Keep the writer record outside the store** (a coordination service, or a separate object in the bucket written
  with a conditional put). An external service becomes a dependency of every startup, and can disagree with the
  store's own fencing. A separate object duplicates a compare-and-set protocol SlateDB already provides and needs its
  own handling in `FLUSHALL`. A record inside the store cannot disagree with the store.
* **Leases** (the writer renews a lease; others promote when it expires). That would turn a planned handover into an
  automatic failover, which needs failure detection, timeouts tuned to object storage latency, and a story for
  writes acknowledged before a pause. It is a bigger feature than a rolling update needs.
* **Let nodes coordinate among themselves** (the follower contacts the writer directly). This needs peer discovery and
  peer authentication, both of which depend on the environment. An HTTP interface called by the orchestrator leaves
  those choices to the deployment, which can still build peer-to-peer coordination on top of it.
* **Only flush at drain, without Durable acknowledgements.** The planned path loses nothing either way, because the
  drain flushes before the marker. Durable acknowledgements protect the cases outside it: the old writer being fenced
  or killed mid-handover, or an orchestrator mistake. Without them, those cases lose writes clients were told had
  succeeded.
* **Durable acknowledgements in each wire protocol** (for example, have the RESP layer wait on every write while a
  handover is in progress). Every protocol built on `kv` would need its own version. Putting it in `kv` gives them all
  the same guarantee.
* **Durable acknowledgements all the time.** Safest, but makes every write wait for a flush in normal operation, a
  much larger change in Invar's performance than this RFC needs. The `AckLevel` interface leaves room to offer it as
  an option later.
* **Serve stale-but-bounded reads from the old writer during the switch.** It shortens the read outage, but makes
  reads after a handover depend on timing. The RFC favours correctness.

# Unresolved Questions

> **Open Question:** Should the follower warm the cache before a drain is requested? A `DbReader` that serves no
> reads warms nothing, so the follower phase is a pre-flight check and a catch-up mechanism, not a cache warmer.
> Prefetching recently used data is possible but needs a policy for what to fetch.

> **Open Question:** Should a Durable commit have a timeout? Without one, a write waits as long as SlateDB keeps
> retrying object storage; with one, the client gets an error whose outcome is unknown. And what flush interval
> should the Durable timer use by default?

> **Open Question:** A writer accepts the first prepare or drain request it receives, for any handover ID. If two
> orchestrators (or a retried, abandoned attempt) both start handovers, one of them will fail and need an abort. Is it
> enough to document that the orchestrator must serialize handovers per store, or should Invar enforce something
> stronger, for example a drain that names the expected follower's node ID?

> **Open Question:** Should `promote` time out on its own if the marker never appears, or wait until it is told to
> stop? Waiting is simpler, but leaves a follower that looks healthy while nothing is happening.

> **Open Question (for the replica RFC):** How should replicas get consistent multi-key reads? Candidates are a
> snapshot API on `DbReader` contributed to SlateDB, rotating readers pinned to checkpoints
> (`DbReaderMode::Checkpoint`, consistent per reader but staler), or limiting replicas to single-key reads. The same
> RFC should also cover staleness guarantees (replicas see only durable writes, so there is no read-your-writes) and
> the cost of each long-lived reader keeping its own managed checkpoint, which delays garbage collection if a replica
> dies without releasing it.
