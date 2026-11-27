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
can commit at any moment. This RFC adds what is needed around it: no acknowledged write is lost in the switch, a
restarted process can always tell whether it is still the writer, and a failed handover leaves the old writer in
charge.

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
  for it to report ready, then stop the old one. Invar's readiness probe reports ready only in the writer role, so
  such a mechanism can follow a handover instead of racing it.

## What this RFC does not change

* **Durability of acknowledged writes in normal operation.** `DbTransaction::commit` in SlateDB 0.18 returns before
  the write is durable in object storage. Durability follows on the next WAL flush (`SLATEDB_FLUSH_INTERVAL`), so a
  crash can lose writes acknowledged within the last flush interval; `WAIT` and `WAITAOF` exist for clients that need
  more. This is existing behaviour and out of scope, but it shapes the protocol below. A handover must flush before
  the new writer opens; otherwise it would lose acknowledged writes in a case where nothing crashed.
* **Unplanned failover.** If the writer's host fails, recovery is whatever it is today: restart the process. This
  RFC only covers a planned switch between two healthy processes. The boot rule it introduces does make restarts
  safer (see *Boot rule*).
* **Read replicas.** The follower serves no client traffic.
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

Neither reserved key is visible to Redis clients. User keys start with a database number and internal keys with `-`
or `+`, so `!` cannot collide, and commands that enumerate keys (`KEYS`, `DBSIZE`) iterate only the current database's
prefix. Any future command that enumerates keys must also stay within a database prefix.

## Roles

Each node is in exactly one role. `/livez` is 200 in every role; `/readyz` is 200 only in **Writer**.

| Role | Store opened as | Clients | Moves to |
|---|---|---|---|
| **Writer** | `Db` | served normally | Draining |
| **Draining** | `Db` | write transactions rejected; reads served | HandedOff, or Writer on abort |
| **HandedOff** | `Db` (fenced, or about to be) | all connections closed, listeners stopped | Writer on abort |
| **Follower** | `DbReader` | none (listeners stopped) | Writer, by promotion |
| **Standby** | `DbReader` | none (listeners stopped) | none: waits to be stopped |

## A handover, end to end

Node A is the writer. The orchestrator wants node B, running the new release, to take over.

1. **Start B as a follower**, with its own node ID and `--handover-id H`. B opens a `DbReader`, reads the writer
   record (which names A) and becomes a Follower. If the new release cannot open or read the store, it fails here,
   and A has not been touched. The orchestrator waits for `GET /handover` on B to report `follower`.
2. **Drain A**: `POST /handover/drain?id=H` on A. A rejects new write transactions with `-READONLY` and lets in-flight
   ones finish. It then writes the handover marker, flushes, and moves to HandedOff, closing every client connection
   and refusing new ones. Clients reconnect and retry.
3. **Promote B**: `POST /handover/promote?id=H` on B. Once B's reader can see the marker, which proves B can read A's
   last write, B closes the reader and opens the store as writer. This fences A in SlateDB. B then writes the writer
   record durably and moves to Writer. Its `/readyz` turns 200.
4. **Move traffic to B** and stop A. In a deployment that routes only to ready instances, this happens on its own:
   A's readiness failed at step 2 and B's succeeded at step 3.

The orchestrator may call promote before drain; promote simply waits for the marker.

Clients see a short period of errors and reconnects between steps 2 and 4, while nothing serves them. They never see a
stale read: A stops serving reads before B can accept a write.

## When things go wrong

* **B fails before step 2** (bad release, bad configuration, cannot open the store): A is untouched. The orchestrator
  stops B and reports the failure.
* **B fails between steps 2 and 3**: A is HandedOff and nobody is writing. The orchestrator stops B, makes sure it
  cannot start again, then calls `POST /handover/abort?id=H` on A. A reopens the store as writer, rewrites the writer
  record and resumes. Writes were unavailable until the abort.
* **A restarts after step 3**: it reads the writer record, finds B, and comes up as Standby instead of reopening as
  writer and fencing B.
* **B restarts after step 3**: it finds itself in the writer record and reopens as Writer.

## Boot rule

A node decides its role at startup from the writer record, never by default:

* the record names this node → **Writer**
* the record names another node, and `--handover-id` is set → **Follower**
* the record names another node, and `--handover-id` is not set → **Standby**
* there is no record (a new store, or one last written by an older release) → **Writer**, unless `--handover-id` is
  set, in which case **Follower**

The last rule allows handover support to be adopted with one ordinary restart: the upgraded writer finds no record,
opens as writer and writes one.

## Example: Kubernetes

As an illustration only, not something this RFC ships: run Invar with the pod name as its node ID through the
downward API, and a Service that selects on readiness. A rolling update with one extra pod and no unavailable pods
starts the new pod, which comes up as a Follower and is not ready. Kubernetes waits for it to become ready, which only
happens after promotion. Something still has to call drain and promote, and that component is not part of Kubernetes
or of Invar. It could be an operator, a job run by the deploy pipeline, or a container in the new pod that drives
steps 2 and 3. The handover ID must reach the new pod, for example through a pod annotation or the rollout's revision.

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
returns the result of the first.

* `GET /handover` → `{role, node_id, handover_id, writer}`
* `POST /handover/drain?id=H` (Writer → Draining → HandedOff). Rejects new write transactions, waits for in-flight
  ones, writes `!invar:handover = H`, calls `Db::flush`, waits until `durable_seq` covers the marker, closes all client
  connections and stops the client listeners. Responds once HandedOff. A drain with a different ID while one is in
  progress or completed is refused with 409.
* `POST /handover/promote?id=H` (Follower → Writer). Responds 202 at once; progress is read from `GET /handover`.
  Waits until the reader reads `!invar:handover = H`, closes the reader, opens `Db`, writes
  `!invar:writer = <node id>`, waits for it to be durable, then starts the client listeners. Refused with 409 if this
  node was started with a different handover ID.
* `POST /handover/abort?id=H` (Draining or HandedOff → Writer). Closes and reopens `Db` (which re-fences), rewrites the
  writer record durably, clears the drain state and restarts the client listeners. **Only safe once the orchestrator
  knows the handover target cannot open the store**; see *Abort*.

## Rejecting writes while draining

The drain gate lives in the SlateDB store, in two places:

* **`begin` with `mutating = true`** returns a new `Error::ReadOnly`. Commands already declare whether they write
  (`is_mutating` on each op, `true` for `EVAL`), so most writes are refused before doing any work.
* **`commit` of a transaction that wrote anything** also returns `Error::ReadOnly`. This backstop is what makes the
  guarantee hold: `SlateDb::begin` ignores the `mutating` flag today, so a transaction opened as non-mutating can
  still put or delete. `SlateTx` records whether it has written, and a commit with writes is refused while draining.
  `destroy` and `drop_prefix` (`FLUSHALL`, `FLUSHDB`), which open their own transactions, are gated the same way.

The RESP layer maps `Error::ReadOnly` to `-READONLY You can't write against a read only replica.`, the error Redis
replicas return. Clients can be configured to reconnect on it (ioredis documents exactly this case for
`reconnectOnError`). The HTTP listener (RFC 0003), when enabled, returns the same error in its JSON error body and
closes keep-alive connections when HandedOff.

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

`abort` reopens the store rather than simply lifting the drain gate. Because `commit` acknowledges writes before they
are durable, a writer that resumed while another process had opened the store would acknowledge writes and then lose
them when its next WAL flush was fenced. Reopening fences any such process first, so every write acknowledged after an
abort reaches a store owned by the aborting node.

Even so, the orchestrator must only abort once the target is stopped and cannot restart. Otherwise the target could
come back, and the two nodes would take turns fencing each other until one of them was stopped.

## Security

The admin endpoints can stop a store's writer. Like the RESP listener and the HTTP listener of RFC 0003, they are
unauthenticated: `--admin-addr` is unset by default, and when set it should be bound to loopback or reachable only
by the orchestrator. Invar logs a warning at startup when it is bound to a non-loopback address.

The metrics and probe listener stays `GET`/`HEAD` only; handover is never controlled through it.

# Drawbacks

* **Not zero downtime.** Clients see a short outage: the drain, the follower's WAL poll, opening the writer, and the
  deployment noticing the change in readiness. The gain over an in-place restart is a shorter outage and a release
  that is proven to start before anything is stopped, not continuous availability.
* **Needs an orchestrator.** Invar cannot hand over on its own, and most rollout tools will not call drain and promote
  without extra glue.
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
* **Serve stale-but-bounded reads from the old writer during the switch.** It shortens the read outage, but makes
  reads after a handover depend on timing. The RFC favours correctness.

# Unresolved Questions

> **Open Question:** Should the follower warm the cache before a drain is requested? A `DbReader` that serves no
> reads warms nothing, so the follower phase is a pre-flight check and a catch-up mechanism, not a cache warmer.
> Prefetching recently used data is possible but needs a policy for what to fetch.

> **Open Question:** A writer accepts the first drain request it receives, for any handover ID. If two orchestrators
> (or a retried, abandoned attempt) both start handovers, one of them will fail and need an abort. Is it enough to
> document that the orchestrator must serialize handovers per store, or should Invar enforce something stronger, for
> example a drain that names the expected follower's node ID?

> **Open Question:** Should `promote` time out on its own if the marker never appears, or wait until it is told to
> stop? Waiting is simpler, but leaves a follower that looks healthy while nothing is happening.
