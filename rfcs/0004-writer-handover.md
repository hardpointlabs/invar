# Writer handover for rolling updates

Status: Draft

Authors:

* Chris Mowforth


# Summary

Invar allows one writer per store, so upgrading it means stopping the writer and starting the new release in its
place. If the new release cannot start, nothing is left serving the store. This RFC proposes the smallest change that
makes that kind of rollout lower risk. The new release is first started beside the writer as a read-only *follower*,
which proves it can open and read the store. The old writer then shuts down gracefully, and the follower is promoted
to writer. Graceful shutdown is extended so that no write a client was told had succeeded can be lost: it handles
`SIGTERM`, and from the moment it begins, commits are only acknowledged once they are durable. That guarantee lives
in the `kv` crate, below any wire protocol.

There is still a gap during which no process accepts writes; with a single writer per store there has to be. The aim
is safer rollouts with very little new machinery. A shorter gap than an in-place restart is a side benefit.

# Motivation

* **Bad releases should fail before they take anything down.** When a writer is replaced in place, a release that
  cannot open the store (a wrong flag, a crash on startup, an incompatible format) is only discovered after the
  working process has stopped. A follower has to open and read the store before the writer is touched.
* **Planned shutdowns should never lose an acknowledged write.** `DbTransaction::commit` in SlateDB 0.18 returns before
  the write is durable in object storage; durability follows on the next WAL flush (`SLATEDB_FLUSH_INTERVAL`). In
  normal operation that is a deliberate trade-off, and `WAIT`/`WAITAOF` exist for clients that need more. A shutdown
  for an upgrade is planned, though, and should cost a temporary slowdown at most, never a lost acknowledgement. Today
  it can cost more: Invar only handles `SIGINT`, while process supervisors and container runtimes stop processes with
  `SIGTERM`, which kills Invar without closing the store. Every ordinary restart can lose writes acknowledged within
  the last flush interval.
* **The guarantee belongs below the wire protocols.** Every protocol built on the `kv` crate should get it without
  managing acknowledgements itself.

## Out of scope

* **Eliminating the write gap.** Between the old writer stopping and the follower opening the store, no process
  accepts writes. Hiding the gap would take something in front of Invar that holds requests until the new writer is
  ready; that is a deployment concern.
* **Durability in normal operation.** Outside a shutdown, commits are acknowledged as they are today. A crash or
  hardware failure can still lose writes acknowledged within the last flush interval.
* **Automatic failover.** Nothing here detects a failed writer or promotes a follower on its own.
* **Deeper release validation and cache warming.** A follower proves only that the new release can open and read the
  store. Checking that it can actually serve traffic, and warming its cache before promotion, are left to the
  orchestrator.
* **Moving client traffic.** Invar reports its state through `/readyz`; the deployment's load balancer, service
  discovery or proxy routes clients.
* **Read replicas.** Expected in a separate RFC. This one defines readiness and startup roles so that replicas can be
  added without changing them (see *Read replicas*).

# Guide Implementation

## Concepts

**Follower.** An Invar process started with `--follower`. It opens the store with SlateDB's `DbReader`, serves no
clients, and reports not ready. When told to, it becomes the writer.

**Orchestrator.** Whatever runs the rollout: a deploy script, a Kubernetes operator, or a person at a terminal. Invar
does not depend on any particular one.

**Acknowledgement level.** A store-wide setting in the `kv` crate that decides when a commit returns. *Buffered* (the
default, and today's only behaviour) returns once the write is committed in memory. *Durable* returns only once the
write would survive the process dying. Graceful shutdown switches to Durable.

## Roles

| Role | Store opened as | Clients | `/readyz` |
|---|---|---|---|
| **Starting** | not yet | none | 503 |
| **Follower** | `DbReader` | none | 503 |
| **Writer** | `Db` | served normally | 200 |
| **Stopping** | `Db`, closing | no new commands; in-flight commits finish | 503 |
| **Replica** *(future RFC)* | `DbReader` | reads served; writes rejected | 200 while serving |

`/livez` is 200 in every role. `/readyz` is 200 when the process is serving clients in the role it was started for,
which keeps readiness a single yes or no per process, as load balancers and probes expect. The current role is
available at `GET /role` on the probe listener, so an orchestrator can tell a healthy follower from a process that is
still starting.

## A rollout, end to end

A is the writer. The orchestrator wants B, running the new release, to replace it.

1. **Start B with `--follower`.** B opens a `DbReader`. If the new release cannot open or read the store, it fails
   here and A has not been touched: the orchestrator stops B and the rollout is over. The orchestrator waits for
   `GET /role` on B to return `follower`.
2. **Stop A gracefully** (`SIGTERM` or `SIGINT`). A switches to Durable acknowledgements, reports not ready, stops
   accepting commands, lets in-flight commits finish (each acknowledged only once durable), closes the store, which
   flushes, and exits.
3. **Once A has exited, promote B** (`POST /promote` on B's admin listener). B closes its reader, opens the store as
   writer, and reports ready.
4. **Traffic moves to B.** In a deployment that routes only to ready processes, this happens on its own.

The write gap runs from step 2 to step 3: A's shutdown, the orchestrator noticing that A has exited, and B opening the
store, which includes replaying the WAL. Clients reconnect and retry. They never see a stale read, because A stops
serving before B starts.

If B fails after step 2, there is no special recovery: start the old release again, as any restart would.

## Two rules for the orchestrator

* **Promote only after the old writer has exited.** Opening a writer fences the previous one in SlateDB, so the two
  can never both commit. But fencing protects the data, not acknowledgements: anything the old writer acknowledged
  and had not yet flushed is lost when the new writer opens. A graceful shutdown flushes everything before it exits,
  so waiting for the exit is enough. (If the old writer's host has already failed, promoting is no worse than
  restarting it today.)
* **Do not restart the old writer once the follower has been promoted.** A restarted old release would open as
  writer, fence the new one, and lose whatever the new one had acknowledged but not yet flushed. Rolling update
  mechanisms that delete the old instance already behave this way; a supervisor that restarts exited processes must
  not be pointed at the old one.

## Example: Kubernetes

As an illustration only, not something this RFC ships: a rolling update with one extra pod and no unavailable pods
starts the new pod, which here would need to run with `--follower`, and waits for it to become ready. Something has
to stop the old pod and then call promote on the new one. That step is not part of Kubernetes or of Invar. It could be
an operator, a job run by the deploy pipeline, or a container in the new pod that watches for the old pod to
terminate and then calls promote over loopback.

# Reference Implementation

## Configuration

| Flag | Env | Purpose |
|---|---|---|
| `--follower` | `INVAR_FOLLOWER` | Start as a follower instead of a writer. Requires the SlateDB backend. |
| `--admin-addr` | `INVAR_ADMIN_ADDR` | Listen address for `POST /promote`. Unset: no admin listener. |

Without `--follower`, Invar starts as a writer exactly as it does today.

## Follower and promotion

`SlateDb` gains a reader variant, opened with `DbReader`. The follower keeps it open until it is promoted, so that a
store it cannot read is caught at startup rather than at promotion. It uses `DbReaderMode::ManagedCheckpoint`
(SlateDB's default), so the writer's garbage collector cannot delete files it is still using.

`POST /promote` responds 202 and starts the switch: close the reader, open `Db`, start the client listeners, then
report `writer`. Progress is read from `GET /role`. Promoting a process that is not a follower returns 409. If opening
the store fails, the process stays not ready, reports `follower` again, and logs the error, so promotion can be
retried.

The follower and the writer it becomes are the same process, so they share the Foyer cache. A follower must have its
own `--cache-path`, distinct from the writer it is replacing: two processes must not share an on-disk cache directory.

## Durable acknowledgements

* **Interface.** `KeyValueStore` gains `set_ack_level(AckLevel)` and `ack_level()`, with `AckLevel::Buffered` (the
  default) and `AckLevel::Durable`. When the level is Durable, `Tx::commit`, and `update` through it, return only once
  the write is durable. The level is read after the backend's commit has succeeded and just before returning, so
  every commit that returns after the switch waits, including commits that started before it. A front end that offers
  per-request durability (as `WAIT` does) can ask for more than the store's level, never less.
* **SlateDB.** The wait is the one `await_until` already performs for `WAIT`: subscribe to `DbStatus` and wait until
  `durable_seq` reaches the commit's sequence number. The two share one helper, which also wakes when `close_reason`
  is set and returns an error rather than waiting on. This fixes an existing problem: when a `Db` is fenced, SlateDB
  keeps the status channel open and only sets `close_reason`, so today `WAIT 0` on a fenced writer waits forever.
* **Latency.** A Durable commit waits for the next WAL flush, up to `SLATEDB_FLUSH_INTERVAL`. To bound that without
  changing SlateDB's settings at runtime, Invar calls `Db::flush` every 50ms while the level is Durable and writes
  are pending. The interval is hard-coded to begin with.
* **Timeout.** A Durable commit that is not durable within 1 second fails with `Error::Timeout`, and the client gets
  an error. The outcome is then unknown, as for any failed commit: the write may still become durable. The client is
  never told it succeeded unless it is durable. The 1 second limit is hard-coded to begin with, and sits well inside
  the default `--shutdown-timeout` of 10 seconds.
* **Fjall.** Durable means the commit's journal entry has been synced to disk before `commit` returns, so graceful
  shutdown gives the same guarantee with every backend.

## Graceful shutdown

1. `SIGTERM` is handled like `SIGINT`.
2. The store switches to Durable acknowledgements, and `/readyz` turns 503.
3. The client listeners stop accepting connections and commands. Commits already in flight finish, durably, or fail;
   none is acknowledged otherwise.
4. The store closes, as today. `Db::close` flushes, so writes acknowledged before step 2 are persisted too, unless
   closing exceeds `--shutdown-timeout`, in which case Invar logs an error saying that acknowledged writes may have
   been lost.

This applies to every Invar process, whether or not it is part of a rollout.

## Fencing

SlateDB reports fencing as `ErrorKind::Closed(CloseReason::Fenced)`, which `map_slate_error` currently folds into
`Error::Undefined`. It gets its own variant, `Error::Fenced`. A writer that sees it has been replaced (which in this
design only happens if the orchestrator broke one of its two rules) stops accepting commands, reports not ready, and
logs an error. It does not exit, because a supervisor might restart it, and a restarted process would open as
writer and fence its replacement.

## Read replicas

Replicas are left to their own RFC. This one fixes the rules they must fit into:

* **Startup:** a replica is selected by its own flag and never opens the store as writer.
* **Readiness:** ready while serving reads, under the per-role definition in *Roles*.
* **Writes:** rejected with `-READONLY You can't write against a read only replica.`, the error Redis replicas
  return, which clients can be configured to treat as a reason to reconnect.
* **Promotion:** refused. `POST /promote` applies only to followers.

That RFC also has to settle consistent multi-key reads. Every Invar read runs in a snapshot transaction today, so a
command that reads several keys (a hash sentinel and its fields, a list's nodes, `MULTI`, Lua) sees one point in time.
In SlateDB 0.18.1 only `Db` offers snapshots: `DbReader` has `get` and `scan`, and `ReadOptions` cannot pin a read to
a sequence number, so a multi-key command on a replica could straddle a refresh. The follower in this RFC serves no
reads, so it is not affected.

## Security

`POST /promote` makes a process open the store as writer, fencing any current writer. Like the RESP listener and the
HTTP listener of RFC 0003, the admin listener is unauthenticated: `--admin-addr` is unset by default, and when set it
should be bound to loopback or reachable only by the orchestrator. Invar logs a warning at startup when it is bound to
a non-loopback address. The probe listener stays `GET`/`HEAD` only; `GET /role` is read-only.

# Drawbacks

* **A write gap remains.** It is shorter than an in-place restart (no scheduling, image pull or process start), but
  clients still see errors and reconnect.
* **Correctness depends on the orchestrator's two rules.** Promoting too early, or restarting the old writer
  afterwards, loses acknowledged writes. Invar cannot detect either.
* **Slower writes during shutdown.** Every commit in that window waits for a flush. This is the intended trade.
* **Needs an orchestrator.** Most rollout tools will not call promote without some glue.

# Rationale / Alternatives

* **Keep the old writer running and hand over in place** (an earlier draft of this RFC). The old writer would stop
  accepting writes, write a durable marker into the store, and stay up so that a failed rollout could be reversed
  without a restart. It needed reserved keys, drain and abort endpoints, extra roles, and a rule for `FLUSHALL`, to
  save one restart of the old release in the rare failed case. A graceful exit gives the same guarantee, since a clean
  exit means everything is flushed.
* **A writer record in the store.** The writer would record its identity in a reserved key on opening, and a process
  would read it at startup to tell whether it had been replaced. That would turn the second orchestrator rule into
  something Invar enforces. It can be added later without changing anything here, if restarted old writers turn out
  to be a real problem.
* **Rely on SlateDB fencing alone** (start the new release as writer straight away). It fences the old writer before
  the new release has shown it can serve, and loses whatever the old writer had acknowledged but not flushed.
* **Durable acknowledgements in each wire protocol.** Every protocol built on `kv` would need its own version.
* **Durable acknowledgements all the time.** Safest, but makes every write wait for a flush in normal operation. The
  `AckLevel` interface leaves room to offer it as an option later.
* **Promotion by signal** (for example `SIGUSR1`) instead of an admin listener. It needs no new listener, but a
  helper running in a separate container usually cannot signal Invar's process, which rules out one of the most
  common ways to build an orchestrator.

# Unresolved Questions

None at present. Earlier questions were resolved as follows:

* **Durable commit timeout:** 1 second, hard-coded (see *Durable acknowledgements*), with the flush timer at 50ms.
* **Validating the new release beyond opening the store, and cache warming:** out of scope, left to the orchestrator.
