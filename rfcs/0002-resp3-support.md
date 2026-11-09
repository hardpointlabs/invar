# RESP3 support

Status: Draft

Authors:

* Chris Mowforth


# Summary

Invar speaks RESP2 only; `HELLO 3` is refused with `NOPROTO`. This RFC proposes adding RESP3 as a protocol a
client can opt into per connection via `HELLO 3`, while **RESP2 remains fully supported and remains the
default**. The design extends `RespValue` with the RESP3 types and makes the *encoder* responsible for
rendering them, degrading to the existing RESP2 shapes on RESP2 connections. Command `WireOp`s keep their
signature and `DbOp`s are untouched; only the commands whose reply shape genuinely differs in RESP3 change,
and each change is local to the value that command's `WireOp` builds.

# Motivation

Several widely used clients now negotiate RESP3 by default or on request (go-redis v9 and Lettuce send
`HELLO 3`; node-redis and redis-py can be configured to). Today those clients either fall back, lose
RESP3-only features, or need explicit configuration. RESP3 also gives clients typed replies (maps, sets,
doubles, a single null) instead of positional, protocol-flattened arrays, and out-of-band push messages.

The existing refusal is deliberate and must stay available until the implementation is complete: `NOPROTO`
is what lets RESP2-only clients such as ioredis (and therefore BullMQ) fall back cleanly. Whatever we build
must not change a byte of RESP2 output.

# Guide Implementation

A client opts in per connection:

```
HELLO 3            -> map reply (server, version, proto=3, id, mode, role, modules)
HELLO 2            -> back to RESP2, array reply as today
HELLO              -> reply in the connection's current protocol; does not change it
RESET              -> connection returns to RESP2
```

A connection that never sends `HELLO 3` is indistinguishable from today.

Within a RESP3 connection:

* nulls are encoded as `_` instead of `$-1` / `*-1`;
* commands that return field/value pairs return a map, set-valued commands return a set, scores are doubles;
* `ZRANGE ... WITHSCORES` and similar return an array of `[member, score]` pairs rather than a flat array;
* pub/sub messages and subscribe/unsubscribe acknowledgements are push frames (`>`).

**Subscribe mode is unchanged.** Real Redis lets a RESP3 connection run arbitrary commands while subscribed.
Invar deliberately does not: a subscribed connection, RESP2 or RESP3, accepts only the subscribe family,
`PING`, `QUIT` and `RESET`, and rejects everything else exactly as it does today. The only RESP3 difference
in subscribe mode is the frame type of what is sent.

`HELLO` is **not permitted inside `MULTI`**, in either protocol. It is rejected like any other command that
is not allowed in a transaction. (Today it is queueable, which is an oversight: it mutates session state at
queue time rather than at `EXEC`.)

# Reference Implementation

## 1. `RespValue` gains the RESP3 types

```rust
pub enum RespValue {
    // existing
    SimpleString(Bytes), Error(Bytes), Integer(i64),
    BulkString(Option<Bytes>), Array(Option<Vec<RespValue>>),
    // new
    Null,
    Boolean(bool),
    Double(f64),
    Map(Vec<(RespValue, RespValue)>),
    Set(Vec<RespValue>),
    Push(Vec<RespValue>),
    // optional, see Unresolved Questions
    Verbatim { format: [u8; 3], data: Bytes },
}
```

`BigNumber` and attributes are omitted: nothing in Invar would emit them.

## 2. The encoder is protocol-aware; the degradation is the single source of truth

`RespDecoder`'s `Encoder` impl gains a `Protocol { Resp2, Resp3 }` field (default `Resp2`, set through a
setter reachable via `Framed::codec_mut`). Each type renders as follows:

| Variant | RESP3 | RESP2 (exactly today's output) |
|---|---|---|
| `BulkString(None)`, `Array(None)`, `Null` | `_` | `$-1`, `*-1`, `$-1` respectively |
| `Boolean` | `#t` / `#f` | `:1` / `:0` |
| `Double` | `,<n>` (`inf`, `-inf`, `nan`) | bulk string using `format_f64` |
| `Map` | `%` | flat array of `2n` elements |
| `Set` | `~` | array |
| `Push` | `>` | array |
| `Verbatim` | `=` | bulk string |

Consequences:

* The 44 `BulkString(None)` and 14 `Array(None)` construction sites need **no change**: the encoder picks
  the null form. This includes the `EXEC` conflict reply.
* `format_f64` currently lives privately in `zset.rs`. It moves (unchanged) into `resp.rs` so `Double`'s
  RESP2 rendering is byte-identical to today's `ZSCORE` output. Moving it is the only RESP2-visible code
  motion.
* Nested values (`EXEC` results, `XREAD`) are handled by the same recursive encode pass.
* The same downgrade logic is exposed as a function so the Lua bridge (below) shares it.

## 3. Protocol state lives on `Session`

`HELLO` already takes `&mut Session` and mutates it (for `SETNAME`), so it sets `session.protocol` as well.
After each `enqueue_command` returns, the listener applies `framed.codec_mut().set_protocol(...)` **before**
sending that batch's replies, so the reply to `HELLO 3` is itself a map. Replies already sent are never
re-encoded; the protocol is only ever read at send time. `HELLO` replies with a map in RESP3 and with the
existing flat array in RESP2, with `proto` reflecting the negotiated version.

Until the audit in step 4 is done, the `NOPROTO` refusal stays behind a single constant so the plumbing can
merge without exposing RESP3 to clients.

## 4. Command audit

Only commands whose RESP3 reply differs from the RESP2 shape change. The candidate list is drawn from the
RESP3 specification and Redis's behaviour, intersected with the commands Invar supports (see the
[command support appendix](https://docs.hardpoint.dev/guides/invar/appendix/redis-tm-command-support)).
**Each entry is verified against the spec before it is changed**, and commands not listed are left alone.
Commands Invar does not implement (e.g. `CONFIG GET`, `XREADGROUP`, other `XINFO` subcommands) are out of
scope until they exist:

* **Map:** `HGETALL`, `XREAD` (keyed by stream), `XINFO STREAM`, `PUBSUB NUMSUB`, `HELLO`, and possibly
  `BF.INFO` / `JSON.RESP`.
* **Set:** `SMEMBERS`, `SINTER` / `SUNION` / `SDIFF`, `SPOP` with count.
* **Double:** `ZSCORE`, `ZINCRBY`, `ZMSCORE`.
* **Pair arrays:** `ZRANGE` / `ZRANGEBYSCORE` / `ZREVRANGE` / `ZREVRANGEBYSCORE` with `WITHSCORES`,
  `ZDIFF` / `ZINTER` / `ZUNION` with `WITHSCORES`, `ZPOPMIN` / `ZPOPMAX` with count, `BZPOPMIN` /
  `BZPOPMAX` (member and double score), `ZRANDMEMBER` / `HRANDFIELD` with scores/values.
* **Push:** the helpers in `pubsub.rs` (`message_resp`, `subscribe_resp`, ...) return `Push`, which renders
  as an array in RESP2.
* **Verbatim (optional):** `INFO`, `CLIENT LIST`, `CLIENT INFO`.

Because `RespValue` carries the richer type and the encoder downgrades, a command converted in this step
still produces exactly its old bytes on RESP2 connections. A unit test per converted command asserts both
encodings.

## 5. Lua scripting

Redis converts command replies for scripts using RESP2 rules unless a script calls `redis.setresp(3)`.
`resp_to_piccolo` is extended to handle the new variants by applying the RESP2 downgrade first (map to flat
table, set to array, double to string, push to array). `redis.setresp` is out of scope. A script's *return*
value is rendered for the calling client's protocol like any other reply.

## 6. Decoder

Servers only receive arrays of bulk strings, and `extract_args` already rejects anything else, so no
RESP3 request parsing is needed. The decoder is extended anyway so tests (Rust and the client side of the
codec) can read RESP3 replies: `ArrayFrame` generalises to an aggregate frame with a kind, a map frame
expects `2n` children and pairs them on completion, and the existing length and depth limits apply to the
new aggregates.

## 7. Testing

* Codec unit tests: every variant under both protocols, and a golden test pinning that all existing RESP2
  output is byte-identical.
* Listener tests: `HELLO 3` → map; `HELLO 2` after `HELLO 3`; `HELLO` rejected in `MULTI`; `RESET`;
  subscribe-mode restrictions identical under both protocols; push framing.
* Integration (`./test`): run the command suite under both protocols, with an optional per-case
  `expected_resp3` override for cases whose shape differs. RESP2 stays the default so BullMQ and
  existing suites are unaffected.

## Rollout

Separate PRs, each independently green on RESP2:

1. Codec: variants, protocol-aware encoder, decoder support, tests. Nothing emits the new variants yet.
2. Session and listener plumbing: `session.protocol`, `HELLO 2|3`, `RESET`, `HELLO`-in-`MULTI` rejection,
   codec sync. `HELLO 3` still returns `NOPROTO`.
3. Command audit (step 4), pub/sub push, Lua downgrade.
4. Dual-protocol integration tests, then remove the `NOPROTO` gate.

# Drawbacks

* `RespValue` grows from 5 to 11 variants; every exhaustive `match` on it (the Lua bridge, tests) must
  handle them.
* The `HELLO`-in-`MULTI` rejection is a change to existing RESP2 behaviour, though a client relying on
  queueing `HELLO` is unlikely.
* Once `NOPROTO` is lifted, go-redis and Lettuce switch to RESP3 automatically, so any shape bug surfaces
  to them immediately. The `NOPROTO` gate and the audit-before-flip ordering exist to mitigate this.
* RESP3 subscribed connections are more restricted than in Redis, which a RESP3 client may not expect.

# Rationale / Alternatives

* **Pass the protocol into `WireOp::reply`.** Touches all 78 `WireOp` impls and every call site, including
  the Lua path and test helpers, for no benefit over encode-time rendering.
* **A separate `Reply` type distinct from `RespValue`.** Cleaner in the abstract, but churns every command
  file; the project guidelines favour minimal churn.
* **A `reply_resp3` method with a default delegating to `reply`.** Needs edits at the same sites and leaves
  two code paths per command that can drift.
* **Allowing arbitrary commands while subscribed in RESP3.** Matches Redis, but requires merging the
  listener's two loops. Rejected for now to keep the subscribe path identical across protocols.

# Unresolved Questions

> **Open Question:** `PING` while subscribed replies with a push-style array in RESP2. What should it
> return in RESP3: the same array shape as a push frame, or a plain `PONG`?

> **Open Question:** should `INFO` and `CLIENT LIST` return verbatim strings (`=txt`) in RESP3? It is
> optional for correctness; it determines whether the `Verbatim` variant is included.

> **Open Question:** how the Deno `redis` client surfaces RESP3 types is unverified, so the exact shape of
> the dual-protocol test harness (step 7) may change.

> **Open Question:** the command audit list is drawn from the spec and the command support appendix, not yet
> checked against each `WireOp` in this repository. The appendix may also omit a few supported commands
> (e.g. `KEYS`).
