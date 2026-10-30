# Upstash-compatible Redis-over-HTTP listener

Status: Draft

Authors:

* Chris Mowforth


# Summary

Upstash exposes Redis over HTTP so that serverless and edge runtimes, which cannot open TCP sockets, can use Redis
through plain `fetch`. Upstash has never published a formal specification for this API; its behaviour is spread across
one documentation page and the source of its SDKs. This RFC does two things. First, it assembles that specification
(the *Upstash REST protocol*, restricted to submitting Redis commands) from the public docs and the SDK sources, and
marks each statement by how well it is evidenced. Second, it proposes an optional, off-by-default HTTP listener in Invar
that implements it, so that `@upstash/redis`, `upstash-redis` (Python) and everything built on them can talk to Invar
unmodified by changing only a URL and a token.

# Motivation

* **Reach.** Invar's pitch is a durable, operationally simple store that Redis clients can speak to. Redis clients
  cover TCP environments. They do not cover Cloudflare Workers, Vercel Edge, Deno Deploy, Fastly Compute, browsers or
  WebAssembly, which is where the Upstash SDKs are used. Libraries layered on those SDKs, such as Upstash's own
  rate-limiting SDKs (`docs/redis/sdks/ratelimit-ts`, `ratelimit-py`), issue ordinary commands (including `EVAL`) through
  them, so they should work without extra effort. This RFC did not review any such library's source.
* **Local development parity.** Today a project that uses Upstash in production cannot run its tests against a local
  store without a mock. Invar with Fjall or in-memory storage can be that store.
* **No new data model.** The listener translates HTTP requests into the command set Invar already implements. It adds a
  front end and no new storage semantics.

## Scope

In scope: submission of Redis commands (single, pipelined, transactional) over HTTP, bearer-token authentication,
response encodings, and `SUBSCRIBE`/`PSUBSCRIBE` over server-sent events.

Out of scope: everything else in Upstash's product surface (QStash, Vector, Search, Workflow, the management API), TLS
termination, multi-tenancy and per-database tokens, billing/quota behaviour, `MONITOR` (see Unresolved Questions), and
global replication semantics (read replicas, regions).

## On "all languages"

The only Upstash-maintained SDKs for this API that I found are **TypeScript** (`@upstash/redis`, v1.40.0 at the time of
writing) and **Python** (`upstash-redis`, v1.8.0). Their docs and tests are the ground truth used below. Community
clients for Go, Rust and Dart exist but are unreviewed here; a protocol implemented faithfully to the two official SDKs
and to the documentation should serve them, but this RFC does not claim to have verified them. See *Compatibility
claims* below.

## Legal and licensing posture

This section records the project's intent and the facts behind it. It is not legal advice; the project's maintainers
(and counsel, if they wish) own the conclusion.

**Implementing a wire protocol.** Invar already speaks the Redis protocol without being Redis, and this RFC proposes the
same thing for the Upstash REST protocol: a separately written server that accepts the same requests and returns the
same responses so that existing clients interoperate. Re-implementing an interface for interoperability, without copying
the original implementation, is long-standing industry practice. The RFC describes *behaviour* (endpoints, headers,
encodings, status codes); it does not reproduce Upstash's server code, because none of it was consulted, and none of
the SDK code is to be reproduced in Invar.

**What was read, and under what terms.**

| Source | License | Use made of it |
|---|---|---|
| `upstash/redis-js` | MIT (Copyright Upstash, Inc.) | read to discover the API surface; no code copied |
| `upstash/redis-python` | MIT (Copyright Upstash, Inc.) | read to discover the API surface; no code copied |
| `upstash/docs` | **no license file in the repository**, so default copyright applies | read for facts about the API; the RFC paraphrases rather than reproduces, and quotes only identifiers (header names, paths, error strings) |

Note that the SDKs are **MIT-licensed, not public domain**. MIT permits reading, using and even copying the code, but
requires that the copyright and permission notice travel with any copy of "substantial portions". Invar's position is
stricter than MIT requires: no SDK code is copied or adapted into Invar. Ideas, facts about wire behaviour, and short
identifiers needed for interoperability are not reproduced *expression*.

**Rules for implementers.**

1. Implement from this RFC, not from SDK source. Where the RFC is silent, ask or run the Appendix A probes; do not
   settle the question by transliterating SDK code.
2. Do not vendor the SDKs, copy their tests, or copy docs prose into the repository. Using an SDK as a *dependency* of
   the Deno integration tests (`npm:@upstash/redis`) is ordinary use under MIT and keeps its license attached. The
   upstream test suites may be *run* against Invar (see Testing) but not checked in.
3. Test fixtures and expected responses are written for Invar, in Invar's own words and data.
4. If any MIT-licensed material is ever copied, retain its notice in the file and add it to a third-party notices file
   in the same change.

**Trademark.** "Upstash" is presumed to be a trademark of Upstash, Inc. and nothing here grants any right to it.

* Invar is not affiliated with, endorsed by, or sponsored by Upstash, Inc. The user-facing documentation for this
  feature must say so.
* The name is used only *nominatively*, to say what Invar is compatible with ("compatible with the Upstash Redis REST
  protocol and its SDKs"), never as the name of Invar, a flag, an environment variable, a binary, a crate or a logo. For
  that reason the proposed flags are `--http-addr`/`--http-token` and not `--upstash-*`, and the module is
  `src/redis/http`, not `upstash`.
* Protocol identifiers that must appear on the wire for clients to work (`Upstash-Encoding`, `Upstash-Sync-Token`, the
  `UPSTASH_REDIS_REST_*` variable names users already have) are used because interoperability requires them; they are
  names of interface elements, not branding of ours. Documentation prefers "Redis-over-HTTP" as the feature name.
* No Upstash logos, styling or copy appear in docs, test output, or the UI.

# Guide Implementation

Start Invar with the HTTP listener enabled:

```
invar --backend fjall --http-addr 0.0.0.0:8080 --http-token s3cret
```

Point an Upstash SDK at it:

```ts
import { Redis } from "@upstash/redis";

const redis = new Redis({ url: "http://localhost:8080", token: "s3cret" });
await redis.set("user:1", { name: "ada" });   // POST http://localhost:8080  body ["set","user:1","{\"name\":\"ada\"}"]
const user = await redis.get("user:1");        // { name: "ada" }
```

```python
from upstash_redis import Redis

redis = Redis(url="http://localhost:8080", token="s3cret")
redis.set("k", "v")
```

Or use it with `curl`, which is how the API is documented:

```
$ curl localhost:8080/set/foo/bar -H "Authorization: Bearer s3cret"
{"result":"OK"}
$ curl localhost:8080/get/foo -H "Authorization: Bearer s3cret"
{"result":"bar"}
$ curl -X POST localhost:8080/pipeline -H "Authorization: Bearer s3cret" \
    -d '[["SET","a","1"],["INCR","a"],["GET","missing"]]'
[{"result":"OK"},{"result":2},{"result":null}]
```

Mental model: **one HTTP request is one short-lived Redis connection.** Each request gets a fresh session on database 0
with no `WATCH` state, runs its command(s), and the session is dropped. The HTTP listener and the RESP listener
operate on the same store, so a key written over HTTP is visible over RESP and vice versa. They also share the
`PUBLISH` and `WATCH` machinery, so an HTTP `PUBLISH` reaches RESP subscribers and an HTTP write invalidates a RESP
connection's `WATCH`.

The listener is off unless `--http-addr` (`INVAR_HTTP_ADDR`) is set. When it is set, `--http-token`
(`INVAR_HTTP_TOKEN`) is mandatory: Invar refuses to start an unauthenticated HTTP listener.

# Reference Implementation

## Part 1: The Upstash REST protocol

### How to read this part

Every rule is tagged with its evidence:

* **[D]** stated in Upstash's documentation (`upstash/docs`, `redis/features/restapi.mdx`, commit `9b28301`).
* **[S]** observed in the official SDK source (`upstash/redis-js` `packages/redis/pkg/http.ts` et al., commit `4d3f58e`,
  v1.40.0; `upstash/redis-python` `upstash_redis/http.py` et al., commit `78567ea`, v1.8.0).
* **[I]** inferred from the above; not stated anywhere.
* **[?]** unknown. These are listed in Unresolved Questions and Appendix A with a proposed way to settle them.

Where Invar must decide something the evidence does not settle, the decision is marked **[Invar]**.

### Transport and endpoints

HTTP/1.1 [I]. Any base path prefix is possible [S]: the TypeScript SDK builds URLs as `[baseUrl, ...path].join("/")`
after stripping one trailing `/` from `baseUrl`; the Python SDK appends `/pipeline` or `/multi-exec` to the URL it was
given without stripping. The server should therefore tolerate empty path segments (`//pipeline`). [Invar]

Allowed methods are `HEAD`, `GET`, `POST` and `PUT`; any other method is `405` **[D]**. Both SDKs only ever send `POST`
**[S]**.

| Endpoint | Method | Body | Meaning | Evidence |
|---|---|---|---|---|
| `/` | POST | JSON array `[cmd, arg…]` | one command | D, S |
| `/{cmd}/{arg}/…` | GET, POST, PUT, HEAD | optional raw value | one command, path form | D |
| `/pipeline` | POST | JSON array of arrays | non-atomic batch | D, S |
| `/multi-exec` | POST | JSON array of arrays | atomic batch | D, S |
| `/subscribe/{channel}/…` | POST | none | SSE stream | D, S |
| `/psubscribe/{pattern}/…` | POST | none | SSE stream | S (not in docs) |
| `/monitor` | POST | none | SSE stream | D (no SDK uses it) |

`Content-Type` is not relied upon: the SDKs send `application/json`, but the documented `curl -d` examples send
`application/x-www-form-urlencoded`, so the server must ignore the request content type. [I]

### Authentication

`Authorization: Bearer <token>` **[D, S]**, or the query parameter `_token=<token>` **[D]**. Failure is `401` **[D]**. The
response body for `401` is `{"error": "..."}` by analogy with other errors [I]; its text is unknown [?].

Upstash issues a *standard* token and a *read-only* token per database, plus per-ACL-user tokens **[D]**. This RFC
proposes a single standard token for the first version (see Unresolved Questions).

### Command encoding

**JSON-array form** (`POST /`). The body is one JSON array whose first element is the command name **[D]**. Elements
the SDKs actually emit are strings, numbers and booleans: the TypeScript serializer passes `string | number | boolean`
through and `JSON.stringify`s everything else into a string; Python passes `str`/`int`/`float` (and, because `bool` is an
`int`, booleans) through and `json.dumps`es the rest **[S]**. The SDKs therefore never send `null`, arrays or objects as
elements.

Conversion of each element to a RESP bulk string **[Invar]**:

* string: its UTF-8 bytes;
* number: the exact decimal text as it appeared in the JSON source (so `100` stays `100`, `1.5` stays `1.5`, and a
  64-bit integer is not rounded through a double);
* boolean: `true`/`false` [?]. This is what a JavaScript `String(true)` produces and is the likeliest behaviour, but
  Upstash's server-side rule is not documented;
* `null`, array, object, or a non-array top-level body: rejected with `400` rather than guessed.

**Pipeline and transaction bodies** are a two-dimensional array; each row is a command in the same form **[D]**.

**Path form** (`GET /set/foo/bar`). Each path segment is percent-decoded into one argument, in order **[D]**. The command
name is the first segment, case-insensitively. Percent-decoding gives path form a binary-safe encoding that the JSON form
lacks **[I]**.

Two extensions are documented **[D]**:

* *Body as last argument.* For `POST`/`PUT` to a path-form URL, the raw request body is appended as the final
  argument: `POST /set/foo` with body `$VALUE` is `SET foo $VALUE`. This is the only way to send an arbitrary binary
  value over this API.
* *Query parameters as trailing arguments.* `POST /set/foo?EX=100` with a body is `SET foo $VALUE EX 100`. The docs give
  one example only. Ordering of multiple parameters, parameters without a value (`?NX`), repeated parameters, and
  whether the name is case-folded are [?]. `_token` is never an argument.

When the path is `/` (empty) and the method is `POST`, the body is always the JSON-array form, never a raw value
(otherwise there would be no command to append it to). [I]

### Response encoding

`Content-Type: application/json` **[D]**.

*Success*: `{"result": <value>}` where `<value>` is a RESP reply mapped as follows **[D, S]**:

| RESP2 reply | JSON |
|---|---|
| simple string (`+OK`) | string |
| bulk string | string (UTF-8; see below) |
| null bulk / null array | `null` |
| integer | number |
| array | array of the above, recursively |

Invar is RESP2-only, so no other reply types need mapping.

*Failure*: `{"error": "<message>"}`, where the message is the Redis error text without the leading `-`, including its
error-code prefix **[D]**: `ERR wrong number of arguments for 'get' command`, `WRONGPASS invalid password`. The
code prefix is load-bearing: the TypeScript SDK recovers from a missing script by lower-casing the message and checking
for `noscript` **[S]**, and the integration tests check `NOSCRIPT No matching script. Please use EVAL.` verbatim **[S]**.

*Non-UTF-8 bytes.* Without further instruction the server replaces invalid UTF-8 with U+FFFD **[D]**. The raw bytes are
available through the base64 encoding below.

#### `Upstash-Encoding: base64`

When the request carries `Upstash-Encoding: base64`, every string in the response is base64 encoded except the `OK`
status reply **[D]**. Both SDKs decode recursively through nested arrays and leave integers and `null` alone;
**error strings are not decoded** **[S]**. The Python SDK **sends this header by default** (`rest_encoding="base64"`),
so a Python client will not work unless it is implemented correctly **[S]**.

A corollary is that status replies other than `OK` are encoded too. Python decodes any non-`"OK"` string with
`b64decode(...).decode()`, and its test-suite asserts `ping() == "PONG"` under the default encoding. Unencoded `PONG`
is valid base64 whose bytes are not valid UTF-8, so Python would raise. Hence `PING` returns `"UE9ORw=="` in this mode
**[I]**. The rule implemented is therefore: *the simple string `OK` is emitted as-is; every other string in a
successful result, status or bulk, is base64 encoded (standard alphabet, padded).*

#### `Upstash-Response-Format: resp2`

Returns the raw RESP2 bytes of the reply, `Content-Type: application/octet-stream`. Any value other than `json` or
`resp2` is `400`; combining `resp2` with `Upstash-Encoding: base64` is `400`; the option does not apply to `/multi-exec`
**[D]**. No SDK uses it. The documentation's own curl examples spell the header `Upstash-Reponse-Format` (sic); the
prose and the table spell it `Upstash-Response-Format`, which is taken as correct. For `/pipeline` the body is the
concatenation of the replies [I]. Because it is documented, cheap to implement and not exercised by any client, it is
the lowest-priority item in this RFC and may be deferred.

### Status codes

| Status | Meaning | Evidence |
|---|---|---|
| 200 | executed (even if an item of a pipeline failed) | D, S |
| 400 | syntax error, unsupported command, or a command failed | D |
| 401 | missing or invalid token | D |
| 405 | unsupported HTTP method | D |

Both SDKs treat any non-2xx status as failure and read `error` from the JSON body (TypeScript throws
`UpstashError("<error>, command was: …")`; Python ignores the status and checks for an `error` key) **[S]**, so the
body must be JSON for every error the server generates, including 401/405 [I]. Anything else Upstash returns (413 for an
oversized request, 429 for quota, 5xx) is not documented [?]. The documented trigger for the 413-like case is the
message `ERR max request size exceeded` **[D]** (troubleshooting page), whose HTTP status is [?].

A single-command request whose command returns a Redis error is `400` with `{"error": …}` **[D]** (the docs list a failed
command execution under `400`). A pipeline always returns `200` with one `{"result"}` or `{"error"}` object per command, in
order **[D, S]**.

### Pipelines (`/pipeline`)

Commands run in order and are **not atomic**: other clients may interleave **[D]**. The response is an array with one
object per command **[D]**. A failing command does not stop later ones **[D, S]**. If the request itself cannot be
parsed (not JSON, not an array of arrays), the whole response is a single `400` `{"error": …}` [I]. The TypeScript SDK
splits auto-pipelined work into batches of at most 1000 commands **[S]**; the server limit should be at least that.

### Transactions (`/multi-exec`)

Behaves as `MULTI`, the commands, `EXEC` **[D]**. The success response has the same shape as a pipeline's. The
transaction is *discarded as a whole*, answered by a single `{"error": …}` object, for a syntax error, an unsupported
command, or a command exceeding the request-size or quota limits **[D]**. A command that is valid but fails at
execution time does **not** abort the others: the failure appears as that command's `{"error"}` item and the rest are
applied **[D]**. This is Redis's own `EXEC` semantics and Invar already implements it. `WATCH`, `UNWATCH` and `DISCARD`
are unsupported **[D]**.

### Server-sent events (`/subscribe`, `/psubscribe`)

The request is `POST` with `Accept: text/event-stream`; the SDK also sends `Cache-Control: no-cache` and
`Connection: keep-alive` **[S]**. The response is a long-lived `text/event-stream` [I]. The channel (or pattern) is the
path segment **[D, S]**; one request per channel is what the SDK sends, but the docs imply the path form permits
several [?].

Each event is a line `data: <payload>` **[D]**. The SDK reads the stream line by line, takes lines that start with
`data: `, and parses the payload by splitting on the first commas **[S]**:

```
subscribe,<channel>,<subscription count>
message,<channel>,<message>
psubscribe,<pattern>,<subscription count>
pmessage,<pattern>,<channel>,<message>
```

The first two fields cannot contain a comma (a channel name containing `,` would be mis-parsed by the SDK; the server
need not guard against that). The message is the remainder of the line, commas included. The SDK stops by aborting the
HTTP request, so the server must treat a client disconnect as `UNSUBSCRIBE` and release the subscription [S].
A message containing a newline cannot be framed on one `data:` line; how Upstash handles this is [?] (see
Unresolved Questions).

### Read-your-writes sync token

Both SDKs send `Upstash-Sync-Token: <token>` on every request (the first one is empty) and store the value of the
response header of the same name **[S]**. Upstash needs it because reads may be served from lagging replicas **[D]**.
Invar has a single writer per store and no replicas, so every request already observes all earlier acknowledged writes.
The server accepts and ignores the request header. Both SDKs tolerate its absence from responses (`?? ""` in
TypeScript, `if sync_token` in Python) **[S]**, so omitting it is safe. See Unresolved Questions for the case for
emitting one anyway.

### Telemetry headers

`Upstash-Telemetry-Runtime`, `-Platform`, `-Sdk` and `-Retry` are sent by the SDKs **[S]**. They are ignored.

### Compatibility claims

What this RFC commits to: the TypeScript and Python SDK behaviours described above, as of the commits cited, for
every command Invar implements. It does not commit to byte-for-byte equivalence with Upstash's servers, and it cannot,
because the points marked [?] are not observable without an Upstash database (Appendix A).

### Command availability over HTTP

Upstash states that over REST the connection commands are limited to `PING` and `ECHO`, blocking list/zset/stream
commands are unsupported, and `WATCH`/`UNWATCH`/`DISCARD` are unsupported **[D]**. Invar proposes to **reject the same
set**, plus the commands that only make sense on a long-lived connection (`SELECT`, `AUTH`, `HELLO`, `CLIENT`, `MULTI`,
`EXEC`, `SUBSCRIBE`-family and `MONITOR` as ordinary commands, `QUIT`, `RESET`), with `400` and
`{"error": "ERR unsupported command '<name>'"}` [Invar]. The error text is ours, not Upstash's.

Rationale: an HTTP request that parks indefinitely on `BLPOP` ties up a connection and has no good cancellation story
besides a client timeout; and a request that `SELECT`s a database has no session to carry the selection forward.

## Part 2: Implementation in Invar

### Placement

A new module `src/redis/http/` in the existing `redis` crate. It needs `Session`, `commands::enqueue_command`, the
`RespValue` type and the `WatchRegistry`/`PubSubRegistry`, all of which live there. A separate crate would have to
re-export that internals surface; it is a reasonable later move if the HTTP dependency footprint matters.

### Request flow

```
hyper service  ->  authenticate  ->  parse (path | json | pipeline | multi-exec)
               ->  Vec<Vec<Bytes>>  ->  Session::new_with_pubsub(...)
               ->  enqueue_command(&mut session, &args)  ->  Vec<RespValue>
               ->  encode (json | base64 | resp2)  ->  response
```

* **One `Session` per request**, constructed exactly as the RESP listener does (`Session::new_with_pubsub`), on db 0 with
  the configured conflict-retry count. HTTP sessions are not registered with `server::register_session`, so they do not
  appear in `CLIENT LIST` or inflate the connected-clients count.
* **Single command**: `enqueue_command` once; its one reply is the result. A `RespValue::Error` becomes `400`.
* **Pipeline**: `enqueue_command` per row on the same `Session`, collecting each reply as an item. Not wrapped in
  `MULTI`.
* **`/multi-exec`**: `MULTI`, then `enqueue_command` per row, then `EXEC`. Invar's `abort_in_tx` already models "reject
  at queue time and dirty the transaction": the first queue-time error aborts, `EXEC` is not sent, and the response is
  `400` with that error. Otherwise `EXEC`'s array of replies is mapped item by item. Invar already guarantees the
  atomicity, so no new transactional code is needed.
* **Restricted commands** are filtered before dispatch, as above, and counted as an enqueue-time error in
  `/multi-exec`.

### Shared state with the RESP listener

`RedisListener` currently owns the `WatchRegistry` and `PubSubRegistry` privately. The HTTP listener needs clones of
the same `Arc`s. The smallest change is two accessor methods on `RedisListener` (or a small `Shared { store, registry,
pubsub, conflict_retry_count }` struct that both listeners are built from). No behavioural change to the RESP path.
Because that touches `RedisListener`, it is called out in Unresolved Questions per the project's churn rule.

### Server and dependencies

The existing hand-rolled HTTP server in `src/main.rs` (for `/livez`, `/readyz`, `/metrics`) reads only a request line
and always closes the connection; it cannot do keep-alive, request bodies, chunked transfer or streaming responses, all
of which this protocol needs. This RFC proposes `hyper` 1.x (with `http1`, `server`) and `hyper-util`, plus
`percent-encoding` and `base64`. `serde_json` is already a dependency; elements must be read with `RawValue` (or the
`arbitrary_precision` feature) so that number text is preserved. No TLS: terminate it in front of Invar. The SDKs accept
`http://` URLs (the TypeScript URL check is `^https?://`).

### Limits

* **Request body size**: `--http-max-body` (`INVAR_HTTP_MAX_BODY`), default 10 MiB, matching Upstash's documented
  limit for its smaller plans **[D]**. Exceeding it is rejected before the body is read in full.
* **Pipeline length**: at most as many commands as the body size admits, but not fewer than 1000 **[S]**.
* **Argument limits** reuse the RESP decoder's `max_bulk_len` so both front ends agree.
* **Header and idle timeouts** to bound slow-client resource use.

### Lifecycle and metrics

The listener starts only if `--http-addr` is given and is part of the same shutdown path as the RESP listener and the
metrics server: stop accepting, let in-flight requests finish (bounded), then close the store. SSE streams are
cancelled at shutdown. Counters follow the `metrics` crate usage already in the tree (requests by endpoint and status,
rejected-auth count, in-flight SSE streams).

### Configuration

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `--http-addr` | `INVAR_HTTP_ADDR` | unset (disabled) | enable the listener on this address |
| `--http-token` | `INVAR_HTTP_TOKEN` | none | bearer token; required when enabled |
| `--http-max-body` | `INVAR_HTTP_MAX_BODY` | 10 MiB | max request body |

`--http-addr` follows the precedent of `--metrics-addr`: presence enables the feature. The token is compared in
constant time. It must not be logged.

## Testing

* **Rust unit tests** for the pure parts: path-form decoding (including binary and `%2F`), body-as-last-argument and
  query-parameter appending, JSON-array to `Vec<Bytes>` (number text preserved, rejections), `RespValue` to JSON, the
  base64 rule (`OK` unencoded, `PONG` encoded, nested arrays, error strings untouched), and status-code mapping.
* **Integration tests** (the project's preferred level). Deno can import `npm:@upstash/redis`, so the real TypeScript
  SDK is driven against a daemon started with `--http-addr`/`--http-token` by `run-tests.sh`. Cases: every endpoint,
  SDK defaults (base64 off), `responseEncoding: "base64"`, auto-pipelining, `multi()`, `Script.exec()` falling back from
  `EVALSHA` to `EVAL`, `subscribe`, and a raw-`fetch` file for status codes and path-form behaviour the SDK never sends.
* **Cross-listener tests**: write over HTTP and read over RESP, `PUBLISH` over HTTP received by a RESP subscriber, an
  HTTP write invalidating a RESP `WATCH`.
* **Python SDK** is not exercised by the Deno suite. A CI job running a small `upstash-redis` script against the daemon
  is proposed as a follow-up, because Python is the SDK whose default (base64) differs from TypeScript's.
* **Upstream test suites as a smoke test, not a gate**: `redis-js` and `redis-python` have large suites that need
  `UPSTASH_REDIS_REST_URL`/`_TOKEN`. Running them against Invar will show failures for commands Invar does not
  implement, which is useful as a compatibility report but not as a pass/fail criterion.

# Drawbacks

* **An undocumented target.** The protocol is defined by one docs page and two SDKs; Upstash can change it without
  notice, and this RFC's [I] and [?] items are our best reading. A faithful implementation of the SDKs is not the same as
  being a drop-in for every use of Upstash's API.
* **New dependency surface and a second network listener** in a project that currently has none beyond a hand-rolled
  probe server. More code that parses untrusted input; one more thing to authenticate and rate-limit.
* **A security regression risk.** The RESP listener accepts `AUTH` and ignores it (Invar has no authentication). The
  HTTP listener introduces the first credential in the system. A shared bearer token, sent in cleartext unless TLS is
  terminated in front, is weaker than it looks, and the `_token` query parameter puts it in access logs.
* **`GET` can mutate.** `GET /del/foo` deletes foo. This is the documented Upstash behaviour, and `HEAD` has the same
  surface, but it is unusual HTTP and can be triggered by link prefetchers or cross-site requests if the token is ever
  ambient.
* **Semantics that differ from the RESP listener**: no `SELECT`, no blocking commands, no connection state. A command
  that works over RESP may be rejected over HTTP.
* **Edge-runtime clients are often public.** Upstash's answer is the read-only token; shipping without one means the
  token must never reach a browser.

# Rationale / Alternatives

* **Why imitate Upstash instead of designing an Invar HTTP API.** The value is in the SDK ecosystem that already
  exists. A bespoke API gets none of it.
* **Why a listener and not a gateway.** An Upstash-compatible proxy in front of Invar's RESP port (several
  community projects do this) would work today, but adds a process and a hop to a project whose goal is operational
  simplicity, and cannot share `PUBLISH`/`WATCH` state in-process or map transactions onto a single KV transaction.
* **Why per-request sessions.** It is what the protocol means: there is no connection to carry state, and Upstash
  rejects the commands that would need one.
* **Why `hyper` and not `axum`/`actix`.** The routing is four fixed shapes plus a catch-all; a framework adds more
  dependencies than the problem needs. A hand-rolled server (as for `/metrics`) cannot do bodies, keep-alive or
  streaming correctly.
* **Why reject `null`/nested args instead of coercing.** The SDKs never send them, Upstash's behaviour is unknown, and
  silently stringifying `null` to `"null"` into a durable store is the kind of unintended modification the project's
  first rule forbids.
* **Why reject instead of ignore unsupported commands.** The same rule: a silent no-op on `SELECT 3` would write to the
  wrong keyspace.

# Unresolved Questions

> **Open Question (decision for the maintainer):** Flag shape. The repository's `CLAUDE.md` says "`--redis` is the only
> supported protocol flag", but `main.rs` has no such flag (RESP is always on, with `--redis-addr`). This RFC proposes
> `--http-addr` (presence enables), mirroring `--metrics-addr`. Is that acceptable, or should protocols become explicit
> opt-in flags (`--redis`, `--http`) with RESP's default behaviour preserved?

> **Open Question (decision):** Authentication. A single mandatory bearer token (proposed). Should a **read-only token**
> be supported, given that browser/edge use is the main reason the protocol exists? If so, the read-only/write
> classification of every command needs an owner, because Upstash also restricts some powerful read commands (it names `SCAN` and
> `KEYS`) for that token **[D]**, and the full list is not documented.

> **Open Question (decision):** Dependency. Adding `hyper`, `hyper-util`, `base64` and `percent-encoding`. Acceptable?

> **Open Question (decision):** `RedisListener` exposes its registries (two accessors or a shared state struct). This is
> a small change to existing code; confirm it is acceptable per the churn rule.

> **Open Question (decision):** Blocking commands and the command deny-list. Mirror Upstash (proposed) or allow
> blocking commands with a server-side timeout?

> **Open Question [?]:** Observable Upstash behaviours that nothing public settles. They are listed with probe commands in
> Appendix A: error body and status for 401/413/malformed JSON; query-parameter ordering and valueless parameters;
> boolean and `null` arguments; whether `PONG` and other status replies are really base64 encoded; `HEAD` semantics (does
> it execute the command?); behaviour of `OPTIONS` and CORS (the documented 405 list suggests preflight is not
> served by the data plane, which would block direct browser use with non-simple headers, yet browser use with the
> read-only token is advertised); SSE event framing for messages containing newlines, keep-alive comments, and whether
> `/subscribe/a/b` is legal. **Is there an Upstash database I can run Appendix A against, or would you rather settle
> each point by decision?** Per the project rule not to speculate on public API behaviour, nothing in this list will be
> implemented from a guess.

> **Open Question [?]:** Should Invar emit `Upstash-Sync-Token`? It is unnecessary for correctness here. The upstream
> SDK test-suites assert that the token changes after a write, so an opaque, strictly increasing token would make those
> suites pass without costing anything, but it would also be a header whose meaning Invar does not honour.

> **Open Question:** `MONITOR`. Documented, no SDK uses it, and Invar does not implement `MONITOR` over RESP. Out of
> scope until it exists over RESP.

> **Open Question:** `Upstash-Response-Format: resp2`. Implement in the first version or defer? No client uses it.

> **Open Question:** TLS. Left to a reverse proxy. Is a built-in `--http-tls-cert/--http-tls-key` ever wanted for the
> edge/browser scenario?

> **Open Question:** Rate limiting and connection limits on the HTTP listener, which the RESP listener does not have.

# Appendix A: Probes to settle the [?] items

To be run against a real Upstash database by someone with one; each answers a question above. `$U` is the REST URL
and `$T` the token.

```
# error shapes and statuses
curl -si $U/get/foo                                     # 401 body and headers
curl -si $U -H "Authorization: Bearer $T" -d 'not json' # malformed body
curl -si $U -H "Authorization: Bearer $T" -d '["SET","k",null]'
curl -si $U -H "Authorization: Bearer $T" -d '["SET","k",true]' ; curl -s $U/get/k -H "Authorization: Bearer $T"
curl -si -X DELETE $U/get/foo -H "Authorization: Bearer $T"   # 405 body
curl -si -X OPTIONS $U -H "Origin: https://example.com" -H "Access-Control-Request-Method: POST" \
     -H "Access-Control-Request-Headers: authorization,content-type"
curl -si -I $U/set/h/1 -H "Authorization: Bearer $T"; curl -s $U/get/h -H "Authorization: Bearer $T"  # does HEAD execute?

# query parameter semantics
curl -s -X POST -d v "$U/set/q?EX=100&NX" -H "Authorization: Bearer $T"; curl -s $U/ttl/q -H "Authorization: Bearer $T"
curl -s -X POST -d v "$U/set/q2?NX&EX=100" -H "Authorization: Bearer $T"

# base64 rules
curl -s $U/ping -H "Authorization: Bearer $T" -H "Upstash-Encoding: base64"
curl -s $U/type/k -H "Authorization: Bearer $T" -H "Upstash-Encoding: base64"
curl -s $U/set/a/OK -H "Authorization: Bearer $T"; curl -s $U/get/a -H "Authorization: Bearer $T" -H "Upstash-Encoding: base64"
curl -s $U/foo -H "Authorization: Bearer $T" -H "Upstash-Encoding: base64"   # is the error encoded?

# transactions
curl -si $U/multi-exec -H "Authorization: Bearer $T" -d '[["SET","a","1"],["NOPE"]]'          # status of a discarded txn
curl -si $U/multi-exec -H "Authorization: Bearer $T" -d '[["SET","a","1"],["INCR","a2x"],["LPUSH","a","x"]]'

# SSE
curl -sN -X POST $U/subscribe/a/b -H "Authorization: Bearer $T" -H "Accept: text/event-stream"
curl -s $U/publish/a/$'line1\nline2' -H "Authorization: Bearer $T"
curl -sN -X POST "$U/psubscribe/a*" -H "Authorization: Bearer $T" -H "Accept: text/event-stream"

# limits
head -c 11000000 /dev/zero | tr '\0' x | curl -si -X POST --data-binary @- $U/set/big -H "Authorization: Bearer $T"
```

# Appendix B: Sources

All read at the commits shown; none of this was observed against a running Upstash service.

* `upstash/docs` @ `9b28301` (2026-10-28): `redis/features/restapi.mdx` (the only prose specification),
  `redis/howto/readyourwrites.mdx`, `redis/troubleshooting/max_request_size_exceeded.mdx`,
  `redis/sdks/ts/retries.mdx`, `redis/sdks/ts/pipelining/*`.
* `upstash/redis-js` @ `4d3f58e` (v1.40.0): `packages/redis/pkg/http.ts` (transport, headers, retry, base64, SSE reader),
  `commands/command.ts` (argument serialization), `commands/subscribe.ts`, `commands/psubscribe.ts` (SSE paths and
  payload parsing), `pipeline.ts` (`/pipeline`, `/multi-exec`), `auto-pipeline.ts` (1000-command batches),
  `script.ts` (`noscript` fallback), `read-your-writes.test.ts`, `http.test.ts`.
* `upstash/redis-python` @ `78567ea` (v1.8.0): `upstash_redis/http.py` (headers, base64 default, retry, sync token),
  `client.py` (`/pipeline`, `/multi-exec` URLs), `tests/test_read_your_writes.py`, `tests/commands/**/test_ping.py`.
