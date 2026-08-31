# Redis Compatibility

Invar aims for compatibility with major projects which depend on Redis such as [BullMQ](https://bullmq.io). Concretely,
that means we aim for broad compatibility with comands as of Redis' 6.2 release, with the following provisos:

* they should be feasible on the underlying LSM store without compromising behavior (generally have the time/space complexity)
* they should semantically make sense in the context of Invar (cluster management doesn't, for example)

At present we have no plans to implement the following command groups:

* Geospatial: this is rather a large maintenance commitment for a use case we've never been asked about so far
* Cluster management: this one doesn't semantically make sense given Invar's single-node/single-writer model
* RBAC: Authz/authn is strictly out of scope for Invar itself. If you need this, consider our [managed options](https://hardpoint.dev).

---

## Legend

- ✅ implemented
- 🚧 not yet implemented
- 🚫 no plan to implement

*note*: entire command groups which currently have no support, but we have not ruled out implementing, are omitted
from the below list.

## Contributing

Contributions for extending our command coverage are greatly appreciated! Before implementing anything, please consult
the table of commands below. For commands marked as not yet implemented (`🚧`), you are more than welcome to go ahead
and submit a PR with an implementation proposal. For commands explicitly marked as off the roadmap (`🚫`), please create
and issue to discuss the merits of the change before spending time implementing anything.

---

## String commands

| Command | Status | Notes |
|---------|--------|-------|
| APPEND | ✅ | |
| DECR | ✅ | |
| DECRBY | ✅ | |
| GET | ✅ | |
| GETDEL | ✅ | |
| GETEX | ✅ | |
| GETRANGE | ✅ | |
| GETSET | ✅ | |
| INCR | ✅ | |
| INCRBY | ✅ | |
| INCRBYFLOAT | ✅ | |
| MGET | ✅ | |
| MSET | ✅ | |
| MSETNX | ✅ | |
| PSETEX | ✅ | |
| SET | ✅ | |
| SETEX | ✅ | |
| SETNX | ✅ | |
| SETRANGE | ✅ | |
| STRLEN | ✅ | |
| SUBSTR | ✅ | |

---

## Hash commands

| Command | Status | Notes |
|---------|--------|-------|
| HDEL | ✅ | |
| HEXISTS | ✅ | |
| HGET | ✅ | |
| HGETALL | ✅ | |
| HINCRBY | ✅ | |
| HINCRBYFLOAT | ✅ | |
| HKEYS | ✅ | |
| HLEN | ✅ | |
| HMGET | ✅ | |
| HMSET | ✅ | |
| HRANDFIELD | ✅ | |
| HSCAN | ✅ | |
| HSET | ✅ | |
| HSETNX | ✅ | |
| HSTRLEN | ✅ | |
| HVALS | ✅ | |

---

## List commands

| Command | Status | Notes                                                      |
|---------|--------|------------------------------------------------------------|
| BLMOVE | 🚧 |                                                            |
| BLPOP | 🚧 |                                                            |
| BRPOP | 🚧 |                                                            |
| BRPOPLPUSH | 🚧 |                                                            |
| LINDEX | ✅ |                                                            |
| LINSERT | ✅ |                                                            |
| LLEN | ✅ |                                                            |
| LMOVE | 🚧 |                                                            |
| LPOP | ✅ |                                                            |
| LPOS | 🚧 | missing RANK/COUNT options |
| LPUSH | ✅ |                                                            |
| LPUSHX | ✅ |                                                            |
| LRANGE | ✅ |                                                            |
| LREM | ✅ |                                                            |
| LSET | ✅ |                                                            |
| LTRIM | ✅ |                                                            |
| RPOP | ✅ |                                                            |
| RPOPLPUSH | ✅ |                                                            |
| RPUSH | ✅ |                                                            |
| RPUSHX | ✅ |                                                            |

---

## Set commands

| Command | Status | Notes |
|---------|--------|-------|
| SADD | ✅ | |
| SCARD | ✅ | |
| SDIFF | ✅ | |
| SDIFFSTORE | ✅ | |
| SINTER | ✅ | |
| SINTERSTORE | ✅ | |
| SISMEMBER | ✅ | |
| SMEMBERS | ✅ | |
| SMISMEMBER | 🚧 | |
| SMOVE | ✅ | |
| SPOP | ✅ | |
| SRANDMEMBER | ✅ | |
| SREM | ✅ | |
| SSCAN | ✅ | |
| SUNION | ✅ | |
| SUNIONSTORE | ✅ | |

---

## Sorted set commands

| Command | Status | Notes |
|---------|--------|-------|
| BZPOPMAX | ✅ | |
| BZPOPMIN | ✅ | |
| ZADD | ✅ | |
| ZCARD | ✅ | |
| ZCOUNT | ✅ | |
| ZDIFF | ✅ | |
| ZDIFFSTORE | ✅ | |
| ZINCRBY | ✅ | |
| ZINTER | ✅ | |
| ZINTERSTORE | ✅ | |
| ZLEXCOUNT | ✅ | |
| ZMSCORE | ✅ | |
| ZPOPMAX | ✅ | |
| ZPOPMIN | ✅ | |
| ZRANDMEMBER | ✅ | |
| ZRANGE | ✅ | |
| ZRANGEBYLEX | ✅ | |
| ZRANGEBYSCORE | ✅ | |
| ZRANGESTORE | ✅ | |
| ZRANK | ✅ | |
| ZREM | ✅ | |
| ZREMRANGEBYLEX | ✅ | |
| ZREMRANGEBYRANK | ✅ | |
| ZREMRANGEBYSCORE | ✅ | |
| ZREVRANGE | ✅ | |
| ZREVRANGEBYLEX | ✅ | |
| ZREVRANGEBYSCORE | ✅ | |
| ZREVRANK | ✅ | |
| ZSCAN | 🚧 | |
| ZSCORE | ✅ | |
| ZUNION | ✅ | |
| ZUNIONSTORE | ✅ | |


---

## Bitmap commands

| Command | Status | Notes |
|---------|--------|-------|
| BITCOUNT | ✅ | Supports BYTE and BIT range modes |
| BITFIELD | 🚧 | |
| BITFIELD_RO | 🚧 | |
| BITOP | ✅ | Supports AND, OR, XOR, NOT, DIFF, DIFF1, ANDOR, ONE |
| BITPOS | ✅ | Supports BYTE and BIT range modes |
| GETBIT | ✅ | |
| SETBIT | ✅ | |

---

## HyperLogLog commands

| Command | Status | Notes |
|---------|--------|-------|
| PFADD | ✅ | |
| PFCOUNT | ✅ | |
| PFDEBUG | 🚧 | |
| PFMERGE | ✅ | |
| PFSELFTEST | 🚧 | |

---

## Stream commands

| Command | Status | Notes |
|---------|--------|-------|
| XADD | ✅ | Supports NOMKSTREAM, MAXLEN, MINID trimming |
| XDEL | ✅ | |
| XINFO STREAM | ✅ | |
| XLEN | ✅ | |
| XREAD | ✅ | Multi-stream support; COUNT option; BLOCK not supported |
| XRANGE | ✅ | With COUNT option |
| XREVRANGE | ✅ | With COUNT option |
| XSETID | ✅ | |
| XTRIM | ✅ | MAXLEN and MINID modes, approximate (~) treated as exact |
| XACK | 🚧 | Consumer group support not implemented |
| XAUTOCLAIM | 🚧 | Consumer group support not implemented |
| XCLAIM | 🚧 | Consumer group support not implemented |
| XGROUP | 🚧 | Consumer group support not implemented |
| XINFO CONSUMERS | 🚧 | Consumer group support not implemented |
| XINFO GROUPS | 🚧 | Consumer group support not implemented |
| XPENDING | 🚧 | Consumer group support not implemented |
| XREADGROUP | 🚧 | Consumer group support not implemented |

---

## Pub/Sub commands

**Note:** Like Redis, Pub/Sub commands sent to Invar are _not_ persisted and exist in memory only. They're also not subject to regular transactional guarantees. They should therefore be treated as best-effort.

| Command | Status | Notes                 |
|---------|--------|-----------------------|
| PSUBSCRIBE | ✅ |                       |
| PUBLISH | ✅ |                       |
| PUBSUB CHANNELS | ✅ |                       |
| PUBSUB NUMPAT | ✅ |                       |
| PUBSUB NUMSUB | ✅ |                       |
| PUNSUBSCRIBE | ✅ |                       |
| SPUBLISH | ✅ | Alias for PUBLISH     |
| SSUBSCRIBE | ✅ | Alias for SUBSCRIBE   |
| SUBSCRIBE | ✅ |                       |
| SUNSUBSCRIBE | ✅ | Alias for UNSUBSCRIBE |
| UNSUBSCRIBE | ✅ |                       |

---

## Transaction commands

| Command | Status | Notes |
|---------|--------|-------|
| DISCARD | ✅ | |
| EXEC | ✅ | |
| MULTI | ✅ | |
| UNWATCH | 🚧 | |
| WATCH | 🚧 | |

---

## Scripting commands

| Command | Status | Notes                                                                                                                                                                                                                                                                 |
|---------|--------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| EVAL | ✅ | [Piccolo](https://github.com/kyren/piccolo)-backed Lua interpreter; core stdlib (base, coroutine, math, string, table) + `tonumber`, `unpack`, `table.insert`/`remove`/`sort`/`concat`, `cmsgpack.unpack`, `cjson.encode`; `redis.call()` and `redis.pcall()` exposed |
| EVALSHA | ✅ | Looks up previously cached script by SHA1 digest                                                                                                                                                                                                                      |

---

## Connection commands

| Command | Status | Notes |
|---------|--------|-|
| AUTH | 🚫 | |
| CLIENT CACHING | 🚫 | |
| CLIENT GETNAME | ✅ | Per-connection name via CLIENT SETNAME |
| CLIENT GETREDIR | 🚫 | |
| CLIENT ID | ✅ | |
| CLIENT INFO | ✅ | |
| CLIENT KILL | 🚫 | |
| CLIENT LIST | ✅ | Reports the current connection only |
| CLIENT NO-EVICT | 🚫 | |
| CLIENT NO-TOUCH | 🚫 | |
| CLIENT PAUSE | 🚫 | |
| CLIENT REPLY | 🚫 | |
| CLIENT SETNAME | ✅ | |
| CLIENT SETINFO | ✅ | LIB-NAME / LIB-VER |
| CLIENT TRACKING | 🚫 | |
| CLIENT TRACKINGINFO | 🚫 | |
| CLIENT UNBLOCK | 🚫 | |
| CLIENT UNPAUSE | 🚫 | |
| ECHO | ✅ | |
| HELLO | ✅ | RESP2 only; RESP3 refused with NOPROTO |
| PING | ✅ | |
| QUIT | ✅ | |
| RESET | 🚧 | |
| SELECT | ✅ | |

---

## Server commands

| Command | Status | Notes                                       |
|---------|--------|---------------------------------------------|
| ACL CAT | 🚫 |                                             |
| ACL DELUSER | 🚫 |                                             |
| ACL DRYRUN | 🚫 |                                             |
| ACL GENPASS | 🚫 |                                             |
| ACL GETUSER | 🚫 |                                             |
| ACL LIST | 🚫 |                                             |
| ACL LOAD | 🚫 |                                             |
| ACL LOG | 🚫 |                                             |
| ACL SAVE | 🚫 |                                             |
| ACL SETUSER | 🚫 |                                             |
| ACL USERS | 🚫 |                                             |
| ACL WHOAMI | 🚫 |                                             |
| BGREWRITEAOF | 🚫 |                                             |
| BGSAVE | ✅ | no-op                                       |
| COMMAND | 🚫 |                                             |
| COMMAND COUNT | 🚫 |                                             |
| COMMAND DOCS | 🚫 |                                             |
| COMMAND GETKEYS | 🚫 |                                             |
| COMMAND GETKEYSANDFLAGS | 🚫 |                                             |
| COMMAND INFO | 🚫 |                                             |
| COMMAND LIST | 🚫 |                                             |
| CONFIG GET | 🚫 |                                             |
| CONFIG RESETSTAT | 🚫 |                                             |
| CONFIG REWRITE | 🚫 |                                             |
| CONFIG SET | 🚫 |                                             |
| DBSIZE | ✅ | ⚠️ Runs in O(n) time                        |
| DEBUG | 🚫 |                                             |
| FAILOVER | 🚫 |                                             |
| FLUSHALL | ✅ |                                             |
| FLUSHDB | ✅ |                                             |
| INFO | ✅ | Reports redis_version:6.2.0 + invar_version |
| LASTSAVE | 🚫 |                                             |
| LATENCY DOCTOR | 🚫 |                                             |
| LATENCY GRAPH | 🚫 |                                             |
| LATENCY HISTORY | 🚫 |                                             |
| LATENCY LATEST | 🚫 |                                             |
| LATENCY RESET | 🚫 |                                             |
| LOLWUT | ✅ | Returns version info instead of ASCII art   |
| MEMORY DOCTOR | 🚫 |                                             |
| MEMORY MALLOC-STATS | 🚫 |                                             |
| MEMORY PURGE | 🚫 |                                             |
| MEMORY STATS | 🚫 |                                             |
| MEMORY USAGE | 🚫 |                                             |
| MODULE LIST | ✅ |                                             |
| MODULE LOAD | 🚫 |                                             |
| MODULE LOADEX | 🚫 |                                             |
| MODULE UNLOAD | 🚫 |                                             |
| MONITOR | 🚫 |                                             |
| PSYNC | ✅ | No-op                                       |
| REPLCONF | 🚫 |                                             |
| REPLICAOF | 🚫 |                                             |
| RESTORE-ASKING | 🚫 |                                             |
| ROLE | 🚫 |                                             |
| SAVE | ✅ |                                             |
| SHUTDOWN | 🚫 |                                             |
| SLAVEOF | 🚫 |                                             |
| SLOWLOG GET | 🚫 |                                             |
| SLOWLOG LEN | 🚫 |                                             |
| SLOWLOG RESET | 🚫 |                                             |
| SWAPDB | 🚫 |                                             |
| SYNC | ✅ |                                             |
| TIME | ✅ |                                             |

---

## Generic (keys) commands

| Command | Status | Notes |
|---------|--------|-------|
| COPY | 🚧 | |
| DEL | ✅ | |
| DUMP | 🚧 | |
| EXISTS | ✅ | |
| EXPIRE | ✅ | |
| EXPIREAT | 🚧 | |
| EXPIRETIME | 🚧 | |
| KEYS | 🚧 | |
| MIGRATE | 🚧 | |
| MOVE | ✅ | |
| OBJECT ENCODING | 🚧 | |
| OBJECT FREQ | 🚧 | |
| OBJECT IDLETIME | ✅ | No-op, returns nil |
| OBJECT REFCOUNT | 🚧 | |
| PERSIST | ✅ | |
| PEXPIRE | ✅ | |
| PEXPIREAT | 🚧 | |
| PEXPIRETIME | 🚧 | |
| PTTL | ✅ | |
| RANDOMKEY | 🚧 | |
| RENAME | ✅ | |
| RENAMENX | ✅ | |
| RESTORE | 🚧 | |
| SCAN | ✅ | MATCH, COUNT, and TYPE options supported |
| SORT | 🚧 | |
| SORT_RO | 🚧 | |
| TOUCH | 🚧 | |
| TTL | ✅ | |
| TYPE | ✅ | |
| UNLINK | ✅ | Equivalant behavior to `DEL` |
| WAIT | ✅ | No-op |

---

## JSON module commands

| Command | Status | Notes |
|---------|--------|-------|
| JSON.ARRAPPEND | ✅ | |
| JSON.ARRINDEX | ✅ | |
| JSON.ARRINSERT | ✅ | |
| JSON.ARRLEN | ✅ | |
| JSON.ARRPOP | ✅ | |
| JSON.ARRTRIM | ✅ | |
| JSON.CLEAR | ✅ | |
| JSON.DEBUG | 🚧 | |
| JSON.DEBUG MEMORY | 🚧 | |
| JSON.DEL | ✅ | |
| JSON.FORGET | 🚧 | |
| JSON.GET | ✅ | |
| JSON.MERGE | 🚧 | |
| JSON.MGET | ✅ | |
| JSON.MSET | 🚧 | |
| JSON.NUMINCRBY | ✅ | |
| JSON.NUMMULTBY | ✅ | |
| JSON.OBJKEYS | ✅ | |
| JSON.OBJLEN | ✅ | |
| JSON.RESP | ✅ | |
| JSON.SET | ✅ | |
| JSON.STRAPPEND | ✅ | |
| JSON.STRLEN | ✅ | |
| JSON.TOGGLE | 🚧 | |
| JSON.TYPE | ✅ | |

---

## Bloom Filter module commands

| Command | Status | Notes |
|---------|--------|-------|
| BF.ADD | ✅ | |
| BF.CARD | 🚧 | |
| BF.EXISTS | ✅ | |
| BF.INFO | ✅ | |
| BF.INSERT | ✅ | |
| BF.LOADCHUNK | 🚧 | |
| BF.MADD | ✅ | |
| BF.MEXISTS | ✅ | |
| BF.RESERVE | ✅ | |
| BF.SCANDUMP | 🚧 | |
