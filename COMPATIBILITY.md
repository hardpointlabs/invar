# Redis Compatibility

Status of all Redis 6.2 core commands (plus JSON and Bloom Filter module commands)
in this implementation.

**Note!** we have no plans to implement the following command groups:

* Streams
* Geospatial
* Cluster
* Scripting

---

## Legend

- ✅ — implemented
- 🚫 — not implemented

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

| Command | Status | Notes |
|---------|--------|-------|
| BLMOVE | 🚫 | |
| BLPOP | 🚫 | |
| BRPOP | 🚫 | |
| BRPOPLPUSH | 🚫 | |
| LINDEX | ✅ | |
| LINSERT | ✅ | |
| LLEN | ✅ | |
| LMOVE | 🚫 | |
| LPOP | ✅ | |
| LPOS | 🚫 | |
| LPUSH | ✅ | |
| LPUSHX | ✅ | |
| LRANGE | ✅ | |
| LREM | ✅ | |
| LSET | ✅ | |
| LTRIM | ✅ | |
| RPOP | ✅ | |
| RPOPLPUSH | 🚫 | |
| RPUSH | ✅ | |
| RPUSHX | ✅ | |

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
| SMISMEMBER | 🚫 | |
| SMOVE | ✅ | |
| SPOP | ✅ | |
| SRANDMEMBER | ✅ | |
| SREM | ✅ | |
| SSCAN | 🚫 | |
| SUNION | ✅ | |
| SUNIONSTORE | ✅ | |

---

## Sorted set commands

| Command | Status | Notes |
|---------|--------|-------|
| BZPOPMAX | 🚫 | Blocking variant, not implementing |
| BZPOPMIN | 🚫 | Blocking variant, not implementing |
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
| ZSCAN | 🚫 | |
| ZSCORE | ✅ | |
| ZUNION | ✅ | |
| ZUNIONSTORE | ✅ | |


---

## Bitmap commands

| Command | Status | Notes |
|---------|--------|-------|
| BITCOUNT | ✅ | Supports BYTE and BIT range modes |
| BITFIELD | 🚫 | |
| BITFIELD_RO | 🚫 | |
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
| PFDEBUG | 🚫 | |
| PFMERGE | ✅ | |
| PFSELFTEST | 🚫 | |

---

## Pub/Sub commands

| Command | Status | Notes |
|---------|--------|-------|
| PSUBSCRIBE | ✅ | |
| PUBLISH | ✅ | |
| PUBSUB CHANNELS | 🚫 | |
| PUBSUB NUMPAT | 🚫 | |
| PUBSUB NUMSUB | 🚫 | |
| PUNSUBSCRIBE | 🚫 | |
| SPUBLISH | 🚫 | |
| SSUBSCRIBE | 🚫 | |
| SUBSCRIBE | ✅ | |
| SUNSUBSCRIBE | 🚫 | |
| UNSUBSCRIBE | 🚫 | |

---

## Transaction commands

| Command | Status | Notes |
|---------|--------|-------|
| DISCARD | 🚫 | |
| EXEC | 🚫 | |
| MULTI | 🚫 | |
| UNWATCH | 🚫 | |
| WATCH | 🚫 | |

---

## Connection commands

| Command | Status | Notes |
|---------|--------|-------|
| AUTH | 🚫 | Not implementing |
| CLIENT CACHING | 🚫 | Not implementing |
| CLIENT GETNAME | 🚫 | Not implementing |
| CLIENT GETREDIR | 🚫 | Not implementing |
| CLIENT ID | ✅ | |
| CLIENT INFO | ✅ | |
| CLIENT KILL | 🚫 | Not implementing |
| CLIENT LIST | 🚫 | |
| CLIENT NO-EVICT | 🚫 | Not implementing |
| CLIENT NO-TOUCH | 🚫 | Not implementing |
| CLIENT PAUSE | 🚫 | Not implementing |
| CLIENT REPLY | 🚫 | Not implementing |
| CLIENT SETNAME | 🚫 | Not implementing |
| CLIENT TRACKING | 🚫 | Not implementing |
| CLIENT TRACKINGINFO | 🚫 | Not implementing |
| CLIENT UNBLOCK | 🚫 | Not implementing |
| CLIENT UNPAUSE | 🚫 | Not implementing |
| ECHO | ✅ | |
| HELLO | 🚫 | Not implementing |
| PING | ✅ | |
| QUIT | ✅ | |
| RESET | 🚫 | |
| SELECT | ✅ | |

---

## Server commands

| Command | Status | Notes |
|---------|--------|-------|
| ACL CAT | 🚫 | |
| ACL DELUSER | 🚫 | |
| ACL DRYRUN | 🚫 | |
| ACL GENPASS | 🚫 | |
| ACL GETUSER | 🚫 | |
| ACL LIST | 🚫 | |
| ACL LOAD | 🚫 | |
| ACL LOG | 🚫 | |
| ACL SAVE | 🚫 | |
| ACL SETUSER | 🚫 | |
| ACL USERS | 🚫 | |
| ACL WHOAMI | 🚫 | |
| BGREWRITEAOF | 🚫 | |
| BGSAVE | ✅ | |
| COMMAND | 🚫 | |
| COMMAND COUNT | 🚫 | |
| COMMAND DOCS | 🚫 | |
| COMMAND GETKEYS | 🚫 | |
| COMMAND GETKEYSANDFLAGS | 🚫 | |
| COMMAND INFO | 🚫 | |
| COMMAND LIST | 🚫 | |
| CONFIG GET | 🚫 | |
| CONFIG RESETSTAT | 🚫 | |
| CONFIG REWRITE | 🚫 | |
| CONFIG SET | 🚫 | |
| DBSIZE | ✅ | |
| DEBUG | 🚫 | |
| FAILOVER | 🚫 | |
| FLUSHALL | ✅ | |
| FLUSHDB | ✅ | |
| INFO | 🚫 | |
| LASTSAVE | 🚫 | |
| LATENCY DOCTOR | 🚫 | |
| LATENCY GRAPH | 🚫 | |
| LATENCY HISTORY | 🚫 | |
| LATENCY LATEST | 🚫 | |
| LATENCY RESET | 🚫 | |
| LOLWUT | ✅ | Returns version info instead of ASCII art |
| MEMORY DOCTOR | 🚫 | |
| MEMORY MALLOC-STATS | 🚫 | |
| MEMORY PURGE | 🚫 | |
| MEMORY STATS | 🚫 | |
| MEMORY USAGE | 🚫 | |
| MODULE LIST | ✅ | |
| MODULE LOAD | 🚫 | |
| MODULE LOADEX | 🚫 | |
| MODULE UNLOAD | 🚫 | |
| MONITOR | 🚫 | |
| PSYNC | ✅ | No-op |
| REPLCONF | 🚫 | |
| REPLICAOF | 🚫 | |
| RESTORE-ASKING | 🚫 | |
| ROLE | 🚫 | |
| SAVE | ✅ | |
| SHUTDOWN | 🚫 | |
| SLAVEOF | 🚫 | |
| SLOWLOG GET | 🚫 | |
| SLOWLOG LEN | 🚫 | |
| SLOWLOG RESET | 🚫 | |
| SWAPDB | 🚫 | |
| SYNC | ✅ | |
| TIME | ✅ | |

---

## Generic (keys) commands

| Command | Status | Notes |
|---------|--------|-------|
| COPY | 🚫 | |
| DEL | ✅ | |
| DUMP | 🚫 | |
| EXISTS | ✅ | |
| EXPIRE | ✅ | |
| EXPIREAT | 🚫 | |
| EXPIRETIME | 🚫 | |
| KEYS | 🚫 | |
| MIGRATE | 🚫 | |
| MOVE | ✅ | |
| OBJECT ENCODING | 🚫 | |
| OBJECT FREQ | 🚫 | |
| OBJECT IDLETIME | ✅ | No-op, returns nil |
| OBJECT REFCOUNT | 🚫 | |
| PERSIST | 🚫 | |
| PEXPIRE | 🚫 | |
| PEXPIREAT | 🚫 | |
| PEXPIRETIME | 🚫 | |
| PTTL | ✅ | |
| RANDOMKEY | 🚫 | |
| RENAME | ✅ | |
| RENAMENX | ✅ | |
| RESTORE | 🚫 | |
| SCAN | 🚫 | |
| SORT | 🚫 | |
| SORT_RO | 🚫 | |
| TOUCH | 🚫 | |
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
| JSON.DEBUG | 🚫 | |
| JSON.DEBUG MEMORY | 🚫 | |
| JSON.DEL | ✅ | |
| JSON.FORGET | 🚫 | |
| JSON.GET | ✅ | |
| JSON.MERGE | 🚫 | |
| JSON.MGET | ✅ | |
| JSON.MSET | 🚫 | |
| JSON.NUMINCRBY | ✅ | |
| JSON.NUMMULTBY | ✅ | |
| JSON.OBJKEYS | ✅ | |
| JSON.OBJLEN | ✅ | |
| JSON.RESP | ✅ | |
| JSON.SET | ✅ | |
| JSON.STRAPPEND | ✅ | |
| JSON.STRLEN | ✅ | |
| JSON.TOGGLE | 🚫 | |
| JSON.TYPE | ✅ | |

---

## Bloom Filter module commands

| Command | Status | Notes |
|---------|--------|-------|
| BF.ADD | ✅ | |
| BF.CARD | 🚫 | |
| BF.EXISTS | ✅ | |
| BF.INFO | ✅ | |
| BF.INSERT | ✅ | |
| BF.LOADCHUNK | 🚫 | |
| BF.MADD | ✅ | |
| BF.MEXISTS | ✅ | |
| BF.RESERVE | ✅ | |
| BF.SCANDUMP | 🚫 | |
