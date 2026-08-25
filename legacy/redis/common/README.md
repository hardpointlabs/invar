# Redis Common Utils

This module contains 4 things:

1. Command operation queuing logic for redcon's read/accept loop, for dispatching operations to a `KeyValueStore`
2. A `QueuedOp` type which unifies DB operations with corresponding Redis wire operations
3. Exposes a `Session` that callers can use to obtain standard key prefixes without having to pollute their code with current db/connection state
4. Housekeeping logic for tracking clients which are blocked, 'watching' for value changes on certain keys

# Usage

The main use of this package is for developers wishing to implement new Redis commands. You should create functions that yield a `QueuedOp`, and take a `Session` if needed.

Let's say we wanted to implement a new Redis command. After adding a new entry to the `switch` block in `listener.go`, we should wire in our function:

```golang
func DoSomething(session *common.Session, key []byte, elements ...[]byte) common.QueuedOp {
    dbOp := func(tx kv.Tx) (any, error) {
        // your key-value operations
    }
    wireOp := func(conn redcon.Conn, result any, err error) {
        // your redis wire operations
        // called after dbOp is invoked
        // dbOp's value or error is passed in here,
        // along with a connection object you can write to
    }

    // return a QueuedOp and let the upstream Tx queuing mechanism know
    // if this command is mutating and needs to run in a write transaction
    return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}
```
