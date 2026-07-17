package common

import (
	"strconv"
	"sync/atomic"

	"github.com/hardpointlabs/invar/kv"
	"github.com/tidwall/redcon"
)

var globalSessionCounter atomic.Uint64

// key delimeters
const internalPrefix = "-"
const prefixSeparator = ":"

// public redis types for LSM tree entries (not private/internal types)
type RedisValueType byte

const (
	RedisString RedisValueType = iota
	RedisList
	RedisSet
	RedisSortedSet
	RedisHash
	RedisStream
	RedisVectorSet
	RedisBloom
	RedisJSON
)

type Session struct {
	kvs       kv.KeyValueStore
	Id        uint64     // Redis connection ID
	currentDB int32      // Current Redis DB this connection is operating on
	queue     []QueuedOp // nil when not in MULTI
	inMulti   bool
}

func NewSession(kvs kv.KeyValueStore) *Session {
	var s = &Session{}
	s.Id = globalSessionCounter.Add(1)
	s.currentDB = 0
	s.kvs = kvs
	return s
}

func (s *Session) EnterMulti() {
	s.inMulti = true
}

func (s *Session) ExitMulti(discard bool) {
	if !s.inMulti {
		panic("Not inside a MULTI block")
	}
	s.inMulti = false
	if discard {
		s.queue = nil
	}
}

// Enqueue an operation for later execution within a database transaction
func (s *Session) EnqueueOp(op QueuedOp) {
	if s.queue == nil {
		s.queue = []QueuedOp{op}
	} else {
		s.queue = append(s.queue, op)
	}
}

// Attempt to acquire a database transaction and execute pending operations
func (s *Session) DispatchPendingOps(conn redcon.Conn) {
	if s.inMulti {
		// scenario A: we're inside a MULTI block, do nothing
		return
	}

	// scenario B: we're not inside a MULTI block (either we never were or we just left one),
	// so we acquire a transaction and apply straight away
	if s.queue == nil {
		// No ops were enqueued — command was handled directly (e.g. strings, lists, JSON).
		return
	}

	tx := s.kvs.Begin(s.needsWritableTx())
	defer tx.Discard()

	results := make([]any, len(s.queue))

	for i, op := range s.queue {
		val, err := op.DbOp(tx)
		if err != nil {
			op.WireOp(conn, val, err)
			return
		}
		results[i] = val
	}

	err := tx.Commit()
	if err != nil {
		conn.WriteError("Couldn't commit transaction")
		return
	}

	for i := range s.queue {
		fn := s.queue[i].WireOp
		fn(conn, results[i], nil)
	}
	s.queue = nil
}

// check if any of the queued ops are mutating
// and therefore require a writable transaction
func (s *Session) needsWritableTx() bool {
	needsWriter := false

	for _, op := range s.queue {
		needsWriter = needsWriter || op.IsMutating
	}

	return needsWriter
}

// Get the current Redis DB (slot) number
func (s *Session) CurrentDB() int {
	if s == nil {
		panic("CurrentDB called on a NIL session pointer! Trace your variable assignments.")
	}
	return int(s.currentDB)
}

// Change the Redis DB (slot) number for this session
func (s *Session) SwitchDB(db int) {
	s.currentDB = int32(db)
}

func (s *Session) PublicKey(key []byte) []byte {
	return append(s.currentDbPrefix(), key...)
}

func (s *Session) PrivateKey(key []byte) []byte {
	return append(append([]byte(internalPrefix), s.currentDbPrefix()...), key...)
}

func (s *Session) NewPublicEntry(key []byte, value []byte) kv.Entry {
	return s.kvs.NewEntry(s.PublicKey(key), value)
}

func (s *Session) currentDbPrefix() []byte {
	return []byte(strconv.Itoa(s.CurrentDB()) + prefixSeparator)
}

// A QueuedOp separates operations on the KeyValueStore from subsequent wire operations
// They're declared lazily so they can be either executed straight away, or enqueued
// and run in a batch if required by the caller. The lifecycle of the Tx instance is
// managed outside of the QueuedOp.
type QueuedOp struct {
	// The DB side: runs inside the transaction, returns an opaque result or error
	DbOp func(tx kv.Tx) (any, error)
	// The wire side: runs after commit, consumes the result
	WireOp func(conn redcon.Conn, result any, err error)
	// Flags whether this op needs to run in a write transaction
	IsMutating bool
}
