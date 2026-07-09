package common

import (
	"log"
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
	Id        uint64        // Redis connection ID
	currentDB int32         // Current Redis DB this connection is operating on
	queue     []kv.QueuedOp // nil when not in MULTI
	inMulti   bool
}

func NewSession() *Session {
	var s = &Session{}
	s.Id = globalSessionCounter.Add(1)
	s.currentDB = 0
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
func (s *Session) EnqueueOp(op kv.QueuedOp) {
	if s.queue == nil {
		s.queue = []kv.QueuedOp{op}
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
		panic("Transaction queue should not be nil when dispatch is called")
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
	return nil
}

func (s *Session) currentDbPrefix() []byte {
	log.Println("HALLO!")
	log.Printf("CURRENT DB: %d", s.currentDB)
	return []byte(strconv.Itoa(s.CurrentDB()) + prefixSeparator)
}
