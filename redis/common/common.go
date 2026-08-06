package common

import (
	"bytes"
	"encoding/binary"
	"math"
	"strconv"
	"sync/atomic"

	"github.com/hardpointlabs/invar/kv"
	"github.com/tidwall/redcon"
)

var globalSessionCounter atomic.Uint64

// GlobalWatchRegistry is the process-wide registry for blocking sorted-set commands.
// It is initialised once at startup and shared across all connections.
var GlobalWatchRegistry = NewWatchRegistry()

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
	inScript  bool // true while a Lua script is executing via redis.call
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

// EnterScript marks the session as executing a Lua script.  Blocking commands must
// degrade to non-blocking pops while this flag is set.
func (s *Session) EnterScript() {
	s.inScript = true
}

// ExitScript clears the Lua-script execution flag.
func (s *Session) ExitScript() {
	s.inScript = false
}

// ShouldBlock reports whether the current session context allows a blocking command to
// actually block.  It returns false whenever the caller must degrade to an immediate,
// non-blocking pop — specifically inside a MULTI/EXEC transaction or a Lua script,
// where stalling the connection is forbidden by the Redis specification.
func (s *Session) ShouldBlock() bool {
	return !s.inMulti && !s.inScript
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
		// No ops were enqueued — command was handled directly (e.g. pubsub).
		return
	}

	tx := s.kvs.Begin(s.needsWritableTx())
	defer tx.Discard()

	results := make([]any, len(s.queue))

	for i, op := range s.queue {
		val, err := op.DbOp(tx)
		if err != nil {
			// Any claims made by earlier ops in this batch must be returned to
			// the front of their queues since the transaction will be discarded.
			for j := 0; j < i; j++ {
				releaseOpClaims(results[j])
			}
			op.WireOp(conn, val, err)
			s.queue = nil
			return
		}
		results[i] = val
	}

	err := tx.Commit()
	if err != nil {
		// The transaction failed to commit — release all claims back to the
		// front of their queues so their waiters remain longest-waiting.
		for _, result := range results {
			releaseOpClaims(result)
		}
		conn.WriteError("Couldn't commit transaction")
		s.queue = nil
		return
	}

	for i := range s.queue {
		fn := s.queue[i].WireOp
		fn(conn, results[i], nil)
	}
	s.queue = nil
}

// releaseOpClaims returns any claims embedded in a DbOp result to the registry.
func releaseOpClaims(result any) {
	if c, ok := result.(Claimer); ok {
		for _, claim := range c.Claims() {
			GlobalWatchRegistry.ReleaseFront(claim)
		}
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

// KVS returns the underlying key-value store for this session.
// Used by blocking commands that need to open transactions directly.
func (s *Session) KVS() kv.KeyValueStore {
	return s.kvs
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

// Derive a full publicly accessible key in the LSM tree with the sessions'
// current database and the supplied key name
func (s *Session) PublicKey(key []byte) []byte {
	return append(s.currentDbPrefix(), key...)
}

// PublicKeyForDB returns the storage key for a public key in a specific DB.
func (s *Session) PublicKeyForDB(db int, key []byte) []byte {
	return append([]byte(strconv.Itoa(db)+prefixSeparator), key...)
}

// Derive a full private (internal) key in the LSM tree with the sessions'
// current database and the supplied key name
func (s *Session) PrivateKey(key []byte) []byte {
	return append(append([]byte(internalPrefix), s.currentDbPrefix()...), key...)
}

// Create a publicly accessible key/value entry in the LSM tree with
// the sessions' current database and the supplied key name
func (s *Session) NewPublicEntry(key []byte, value []byte) kv.Entry {
	return s.kvs.NewEntry(s.PublicKey(key), value)
}

// NewEntryForDB creates a public entry in a specific DB.
// Similar to NewPublicEntry but allows the caller to specify DB.
func (s *Session) NewEntryForDB(db int, key []byte, value []byte) kv.Entry {
	return s.kvs.NewEntry(s.PublicKeyForDB(db, key), value)
}

// Create a private (internal) key/value entry in the LSM tree with
// the sessions' current database and the supplied key name
func (s *Session) NewPrivateEntry(key []byte, value []byte) kv.Entry {
	return s.kvs.NewEntry(s.PrivateKey(key), value)
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

// MemberFromInternalKey extracts the field/member from an internal key
// after the null separator byte (\x00). Used by both hash and set sub-packages.
func MemberFromInternalKey(key []byte) []byte {
	idx := bytes.LastIndexByte(key, 0)
	if idx < 0 {
		return nil
	}
	return key[idx+1:]
}

// ReadUint32Sentinel reads a 4-byte big-endian uint32 from the public sentinel key.
func ReadUint32Sentinel(tx kv.Tx, session *Session, key []byte) (uint32, error) {
	item, err := tx.Get(session.PublicKey(key))
	if err != nil {
		return 0, err
	}
	val, err := item.Value()
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(val), nil
}

// WriteUint32Sentinel writes a 4-byte big-endian uint32 to the public sentinel key.
func WriteUint32Sentinel(tx kv.Tx, session *Session, key []byte, count uint32, typ RedisValueType) error {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, count)
	entry := session.NewPublicEntry(key, buf).Metadata(byte(typ))
	return tx.Set(entry)
}

// ClearPrefixedKeys deletes all keys under the given internal prefix, then deletes the sentinel key.
func ClearPrefixedKeys(tx kv.Tx, prefix, sentinelKey []byte) error {
	kvIt := tx.NewIterator(prefix)
	it := kvIt
	defer it.Close()
	for it.Next() {
		if err := tx.Delete(it.Item().Key()); err != nil {
			return err
		}
	}
	return tx.Delete(sentinelKey)
}

// FormatFloat formats a float64 for Redis responses, handling infinities.
func FormatFloat(f float64) string {
	if math.IsInf(f, 1) {
		return "inf"
	}
	if math.IsInf(f, -1) {
		return "-inf"
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}
