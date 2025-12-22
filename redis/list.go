package redis

import (
	"bytes"
	"encoding/binary"
	"iter"
	"log"
	"math/rand/v2"

	"github.com/dgraph-io/badger/v4"
	"github.com/tidwall/redcon"
)

type listNode struct {
	sentinel *linkedList
	key      []byte
	value    []byte
	prev     *listNode
	next     *listNode
}

type linkedList struct {
	size uint32
	head *listNode
	tail *listNode
	name []byte
}

func (ll *linkedList) all() iter.Seq[listNode] {
	return func(yield func(listNode) bool) {
		for node := ll.head; node != nil; node = node.next {
			if !yield(*node) {
				return
			}
		}
	}
}

func (ll *linkedList) addFirst(value []byte) uint32 {
	currentHead := ll.head
	newHead := &listNode{sentinel: ll, key: randomKey(), next: currentHead, value: value}
	currentHead.prev = newHead
	ll.head = newHead
	ll.size++
	return ll.size
}

func (ll *linkedList) toEntry(conn redcon.Conn) *badger.Entry {
	return newSentinelNode(conn, ll.name, ll.head.key, ll.tail.key)
}

func (ln *listNode) toEntry(conn redcon.Conn) *badger.Entry {
	return newListNode(conn, ln.sentinel.name, ln.value, ln.next.key, ln.prev.key)
}

func makeNewList(name []byte, values ...[]byte) *linkedList {
	var sentinel = &linkedList{name: name, size: uint32(len(values))}
	var entries = make([]listNode, len(values))
	for i, value := range values {
		entries[i] = listNode{sentinel: sentinel, value: value, key: randomKey()}
		if i > 0 {
			entries[i].prev = &entries[i-1]
			entries[i-1].next = &entries[i]
		}
	}
	sentinel.head = &entries[0]
	sentinel.tail = &entries[len(entries)-1]
	return sentinel
}

const keyLength = 16

func randomKey() []byte {
	bytes := make([]byte, keyLength)
	for i := range keyLength {
		bytes[i] = byte(rand.IntN(256))
	}
	return bytes
}

// creates the top-level user-accessible entry and sets up the bookkeeping for adding more elements
// redis lists are doubly linked lists with head and tail pointers so we keep track of those in this entry,
// treating it as the 'sentinel' for the list, just keeping the metadata:
// - number of elements in the list (redis lists can have max 2^32-1 elements)
// - head key bytes
// - tail key bytes
// node keys are generated as random 4-byte sequences prefixed with the sentinel key name, e.g.
// -0:mylist:<random-bytes>
func newSentinelNode(conn redcon.Conn, listName []byte, head []byte, tail []byte) *badger.Entry {
	// TODO metadata
	buf := bytes.NewBuffer(nil)
	binary.Write(buf, binary.BigEndian, uint32(0)) // size
	return badger.NewEntry(rawKeyPrefix(listName, currentDb(conn)), buf.Bytes()).WithMeta(byte(RedisList))
}

// then each node (in the private namespace, prefixed with "-<db-index>:") is stored as a separate key with the following format:
// - previous element key bytes
// - next element key bytes
func newListNode(conn redcon.Conn, listName []byte, value []byte, next []byte, prev []byte) *badger.Entry {
	entryName := append(append(append(currentDbInternalPrefix(conn), ':'), listName...), randomKey()...)
	log.Println("new list node: ", entryName)
	return badger.NewEntry(entryName, append(append(value, prev...), next...))
}

// cons 1 or more values onto a list, creating it if it does not exist
func consList(conn redcon.Conn, db *badger.DB, key []byte, values ...[]byte) {
	if len(values) == 0 {
		return
	}

	err := db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(rawKeyPrefix(key, currentDb(conn)))
		return err
	})

	if err == badger.ErrKeyNotFound {
		var sentinel = makeNewList(key, values...)
		wb := db.NewWriteBatch()
		defer wb.Cancel()

		wb.SetEntry(sentinel.toEntry(conn))
		var next = sentinel.head
		for next != nil {
			err = wb.SetEntry(next.toEntry(conn))
			if err != nil {
				conn.WriteError(err.Error())
				return
			}
		}
	}
}
