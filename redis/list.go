package redis

import (
	"bytes"
	"encoding/binary"
	"iter"
	"math/rand/v2"
	"strconv"

	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog/log"
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
	newHead := &listNode{sentinel: ll, key: randomKey(), value: value}
	if ll.head != nil {
		newHead.next = ll.head
		ll.head.prev = newHead
	} else {
		ll.tail = newHead
	}
	ll.head = newHead
	ll.size++
	return ll.size
}

func (ll *linkedList) addLast(value []byte) uint32 {
	newTail := &listNode{sentinel: ll, key: randomKey(), value: value}
	if ll.tail != nil {
		newTail.prev = ll.tail
		ll.tail.next = newTail
	} else {
		ll.head = newTail
	}
	ll.tail = newTail
	ll.size++
	return ll.size
}

func (ll *linkedList) removeFirst() []byte {
	if ll.head == nil {
		return nil
	}
	val := ll.head.value
	ll.head = ll.head.next
	if ll.head != nil {
		ll.head.prev = nil
	} else {
		ll.tail = nil
	}
	ll.size--
	return val
}

func (ll *linkedList) removeLast() []byte {
	if ll.tail == nil {
		return nil
	}
	val := ll.tail.value
	ll.tail = ll.tail.prev
	if ll.tail != nil {
		ll.tail.next = nil
	} else {
		ll.head = nil
	}
	ll.size--
	return val
}

func (ll *linkedList) toEntry(dbSlot int) *badger.Entry {
	return newSentinelNode(ll.name, ll.head.key, ll.tail.key, ll.size, dbSlot)
}

func (ln *listNode) toEntry(listName []byte, dbSlot int) *badger.Entry {
	nextKey := make([]byte, keyLength)
	prevKey := make([]byte, keyLength)
	if ln.next != nil {
		copy(nextKey, ln.next.key)
	}
	if ln.prev != nil {
		copy(prevKey, ln.prev.key)
	}
	return newListNode(ln.sentinel.name, ln.key, ln.value, nextKey, prevKey, dbSlot)
}

func makeNewList(name []byte, values ...[]byte) *linkedList {
	var sentinel = &linkedList{name: name, size: uint32(len(values))}
	if len(values) == 0 {
		return sentinel
	}
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

func isZeroKey(key []byte) bool {
	for _, b := range key {
		if b != 0 {
			return false
		}
	}
	return true
}

// Internal key format: -db:listname:randomkey
func internalNodeKey(listName []byte, nodeKey []byte, dbSlot int) []byte {
	prefix := append(append([]byte(internalPrefix), []byte(strconv.Itoa(dbSlot)+prefixSeparator)...), ':')
	return append(append(prefix, listName...), nodeKey...)
}

// creates the top-level user-accessible entry (sentinel)
func newSentinelNode(listName []byte, head []byte, tail []byte, size uint32, dbSlot int) *badger.Entry {
	buf := bytes.NewBuffer(nil)
	binary.Write(buf, binary.BigEndian, size)
	buf.Write(head)
	buf.Write(tail)
	return badger.NewEntry(rawKeyPrefixWithDb(listName, dbSlot), buf.Bytes()).WithMeta(byte(RedisList))
}

// creates a list node entry
func newListNode(listName []byte, nodeKey []byte, value []byte, next []byte, prev []byte, dbSlot int) *badger.Entry {
	entryName := internalNodeKey(listName, nodeKey, dbSlot)
	log.Info().Str("entry", string(entryName)).Msg("new list node")
	buf := bytes.NewBuffer(nil)
	buf.Write(value)
	buf.Write(prev)
	buf.Write(next)
	return badger.NewEntry(entryName, buf.Bytes())
}

// loadList reads a list from Badger and returns the linkedList struct
func loadList(txn *badger.Txn, listName []byte, dbSlot int) (*linkedList, error) {
	item, err := txn.Get(rawKeyPrefixWithDb(listName, dbSlot))
	if err != nil {
		return nil, err
	}

	ll := &linkedList{name: listName}
	var headKey []byte
	var tailKey []byte

	err = item.Value(func(val []byte) error {
		if len(val) < 36 { // 4 (size) + 16 (head) + 16 (tail)
			return badger.ErrKeyNotFound
		}
		ll.size = binary.BigEndian.Uint32(val[0:4])
		headKey = make([]byte, 16)
		copy(headKey, val[4:20])
		tailKey = make([]byte, 16)
		copy(tailKey, val[20:36])
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Map to store nodes by key
	nodeMap := make(map[string]*listNode)

	// Traverse from head
	currentKey := headKey
	for len(currentKey) > 0 {
		keyStr := string(currentKey)
		if _, exists := nodeMap[keyStr]; exists {
			break // cycle detection
		}

		item, err := txn.Get(internalNodeKey(listName, currentKey, dbSlot))
		if err != nil {
			return nil, err
		}

		var value []byte
		var prevKey []byte
		var nextKey []byte

		err = item.Value(func(val []byte) error {
			if len(val) < 32 {
				return badger.ErrKeyNotFound
			}
			value = make([]byte, len(val)-32)
			copy(value, val[:len(val)-32])
			prevKey = make([]byte, 16)
			copy(prevKey, val[len(val)-32:len(val)-16])
			nextKey = make([]byte, 16)
			copy(nextKey, val[len(val)-16:])
			return nil
		})
		if err != nil {
			return nil, err
		}

		// Create node
		node := &listNode{
			sentinel: ll,
			key:      currentKey,
			value:    value,
		}
		nodeMap[keyStr] = node

		// Store keys for linking later
		if !isZeroKey(prevKey) {
			node.prev = &listNode{key: prevKey, sentinel: ll}
		}
		if !isZeroKey(nextKey) {
			node.next = &listNode{key: nextKey, sentinel: ll}
		}

		// Move to next
		if !isZeroKey(nextKey) {
			currentKey = nextKey
		} else {
			break
		}
	}

	// Link nodes: replace stub nodes with actual nodes from map
	for _, node := range nodeMap {
		if node.prev != nil {
			if actual, ok := nodeMap[string(node.prev.key)]; ok {
				node.prev = actual
			}
		}
		if node.next != nil {
			if actual, ok := nodeMap[string(node.next.key)]; ok {
				node.next = actual
			}
		}
	}

	// Set head and tail
	ll.head = nodeMap[string(headKey)]
	ll.tail = nodeMap[string(tailKey)]

	return ll, nil
}

// persistList writes the entire list to Badger
func persistList(txn *badger.Txn, ll *linkedList, dbSlot int) error {
	// Delete old sentinel
	if err := txn.Delete(rawKeyPrefixWithDb(ll.name, dbSlot)); err != nil && err != badger.ErrKeyNotFound {
		return err
	}

	// Write new sentinel
	if err := txn.SetEntry(ll.toEntry(dbSlot)); err != nil {
		return err
	}

	// Write all nodes
	for node := ll.head; node != nil; node = node.next {
		if err := txn.SetEntry(node.toEntry(ll.name, dbSlot)); err != nil {
			return err
		}
	}

	return nil
}

// lpush pushes values to the head of the list
func lpush(txn *badger.Txn, conn redcon.Conn, key []byte, values ...[]byte) (uint32, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}
	ll, err := loadList(txn, key, dbSlot)
	if err == badger.ErrKeyNotFound {
		// Create new list and push values to head
		ll = &linkedList{name: key}
		for _, value := range values {
			ll.addFirst(value)
		}
	} else if err != nil {
		return 0, err
	} else {
		// Add to head
		for _, value := range values {
			ll.addFirst(value)
		}
	}

	if err := persistList(txn, ll, dbSlot); err != nil {
		return 0, err
	}

	return ll.size, nil
}

// rpush pushes values to the tail of the list
func rpush(txn *badger.Txn, conn redcon.Conn, key []byte, values ...[]byte) (uint32, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}
	ll, err := loadList(txn, key, dbSlot)
	if err == badger.ErrKeyNotFound {
		// Create new list and push values to tail
		ll = &linkedList{name: key}
		for _, value := range values {
			ll.addLast(value)
		}
	} else if err != nil {
		return 0, err
	} else {
		for _, value := range values {
			ll.addLast(value)
		}
	}

	if err := persistList(txn, ll, dbSlot); err != nil {
		return 0, err
	}

	return ll.size, nil
}

// lpop removes and returns the first element
func lpop(txn *badger.Txn, conn redcon.Conn, key []byte) ([]byte, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}
	ll, err := loadList(txn, key, dbSlot)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	val := ll.removeFirst()
	if ll.size == 0 {
		// Delete the entire list
		if err := txn.Delete(rawKeyPrefixWithDb(key, dbSlot)); err != nil {
			return nil, err
		}
	} else {
		if err := persistList(txn, ll, dbSlot); err != nil {
			return nil, err
		}
	}

	return val, nil
}

// rpop removes and returns the last element
func rpop(txn *badger.Txn, conn redcon.Conn, key []byte) ([]byte, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}
	ll, err := loadList(txn, key, dbSlot)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	val := ll.removeLast()
	if ll.size == 0 {
		if err := txn.Delete(rawKeyPrefixWithDb(key, dbSlot)); err != nil {
			return nil, err
		}
	} else {
		if err := persistList(txn, ll, dbSlot); err != nil {
			return nil, err
		}
	}

	return val, nil
}

// llen returns the length of the list
func llen(txn *badger.Txn, conn redcon.Conn, key []byte) (int, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}
	item, err := txn.Get(rawKeyPrefixWithDb(key, dbSlot))
	if err == badger.ErrKeyNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	var size uint32
	err = item.Value(func(val []byte) error {
		if len(val) < 4 {
			return badger.ErrKeyNotFound
		}
		size = binary.BigEndian.Uint32(val[0:4])
		return nil
	})
	return int(size), err
}

// lrange returns elements from start to stop
func lrange(txn *badger.Txn, conn redcon.Conn, key []byte, start, stop int) ([][]byte, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}
	ll, err := loadList(txn, key, dbSlot)
	if err == badger.ErrKeyNotFound {
		return [][]byte{}, nil
	}
	if err != nil {
		return nil, err
	}

	// Handle negative indices
	if start < 0 {
		start = int(ll.size) + start
	}
	if stop < 0 {
		stop = int(ll.size) + stop
	}

	if start < 0 {
		start = 0
	}
	if stop >= int(ll.size) {
		stop = int(ll.size) - 1
	}
	if start > stop || start >= int(ll.size) {
		return [][]byte{}, nil
	}

	var result [][]byte
	node := ll.head
	for i := 0; i < start && node != nil; i++ {
		node = node.next
	}

	for i := start; i <= stop && node != nil; i++ {
		result = append(result, node.value)
		node = node.next
	}

	return result, nil
}

// lindex returns the element at index
func lindex(txn *badger.Txn, conn redcon.Conn, key []byte, index int) ([]byte, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}
	ll, err := loadList(txn, key, dbSlot)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if index < 0 {
		index = int(ll.size) + index
	}
	if index < 0 || index >= int(ll.size) {
		return nil, nil
	}

	node := ll.head
	for i := 0; i < index; i++ {
		node = node.next
	}

	return node.value, nil
}

// lset sets the value at index
func lset(txn *badger.Txn, conn redcon.Conn, key []byte, index int, value []byte) error {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}
	ll, err := loadList(txn, key, dbSlot)
	if err == badger.ErrKeyNotFound {
		return badger.ErrKeyNotFound
	}
	if err != nil {
		return err
	}

	if index < 0 {
		index = int(ll.size) + index
	}
	if index < 0 || index >= int(ll.size) {
		return badger.ErrKeyNotFound
	}

	node := ll.head
	for i := 0; i < index; i++ {
		node = node.next
	}

	node.value = value
	return persistList(txn, ll, dbSlot)
}

// lrem removes count occurrences of value
func lrem(txn *badger.Txn, conn redcon.Conn, key []byte, count int, value []byte) (int, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}
	ll, err := loadList(txn, key, dbSlot)
	if err == badger.ErrKeyNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	var removed int

	if count == 0 {
		// Remove all occurrences
		var newHead *listNode
		var prev *listNode

		for node := ll.head; node != nil; {
			next := node.next
			if bytes.Equal(node.value, value) {
				// Remove this node
				if prev != nil {
					prev.next = node.next
				}
				if node.next != nil {
					node.next.prev = prev
				}
				removed++
				ll.size--
			} else {
				if newHead == nil {
					newHead = node
				}
				prev = node
			}
			node = next
		}
		ll.head = newHead
		ll.tail = prev
	} else if count > 0 {
		// Remove first count occurrences
		var newHead *listNode
		var prev *listNode

		for node := ll.head; node != nil && removed < count; {
			next := node.next
			if bytes.Equal(node.value, value) {
				if prev != nil {
					prev.next = node.next
				}
				if node.next != nil {
					node.next.prev = prev
				}
				removed++
				ll.size--
			} else {
				if newHead == nil {
					newHead = node
				}
				prev = node
			}
			node = next
		}
		ll.head = newHead
		ll.tail = prev
	} else {
		// count < 0, remove last |count| occurrences
		// Need to traverse from tail
		type nodeWithPrev struct {
			node *listNode
			prev *listNode
		}
		var nodes []nodeWithPrev
		for node := ll.head; node != nil; node = node.next {
			nodes = append(nodes, nodeWithPrev{node: node, prev: nil})
		}
		// Fix prev pointers
		for i := 1; i < len(nodes); i++ {
			nodes[i].prev = nodes[i-1].node
		}

		removed = 0
		for i := len(nodes) - 1; i >= 0 && removed < -count; i-- {
			if bytes.Equal(nodes[i].node.value, value) {
				// Remove this node
				if nodes[i].prev != nil {
					nodes[i].prev.next = nodes[i].node.next
				}
				if nodes[i].node.next != nil {
					nodes[i].node.next.prev = nodes[i].prev
				}
				removed++
				ll.size--
			}
		}

		// Rebuild list
		ll.head = nil
		ll.tail = nil
		for _, n := range nodes {
			if n.node.prev == nil && n.node.next == nil {
				// This node was removed, skip
				continue
			}
			if ll.head == nil {
				ll.head = n.node
				ll.tail = n.node
			} else {
				ll.tail.next = n.node
				n.node.prev = ll.tail
				ll.tail = n.node
			}
		}
	}

	if ll.size == 0 {
		return removed, txn.Delete(rawKeyPrefixWithDb(key, dbSlot))
	}

	return removed, persistList(txn, ll, dbSlot)
}

// ltrim trims the list to the specified range
func ltrim(txn *badger.Txn, conn redcon.Conn, key []byte, start, stop int) error {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}
	ll, err := loadList(txn, key, dbSlot)
	if err == badger.ErrKeyNotFound {
		return nil
	}
	if err != nil {
		return err
	}

	if start < 0 {
		start = int(ll.size) + start
	}
	if stop < 0 {
		stop = int(ll.size) + stop
	}

	if start < 0 {
		start = 0
	}
	if stop >= int(ll.size) {
		stop = int(ll.size) - 1
	}

	if start > stop || start >= int(ll.size) {
		// Delete entire list
		ll.size = 0
		ll.head = nil
		ll.tail = nil
		return txn.Delete(rawKeyPrefixWithDb(key, dbSlot))
	}

	// Keep only elements in range [start, stop]
	var newHead *listNode
	var newTail *listNode
	node := ll.head
	for i := 0; i <= stop && node != nil; i++ {
		if i >= start {
			newNode := &listNode{
				sentinel: ll,
				key:      node.key,
				value:    node.value,
			}
			if newHead == nil {
				newHead = newNode
				newTail = newNode
			} else {
				newTail.next = newNode
				newNode.prev = newTail
				newTail = newNode
			}
		}
		node = node.next
	}

	ll.head = newHead
	ll.tail = newTail
	ll.size = uint32(stop - start + 1)

	return persistList(txn, ll, dbSlot)
}

// linsert inserts value before or after pivot
func linsert(txn *badger.Txn, conn redcon.Conn, key []byte, before bool, pivot []byte, value []byte) (int, error) {
	dbSlot := 0
	if conn != nil {
		dbSlot = currentDb(conn)
	}
	ll, err := loadList(txn, key, dbSlot)
	if err == badger.ErrKeyNotFound {
		return -1, nil
	}
	if err != nil {
		return 0, err
	}

	// Find pivot
	found := false
	for node := ll.head; node != nil; node = node.next {
		if bytes.Equal(node.value, pivot) {
			found = true
			newNode := &listNode{
				sentinel: ll,
				key:      randomKey(),
				value:    value,
			}
			if before {
				newNode.next = node
				newNode.prev = node.prev
				if node.prev != nil {
					node.prev.next = newNode
				} else {
					ll.head = newNode
				}
				node.prev = newNode
			} else {
				newNode.prev = node
				newNode.next = node.next
				if node.next != nil {
					node.next.prev = newNode
				} else {
					ll.tail = newNode
				}
				node.next = newNode
			}
			ll.size++
			break
		}
	}

	if !found {
		return -1, nil
	}

	return int(ll.size), persistList(txn, ll, dbSlot)
}

// consList is a convenience function for LPUSH/RPUSH logic
func consList(conn redcon.Conn, db *badger.DB, key []byte, values ...[]byte) {
	if len(values) == 0 {
		return
	}

	err := db.Update(func(txn *badger.Txn) error {
		_, err := lpush(txn, conn, key, values...)
		return err
	})

	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}

	// Return the new length
	db.View(func(txn *badger.Txn) error {
		size, err := llen(txn, conn, key)
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return err
		}
		conn.WriteInt(size)
		return nil
	})
}
