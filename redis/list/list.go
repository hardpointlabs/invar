package list

import (
	"bytes"
	"encoding/binary"
	"iter"
	"math/rand/v2"

	"github.com/hardpointlabs/invar/kv"
	"github.com/hardpointlabs/invar/redis/common"
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

// nodeKeyCompound builds the raw internal compound "{listName}:{nodeKey}" without session prefix.
func nodeKeyCompound(listName []byte, nodeKey []byte) []byte {
	compound := make([]byte, 0, len(listName)+1+len(nodeKey))
	compound = append(compound, listName...)
	compound = append(compound, ':')
	compound = append(compound, nodeKey...)
	return compound
}

// internalNodeKey builds the internal key: session.PrivateKey(listName + ":" + nodeKey)
func internalNodeKey(session *common.Session, listName []byte, nodeKey []byte) []byte {
	return session.PrivateKey(nodeKeyCompound(listName, nodeKey))
}

// readSentinel reads the list sentinel: size (4 bytes) + head (16) + tail (16) = 36 bytes.
func readSentinel(tx kv.Tx, session *common.Session, listName []byte) (uint32, []byte, []byte, error) {
	item, err := tx.Get(session.PublicKey(listName))
	if err != nil {
		return 0, nil, nil, err
	}
	val, err := item.Value()
	if err != nil {
		return 0, nil, nil, err
	}
	if len(val) < 36 {
		return 0, nil, nil, kv.ErrKeyNotFound
	}
	size := binary.BigEndian.Uint32(val[0:4])
	head := make([]byte, keyLength)
	copy(head, val[4:20])
	tail := make([]byte, keyLength)
	copy(tail, val[20:36])
	return size, head, tail, nil
}

// writeSentinel writes the list sentinel entry with RedisList metadata.
func writeSentinel(tx kv.Tx, session *common.Session, listName []byte, head, tail []byte, size uint32) error {
	buf := make([]byte, 36)
	binary.BigEndian.PutUint32(buf[0:4], size)
	copy(buf[4:20], head)
	copy(buf[20:36], tail)
	entry := session.NewPublicEntry(listName, buf).Metadata(byte(common.RedisList))
	return tx.Set(entry)
}

// nodeEntry builds a list node entry: value + prev (16) + next (16).
func nodeEntry(session *common.Session, listName []byte, node *listNode) kv.Entry {
	nextKey := make([]byte, keyLength)
	prevKey := make([]byte, keyLength)
	if node.next != nil {
		copy(nextKey, node.next.key)
	}
	if node.prev != nil {
		copy(prevKey, node.prev.key)
	}
	buf := bytes.NewBuffer(nil)
	buf.Write(node.value)
	buf.Write(prevKey)
	buf.Write(nextKey)
	return session.NewPrivateEntry(nodeKeyCompound(listName, node.key), buf.Bytes())
}

// loadList reads a list from the store and returns the linkedList struct
func loadList(tx kv.Tx, session *common.Session, listName []byte) (*linkedList, error) {
	size, headKey, tailKey, err := readSentinel(tx, session, listName)
	if err != nil {
		return nil, err
	}

	ll := &linkedList{name: listName, size: size}
	nodeMap := make(map[string]*listNode)

	// Traverse from head
	currentKey := headKey
	for len(currentKey) > 0 {
		keyStr := string(currentKey)
		if _, exists := nodeMap[keyStr]; exists {
			break // cycle detection
		}

		item, err := tx.Get(internalNodeKey(session, listName, currentKey))
		if err != nil {
			return nil, err
		}
		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		if len(val) < 32 {
			return nil, kv.ErrKeyNotFound
		}
		value := make([]byte, len(val)-32)
		copy(value, val[:len(val)-32])
		prevKey := make([]byte, keyLength)
		copy(prevKey, val[len(val)-32:len(val)-16])
		nextKey := make([]byte, keyLength)
		copy(nextKey, val[len(val)-16:])

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

// persistList writes the entire list to the store
func persistList(tx kv.Tx, session *common.Session, ll *linkedList) error {
	var headKey, tailKey []byte
	if ll.head != nil {
		headKey = ll.head.key
	}
	if ll.tail != nil {
		tailKey = ll.tail.key
	}

	// Write new sentinel
	if err := writeSentinel(tx, session, ll.name, headKey, tailKey, ll.size); err != nil {
		return err
	}

	// Write all nodes
	for node := ll.head; node != nil; node = node.next {
		if err := tx.Set(nodeEntry(session, ll.name, node)); err != nil {
			return err
		}
	}

	return nil
}

func LPush(session *common.Session, key []byte, values ...[]byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		ll, err := loadList(tx, session, key)
		if err == kv.ErrKeyNotFound {
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

		if err := persistList(tx, session, ll); err != nil {
			return 0, err
		}

		return int(ll.size), nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteInt(result.(int))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

func RPush(session *common.Session, key []byte, values ...[]byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		ll, err := loadList(tx, session, key)
		if err == kv.ErrKeyNotFound {
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

		if err := persistList(tx, session, ll); err != nil {
			return 0, err
		}

		return int(ll.size), nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteInt(result.(int))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

func LPop(session *common.Session, key []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		ll, err := loadList(tx, session, key)
		if err == kv.ErrKeyNotFound {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}

		val := ll.removeFirst()
		if ll.size == 0 {
			// Delete the entire list
			if err := tx.Delete(session.PublicKey(key)); err != nil {
				return nil, err
			}
		} else if err := persistList(tx, session, ll); err != nil {
			return nil, err
		}

		return val, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		if result == nil {
			conn.WriteNull()
		} else {
			conn.WriteBulk(result.([]byte))
		}
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

func RPop(session *common.Session, key []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		ll, err := loadList(tx, session, key)
		if err == kv.ErrKeyNotFound {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}

		val := ll.removeLast()
		if ll.size == 0 {
			if err := tx.Delete(session.PublicKey(key)); err != nil {
				return nil, err
			}
		} else if err := persistList(tx, session, ll); err != nil {
			return nil, err
		}

		return val, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		if result == nil {
			conn.WriteNull()
		} else {
			conn.WriteBulk(result.([]byte))
		}
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

func LLen(session *common.Session, key []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		size, _, _, err := readSentinel(tx, session, key)
		if err == kv.ErrKeyNotFound {
			return 0, nil
		}
		if err != nil {
			return 0, err
		}
		return int(size), nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteInt(result.(int))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

func LRange(session *common.Session, key []byte, start, stop int) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		ll, err := loadList(tx, session, key)
		if err == kv.ErrKeyNotFound {
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

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		items := result.([][]byte)
		conn.WriteArray(len(items))
		for _, item := range items {
			conn.WriteBulk(item)
		}
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

func LIndex(session *common.Session, key []byte, index int) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		ll, err := loadList(tx, session, key)
		if err == kv.ErrKeyNotFound {
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

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		if result == nil {
			conn.WriteNull()
		} else {
			conn.WriteBulk(result.([]byte))
		}
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

func LSet(session *common.Session, key []byte, index int, value []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		ll, err := loadList(tx, session, key)
		if err == kv.ErrKeyNotFound {
			return nil, kv.ErrKeyNotFound
		}
		if err != nil {
			return nil, err
		}

		if index < 0 {
			index = int(ll.size) + index
		}
		if index < 0 || index >= int(ll.size) {
			return nil, kv.ErrKeyNotFound
		}

		node := ll.head
		for i := 0; i < index; i++ {
			node = node.next
		}

		node.value = value
		return nil, persistList(tx, session, ll)
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			if err == kv.ErrKeyNotFound {
				conn.WriteError("ERR no such key")
			} else {
				conn.WriteError("ERR " + err.Error())
			}
			return
		}
		conn.WriteString("OK")
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

func LRem(session *common.Session, key []byte, count int, value []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		ll, err := loadList(tx, session, key)
		if err == kv.ErrKeyNotFound {
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
			return removed, tx.Delete(session.PublicKey(key))
		}

		return removed, persistList(tx, session, ll)
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteInt(result.(int))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

func LTrim(session *common.Session, key []byte, start, stop int) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		ll, err := loadList(tx, session, key)
		if err == kv.ErrKeyNotFound {
			return nil, nil
		}
		if err != nil {
			return nil, err
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
			return nil, tx.Delete(session.PublicKey(key))
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

		return nil, persistList(tx, session, ll)
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteString("OK")
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

func LInsert(session *common.Session, key []byte, before bool, pivot, value []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		ll, err := loadList(tx, session, key)
		if err == kv.ErrKeyNotFound {
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

		return int(ll.size), persistList(tx, session, ll)
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteInt(result.(int))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

func LPushX(session *common.Session, key, value []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		_, err := tx.Get(session.PublicKey(key))
		if err == kv.ErrKeyNotFound {
			return 0, nil
		}
		if err != nil {
			return 0, err
		}
		return LPush(session, key, value).DbOp(tx)
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteInt(result.(int))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

func RPushX(session *common.Session, key, value []byte) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		_, err := tx.Get(session.PublicKey(key))
		if err == kv.ErrKeyNotFound {
			return 0, nil
		}
		if err != nil {
			return 0, err
		}
		return RPush(session, key, value).DbOp(tx)
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteInt(result.(int))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}
