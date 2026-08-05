package json

import (
	"fmt"

	"github.com/hardpointlabs/invar/kv"
	"github.com/hardpointlabs/invar/redis/common"
	"github.com/tidwall/redcon"
)

// Set stores a JSON document at key, or updates a path within an existing one.
// nx/xx implement the NX/XX conditional semantics; ft is the FPHA type to
// validate against (FphaNone to skip validation).
func Set(s *common.Session, key []byte, path string, value any, nx, xx bool, ft fphaType) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		item, err := tx.Get(s.PublicKey(key))

		var doc *JSONDocument
		keyExists := false

		if err == nil {
			if item.Metadata() != byte(common.RedisJSON) {
				return nil, errWrongType
			}
			data, err := item.Value()
			if err != nil {
				return nil, err
			}
			doc, err = newJSONDocument(data)
			if err != nil {
				return nil, fmt.Errorf("ERR existing JSON document is corrupted")
			}
			keyExists = true
		} else if err == kv.ErrKeyNotFound {
			doc = newEmptyJSONDocument()
		} else {
			return nil, err
		}

		if path == "$" || path == "." {
			if nx && keyExists {
				return nil, errSkip
			}
			if xx && !keyExists {
				return nil, errSkip
			}
			doc.root = value
		} else {
			if nx {
				if _, err := doc.get(path); err == nil {
					return nil, errSkip
				}
			}
			if xx {
				if _, err := doc.get(path); err != nil {
					return nil, errSkip
				}
			}
			if err := doc.set(path, value); err != nil {
				return nil, err
			}
		}

		data, err := doc.serialize()
		if err != nil {
			return nil, err
		}
		if err := tx.Set(s.NewPublicEntry(key, data).Metadata(byte(common.RedisJSON))); err != nil {
			return nil, err
		}
		return "OK", nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeJSONErr(conn, err)
			return
		}
		conn.WriteString("OK")
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

// Get returns the JSON document at key, or a subset of paths within it.
// An empty paths slice returns the whole document.
func Get(s *common.Session, key []byte, paths []string) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		doc, err := getDoc(tx, s, key)
		if err != nil {
			return nil, err
		}

		if len(paths) == 0 {
			return doc.root, nil
		}
		if len(paths) == 1 {
			val, err := doc.get(paths[0])
			if err != nil {
				return nil, errSkip
			}
			return val, nil
		}

		result := make(map[string]any, len(paths))
		for _, p := range paths {
			if val, err := doc.get(p); err != nil {
				result[p] = nil
			} else {
				result[p] = val
			}
		}
		return result, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeJSONErr(conn, err)
			return
		}
		writeJSONValue(conn, result)
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

// Del deletes the whole document at key when paths is empty (returning 1 even
// when the key is missing), or deletes the given paths and returns how many
// were removed.
func Del(s *common.Session, key []byte, paths []string) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		if len(paths) == 0 {
			item, err := tx.Get(s.PublicKey(key))
			if err == kv.ErrKeyNotFound {
				return 1, nil
			}
			if err != nil {
				return nil, err
			}
			if item.Metadata() != byte(common.RedisJSON) {
				return nil, errWrongType
			}
			if err := tx.Delete(s.PublicKey(key)); err != nil {
				return nil, err
			}
			return 1, nil
		}

		doc, err := getDoc(tx, s, key)
		if err == kv.ErrKeyNotFound {
			return 0, nil
		}
		if err != nil {
			return nil, err
		}

		deleted := 0
		for _, path := range paths {
			if err := doc.delete(path); err == nil {
				deleted++
			}
		}

		if deleted > 0 {
			newData, err := doc.serialize()
			if err != nil {
				return nil, err
			}
			if err := tx.Set(s.NewPublicEntry(key, newData).Metadata(byte(common.RedisJSON))); err != nil {
				return nil, err
			}
		}
		return deleted, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeJSONErr(conn, err)
			return
		}
		conn.WriteInt(result.(int))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

// Type returns the JSON type name of the value at path, or null when the key
// is missing or the path does not resolve.
func Type(s *common.Session, key []byte, path string) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		doc, err := getDoc(tx, s, key)
		if err != nil {
			return nil, err
		}
		typeName, err := doc.typeOf(path)
		if err != nil {
			return nil, errSkip
		}
		return typeName, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeJSONErr(conn, err)
			return
		}
		conn.WriteBulkString(result.(string))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

// ArrAppend appends the given values to the array at path and returns its new
// length.
func ArrAppend(s *common.Session, key []byte, path string, values []any) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		var newLen int
		err := updateDoc(tx, s, key, func(doc *JSONDocument) error {
			var e error
			newLen, e = doc.arrAppend(path, values...)
			return e
		})
		if err != nil {
			return nil, err
		}
		return newLen, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeJSONErr(conn, err)
			return
		}
		conn.WriteInt(result.(int))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

// ArrIndex returns the first index of value within the array at path, or -1
// when it is absent or the path does not resolve.
func ArrIndex(s *common.Session, key []byte, path string, value any) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		doc, err := getDoc(tx, s, key)
		if err != nil {
			return nil, err
		}
		idx, err := doc.arrIndex(path, value)
		if err != nil {
			return -1, nil
		}
		return idx, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeJSONErr(conn, err)
			return
		}
		conn.WriteInt(result.(int))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

// ArrLen returns the length of the array at path, or null when the key is
// missing or the path does not resolve to an array.
func ArrLen(s *common.Session, key []byte, path string) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		doc, err := getDoc(tx, s, key)
		if err != nil {
			return nil, err
		}
		length, err := doc.arrLen(path)
		if err != nil {
			return nil, errSkip
		}
		return length, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeJSONErr(conn, err)
			return
		}
		conn.WriteInt(result.(int))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

// NumIncrBy increments the number at path by delta and returns the new value.
func NumIncrBy(s *common.Session, key []byte, path string, delta float64) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		var newVal float64
		err := updateDoc(tx, s, key, func(doc *JSONDocument) error {
			var e error
			newVal, e = doc.numIncrBy(path, delta)
			return e
		})
		if err != nil {
			return nil, err
		}
		return newVal, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeJSONErr(conn, err)
			return
		}
		conn.WriteBulkString(common.FormatFloat(result.(float64)))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

// NumMultBy multiplies the number at path by factor and returns the new value.
func NumMultBy(s *common.Session, key []byte, path string, factor float64) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		var newVal float64
		err := updateDoc(tx, s, key, func(doc *JSONDocument) error {
			var e error
			newVal, e = doc.numMultBy(path, factor)
			return e
		})
		if err != nil {
			return nil, err
		}
		return newVal, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeJSONErr(conn, err)
			return
		}
		conn.WriteBulkString(common.FormatFloat(result.(float64)))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

// ObjKeys returns the (sorted) keys of the object at path, or null when the
// key is missing or the path does not resolve to an object.
func ObjKeys(s *common.Session, key []byte, path string) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		doc, err := getDoc(tx, s, key)
		if err != nil {
			return nil, err
		}
		keys, err := doc.objKeys(path)
		if err != nil {
			return nil, errSkip
		}
		return keys, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeJSONErr(conn, err)
			return
		}
		keys := result.([]string)
		conn.WriteArray(len(keys))
		for _, k := range keys {
			conn.WriteBulkString(k)
		}
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

// ObjLen returns the number of keys in the object at path, or null when the
// key is missing or the path does not resolve to an object.
func ObjLen(s *common.Session, key []byte, path string) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		doc, err := getDoc(tx, s, key)
		if err != nil {
			return nil, err
		}
		length, err := doc.objLen(path)
		if err != nil {
			return nil, errSkip
		}
		return length, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeJSONErr(conn, err)
			return
		}
		conn.WriteInt(result.(int))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

// StrAppend appends suffix to the string at path and returns its new length.
func StrAppend(s *common.Session, key []byte, path string, suffix string) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		var newLen int
		err := updateDoc(tx, s, key, func(doc *JSONDocument) error {
			var e error
			newLen, e = doc.strAppend(path, suffix)
			return e
		})
		if err != nil {
			return nil, err
		}
		return newLen, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeJSONErr(conn, err)
			return
		}
		conn.WriteInt(result.(int))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

// StrLen returns the length of the string at path, or null when the key is
// missing or the path does not resolve to a string.
func StrLen(s *common.Session, key []byte, path string) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		doc, err := getDoc(tx, s, key)
		if err != nil {
			return nil, err
		}
		length, err := doc.strLen(path)
		if err != nil {
			return nil, errSkip
		}
		return length, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeJSONErr(conn, err)
			return
		}
		conn.WriteInt(result.(int))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

// MGet returns the value at path from each of the given keys, reading all of
// them from a single snapshot.
func MGet(s *common.Session, keys [][]byte, path string) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		results := make([]any, len(keys))
		for i, key := range keys {
			doc, err := getDoc(tx, s, key)
			if err != nil {
				results[i] = nil
				continue
			}
			val, err := doc.get(path)
			if err != nil {
				results[i] = nil
				continue
			}
			results[i] = val
		}
		return results, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeJSONErr(conn, err)
			return
		}
		results := result.([]any)
		conn.WriteArray(len(results))
		for _, val := range results {
			writeJSONValue(conn, val)
		}
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

// Resp returns the value at path (or the whole document) converted to a RESP
// representation, or null when the path does not resolve.
func Resp(s *common.Session, key []byte, path string) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		doc, err := getDoc(tx, s, key)
		if err != nil {
			return nil, err
		}
		if path == "" {
			return doc.root, nil
		}
		val, err := doc.get(path)
		if err != nil {
			return nil, errSkip
		}
		return val, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeJSONErr(conn, err)
			return
		}
		writeRESPValue(conn, result)
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: false}
}

// Clear empties the object or array at path (or the whole document) and
// returns how many containers were cleared.
func Clear(s *common.Session, key []byte, path string) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		item, err := tx.Get(s.PublicKey(key))
		if err == kv.ErrKeyNotFound {
			return 0, nil
		}
		if err != nil {
			return nil, err
		}
		if item.Metadata() != byte(common.RedisJSON) {
			return nil, errWrongType
		}
		data, err := item.Value()
		if err != nil {
			return nil, err
		}
		doc, err := newJSONDocument(data)
		if err != nil {
			return nil, err
		}

		cleared := 0
		if path == "$" || path == "." {
			doc.root = make(map[string]any)
			cleared = 1
		} else {
			val, err := doc.get(path)
			if err != nil {
				return 0, nil
			}
			switch v := val.(type) {
			case []any:
				if len(v) > 0 {
					doc.set(path, []any{})
					cleared = 1
				}
			case map[string]any:
				if len(v) > 0 {
					doc.set(path, make(map[string]any))
					cleared = 1
				}
			default:
				cleared = 1
				doc.set(path, make(map[string]any))
			}
		}

		if cleared > 0 {
			newData, err := doc.serialize()
			if err != nil {
				return nil, err
			}
			if err := tx.Set(s.NewPublicEntry(key, newData).Metadata(byte(common.RedisJSON))); err != nil {
				return nil, err
			}
		}
		return cleared, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeJSONErr(conn, err)
			return
		}
		conn.WriteInt(result.(int))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

// ArrPop removes and returns the element at index (negative indices count
// from the end) from the array at path. An empty array or a missing key
// yields null.
func ArrPop(s *common.Session, key []byte, path string, idx int) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		doc, err := getDoc(tx, s, key)
		if err != nil {
			return nil, err
		}

		val, err := doc.get(path)
		if err != nil {
			return nil, fmt.Errorf("err path does not exist")
		}
		arr, ok := val.([]any)
		if !ok {
			return nil, fmt.Errorf("err not an array")
		}
		if len(arr) == 0 {
			return nil, errSkip
		}

		popIdx := idx
		if popIdx < 0 {
			popIdx = len(arr) + popIdx
		}
		if popIdx < 0 || popIdx >= len(arr) {
			return nil, fmt.Errorf("err index out of range")
		}

		popped := arr[popIdx]
		arr = append(arr[:popIdx], arr[popIdx+1:]...)

		if err := setArrayResult(doc, path, arr); err != nil {
			return nil, err
		}

		newData, err := doc.serialize()
		if err != nil {
			return nil, err
		}
		if err := tx.Set(s.NewPublicEntry(key, newData).Metadata(byte(common.RedisJSON))); err != nil {
			return nil, err
		}
		return popped, nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeJSONErr(conn, err)
			return
		}
		writeJSONValue(conn, result)
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

// ArrTrim trims the array at path to the inclusive [start, stop] range
// (negative indices count from the end) and returns its new length.
func ArrTrim(s *common.Session, key []byte, path string, start, stop int) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		doc, err := getDoc(tx, s, key)
		if err != nil {
			return nil, err
		}

		val, err := doc.get(path)
		if err != nil {
			return nil, fmt.Errorf("err path does not exist")
		}
		arr, ok := val.([]any)
		if !ok {
			return nil, fmt.Errorf("err not an array")
		}

		if start < 0 {
			start = len(arr) + start
		}
		if stop < 0 {
			stop = len(arr) + stop
		}
		if start < 0 {
			start = 0
		}
		if stop >= len(arr) {
			stop = len(arr) - 1
		}
		if start > stop || start >= len(arr) {
			arr = []any{}
		} else {
			arr = arr[start : stop+1]
		}

		if err := setArrayResult(doc, path, arr); err != nil {
			return nil, err
		}

		newData, err := doc.serialize()
		if err != nil {
			return nil, err
		}
		if err := tx.Set(s.NewPublicEntry(key, newData).Metadata(byte(common.RedisJSON))); err != nil {
			return nil, err
		}
		return len(arr), nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeJSONErr(conn, err)
			return
		}
		conn.WriteInt(result.(int))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}

// ArrInsert inserts the given values at index within the array at path and
// returns its new length.
func ArrInsert(s *common.Session, key []byte, path string, index int, values []any) common.QueuedOp {
	dbOp := func(tx kv.Tx) (any, error) {
		doc, err := getDoc(tx, s, key)
		if err != nil {
			return nil, err
		}

		val, err := doc.get(path)
		if err != nil {
			return nil, fmt.Errorf("err path does not exist")
		}
		arr, ok := val.([]any)
		if !ok {
			return nil, fmt.Errorf("err not an array")
		}

		if index < 0 || index > len(arr) {
			return nil, fmt.Errorf("err index out of range")
		}

		newArr := make([]any, 0, len(arr)+len(values))
		newArr = append(newArr, arr[:index]...)
		newArr = append(newArr, values...)
		newArr = append(newArr, arr[index:]...)

		if err := setArrayResult(doc, path, newArr); err != nil {
			return nil, err
		}

		newData, err := doc.serialize()
		if err != nil {
			return nil, err
		}
		if err := tx.Set(s.NewPublicEntry(key, newData).Metadata(byte(common.RedisJSON))); err != nil {
			return nil, err
		}
		return len(newArr), nil
	}

	wireOp := func(conn redcon.Conn, result any, err error) {
		if err != nil {
			writeJSONErr(conn, err)
			return
		}
		conn.WriteInt(result.(int))
	}

	return common.QueuedOp{DbOp: dbOp, WireOp: wireOp, IsMutating: true}
}
