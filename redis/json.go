package redis

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/tidwall/redcon"
)

type pathPartType int

const (
	partRoot pathPartType = iota
	partKey
	partIndex
	partWildcard
	partRecursive
)

type pathPart struct {
	typ pathPartType
	key string
	idx int
}

func parsePath(s string) ([]pathPart, error) {
	if s == "" {
		return nil, fmt.Errorf("err path cannot be empty")
	}

	var parts []pathPart

	if s[0] == '$' {
		parts = append(parts, pathPart{typ: partRoot})
		s = s[1:]
	} else if s[0] == '.' || s[0] == '[' {
		parts = append(parts, pathPart{typ: partRoot})
	} else {
		return nil, fmt.Errorf("err path must start with $")
	}

	for i := 0; i < len(s); {
		ch := s[i]
		switch {
		case ch == '.':
			i++
			if i < len(s) && s[i] == '.' {
				parts = append(parts, pathPart{typ: partRecursive})
				i++
				// After .., consume the following key/wildcard (no dot needed)
				if i < len(s) && s[i] == '*' {
					parts = append(parts, pathPart{typ: partWildcard})
					i++
				} else if i < len(s) && s[i] == '[' {
					// bracket will be handled by next iteration
				} else if i < len(s) {
					start := i
					for i < len(s) && s[i] != '.' && s[i] != '[' && s[i] != '*' {
						i++
					}
					if start < i {
						parts = append(parts, pathPart{typ: partKey, key: s[start:i]})
					}
				}
			} else if i < len(s) && s[i] == '*' {
				parts = append(parts, pathPart{typ: partWildcard})
				i++
			} else {
				start := i
				for i < len(s) && s[i] != '.' && s[i] != '[' && s[i] != '*' {
					i++
				}
				if i == start {
					return nil, fmt.Errorf("err empty key in path")
				}
				parts = append(parts, pathPart{typ: partKey, key: s[start:i]})
			}
		case ch == '[':
			i++
			if i < len(s) && s[i] == '*' {
				parts = append(parts, pathPart{typ: partWildcard})
				i++
				if i >= len(s) || s[i] != ']' {
					return nil, fmt.Errorf("err invalid path")
				}
				i++
			} else if i < len(s) && s[i] == '"' {
				i++
				start := i
				for i < len(s) && s[i] != '"' {
					if s[i] == '\\' {
						i++
					}
					i++
				}
				if i >= len(s) {
					return nil, fmt.Errorf("err unclosed string in path")
				}
				parts = append(parts, pathPart{typ: partKey, key: s[start:i]})
				i++
				if i < len(s) && s[i] == ']' {
					i++
				} else {
					return nil, fmt.Errorf("err expected ] after string key")
				}
			} else {
				start := i
				for i < len(s) && s[i] != ']' {
					i++
				}
				if i >= len(s) {
					return nil, fmt.Errorf("err unclosed bracket in path")
				}
				idxStr := s[start:i]
				if idxStr == "" {
					return nil, fmt.Errorf("err empty brackets in path")
				}
				idx, err := strconv.Atoi(idxStr)
				if err != nil {
					return nil, fmt.Errorf("err invalid array index: %s", idxStr)
				}
				parts = append(parts, pathPart{typ: partIndex, idx: idx})
				i++
			}
		case ch == '*':
			parts = append(parts, pathPart{typ: partWildcard})
			i++
		default:
			return nil, fmt.Errorf("err unexpected character '%c' in path", ch)
		}
	}

	return parts, nil
}

func resolveValue(data any, parts []pathPart) ([]any, error) {
	var results []any
	err := resolveRecursive(data, parts, 0, &results)
	if err != nil {
		return nil, err
	}
	return results, nil
}

func resolveRecursive(data any, parts []pathPart, depth int, results *[]any) error {
	if depth >= len(parts) {
		*results = append(*results, data)
		return nil
	}

	part := parts[depth]

	if part.typ == partRoot {
		return resolveRecursive(data, parts, depth+1, results)
	}

	if part.typ == partRecursive {
		if depth+1 >= len(parts) {
			*results = append(*results, data)
		} else {
			resolveRecursive(data, parts, depth+1, results)
		}
		switch v := data.(type) {
		case map[string]any:
			for _, child := range v {
				resolveRecursive(child, parts, depth, results)
			}
		case []any:
			for _, child := range v {
				resolveRecursive(child, parts, depth, results)
			}
		}
		return nil
	}

	if part.typ == partWildcard {
		switch v := data.(type) {
		case map[string]any:
			for _, child := range v {
				resolveRecursive(child, parts, depth+1, results)
			}
		case []any:
			for _, child := range v {
				resolveRecursive(child, parts, depth+1, results)
			}
		default:
			return fmt.Errorf("err cannot wildcard on scalar value")
		}
		return nil
	}

	if part.typ == partKey {
		m, ok := data.(map[string]any)
		if !ok {
			return fmt.Errorf("err path does not exist")
		}
		val, ok := m[part.key]
		if !ok {
			return fmt.Errorf("err path does not exist")
		}
		return resolveRecursive(val, parts, depth+1, results)
	}

	if part.typ == partIndex {
		arr, ok := data.([]any)
		if !ok {
			return fmt.Errorf("err not an array")
		}
		idx := part.idx
		if idx < 0 {
			idx = len(arr) + idx
		}
		if idx < 0 || idx >= len(arr) {
			return fmt.Errorf("err index out of range")
		}
		return resolveRecursive(arr[idx], parts, depth+1, results)
	}

	return fmt.Errorf("err unexpected path part type")
}

func ensureParent(data any, parts []pathPart) (any, pathPart, error) {
	if len(parts) <= 1 {
		return data, parts[0], nil
	}

	current := data
	for i := 1; i < len(parts)-1; i++ {
		part := parts[i]
		switch part.typ {
		case partKey:
			m, ok := current.(map[string]any)
			if !ok {
				return nil, pathPart{}, fmt.Errorf("err existing key has wrong type")
			}
			next, exists := m[part.key]
			if !exists {
				next = make(map[string]any)
				m[part.key] = next
			}
			current = next
		case partIndex:
			arr, ok := current.([]any)
			if !ok {
				return nil, pathPart{}, fmt.Errorf("err not an array")
			}
			idx := part.idx
			if idx < 0 {
				idx = len(arr) + idx
			}
			if idx < 0 || idx >= len(arr) {
				return nil, pathPart{}, fmt.Errorf("err index out of range")
			}
			current = arr[idx]
		default:
			return nil, pathPart{}, fmt.Errorf("err wildcard/recursive paths not supported for set")
		}
	}

	return current, parts[len(parts)-1], nil
}

func jsonTypeName(val any) string {
	if val == nil {
		return "null"
	}
	switch val.(type) {
	case bool:
		return "boolean"
	case float64:
		return "number"
	case string:
		return "string"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	default:
		return "unknown"
	}
}

type JSONDocument struct {
	root any
}

func newJSONDocument(raw []byte) (*JSONDocument, error) {
	var root any
	if err := json.Unmarshal(raw, &root); err != nil {
		return nil, err
	}
	return &JSONDocument{root: root}, nil
}

func newEmptyJSONDocument() *JSONDocument {
	return &JSONDocument{root: make(map[string]any)}
}

func (d *JSONDocument) serialize() ([]byte, error) {
	return json.Marshal(d.root)
}

func (d *JSONDocument) get(path string) (any, error) {
	parts, err := parsePath(path)
	if err != nil {
		return nil, err
	}

	if len(parts) == 1 {
		return d.root, nil
	}

	results, err := resolveValue(d.root, parts)
	if err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("err path does not exist")
	}
	if len(results) == 1 {
		return results[0], nil
	}
	return results, nil
}

func (d *JSONDocument) set(path string, value any) error {
	parts, err := parsePath(path)
	if err != nil {
		return err
	}

	if len(parts) == 1 {
		d.root = value
		return nil
	}

	for _, p := range parts[1:] {
		if p.typ == partWildcard || p.typ == partRecursive {
			return fmt.Errorf("err wildcard/recursive paths not supported for set")
		}
	}

	parent, lastPart, err := ensureParent(d.root, parts)
	if err != nil {
		return err
	}

	switch lastPart.typ {
	case partKey:
		m, ok := parent.(map[string]any)
		if !ok {
			return fmt.Errorf("err existing key has wrong type")
		}
		m[lastPart.key] = value
	case partIndex:
		arr, ok := parent.([]any)
		if !ok {
			return fmt.Errorf("err not an array")
		}
		idx := lastPart.idx
		if idx < 0 {
			idx = len(arr) + idx
		}
		if idx < 0 || idx >= len(arr) {
			return fmt.Errorf("err index out of range")
		}
		arr[idx] = value
	default:
		return fmt.Errorf("err unexpected path part")
	}

	return nil
}

func (d *JSONDocument) delete(path string) error {
	parts, err := parsePath(path)
	if err != nil {
		return err
	}

	if len(parts) == 1 {
		d.root = nil
		return nil
	}

	for _, p := range parts[1:] {
		if p.typ == partWildcard || p.typ == partRecursive {
			return fmt.Errorf("err wildcard/recursive paths not supported for delete")
		}
	}

	lastPart := parts[len(parts)-1]

	if lastPart.typ == partKey {
		parent, _, err := ensureParent(d.root, parts)
		if err != nil {
			return err
		}
		m, ok := parent.(map[string]any)
		if !ok {
			return fmt.Errorf("err existing key has wrong type")
		}
		_, exists := m[lastPart.key]
		if !exists {
			return fmt.Errorf("err path does not exist")
		}
		delete(m, lastPart.key)
		return nil
	}

	if lastPart.typ == partIndex {
		arrayParts := parts[:len(parts)-1]

		arr, err := resolveSingle(d.root, arrayParts)
		if err != nil {
			return err
		}
		a, ok := arr.([]any)
		if !ok {
			return fmt.Errorf("err not an array")
		}

		idx := lastPart.idx
		if idx < 0 {
			idx = len(a) + idx
		}
		if idx < 0 || idx >= len(a) {
			return fmt.Errorf("err index out of range")
		}

		newArr := make([]any, 0, len(a)-1)
		newArr = append(newArr, a[:idx]...)
		newArr = append(newArr, a[idx+1:]...)

		return d.setAtParts(arrayParts, newArr)
	}

	return fmt.Errorf("err unexpected path part")
}

func resolveSingle(data any, parts []pathPart) (any, error) {
	results, err := resolveValue(data, parts)
	if err != nil {
		return nil, err
	}
	if len(results) != 1 {
		return nil, fmt.Errorf("err ambiguous path")
	}
	return results[0], nil
}

func (d *JSONDocument) setAtParts(parts []pathPart, value any) error {
	if len(parts) <= 1 {
		d.root = value
		return nil
	}

	for _, p := range parts[1:] {
		if p.typ == partWildcard || p.typ == partRecursive {
			return fmt.Errorf("err wildcard/recursive paths not supported for set")
		}
	}

	parent, lastPart, err := ensureParent(d.root, parts)
	if err != nil {
		return err
	}

	switch lastPart.typ {
	case partKey:
		m, ok := parent.(map[string]any)
		if !ok {
			return fmt.Errorf("err existing key has wrong type")
		}
		m[lastPart.key] = value
	case partIndex:
		arr, ok := parent.([]any)
		if !ok {
			return fmt.Errorf("err not an array")
		}
		idx := lastPart.idx
		if idx < 0 {
			idx = len(arr) + idx
		}
		if idx < 0 || idx >= len(arr) {
			return fmt.Errorf("err index out of range")
		}
		arr[idx] = value
	default:
		return fmt.Errorf("err unexpected path part")
	}

	return nil
}

func (d *JSONDocument) typeOf(path string) (string, error) {
	if path == "$" || path == "." || path == "" {
		return jsonTypeName(d.root), nil
	}
	val, err := d.get(path)
	if err != nil {
		return "", err
	}
	return jsonTypeName(val), nil
}

func (d *JSONDocument) arrAppend(path string, values ...any) (int, error) {
	parts, err := parsePath(path)
	if err != nil {
		return 0, err
	}

	if len(parts) == 1 {
		return 0, fmt.Errorf("err cannot append to root document")
	}

	for _, p := range parts[1:] {
		if p.typ == partWildcard || p.typ == partRecursive {
			return 0, fmt.Errorf("err wildcard/recursive paths not supported")
		}
	}

	parent, lastPart, err := ensureParent(d.root, parts)
	if err != nil {
		return 0, err
	}

	var arr []any

	switch lastPart.typ {
	case partKey:
		m, ok := parent.(map[string]any)
		if !ok {
			return 0, fmt.Errorf("err existing key has wrong type")
		}
		existing, exists := m[lastPart.key]
		if exists {
			var ok bool
			arr, ok = existing.([]any)
			if !ok {
				return 0, fmt.Errorf("err existing key has wrong type")
			}
		} else {
			arr = []any{}
		}
		arr = append(arr, values...)
		m[lastPart.key] = arr
	case partIndex:
		arr2, ok := parent.([]any)
		if !ok {
			return 0, fmt.Errorf("err not an array")
		}
		idx := lastPart.idx
		if idx < 0 {
			idx = len(arr2) + idx
		}
		if idx < 0 || idx >= len(arr2) {
			return 0, fmt.Errorf("err index out of range")
		}
		arr, ok = arr2[idx].([]any)
		if !ok {
			return 0, fmt.Errorf("err existing key has wrong type")
		}
		arr = append(arr, values...)
		arr2[idx] = arr
	}

	return len(arr), nil
}

func (d *JSONDocument) arrIndex(path string, value any) (int, error) {
	val, err := d.get(path)
	if err != nil {
		return -1, err
	}
	arr, ok := val.([]any)
	if !ok {
		return -1, fmt.Errorf("err not an array")
	}

	valJSON, _ := json.Marshal(value)
	for i, elem := range arr {
		elemJSON, _ := json.Marshal(elem)
		if string(valJSON) == string(elemJSON) {
			return i, nil
		}
	}

	return -1, nil
}

func (d *JSONDocument) arrLen(path string) (int, error) {
	if path == "$" || path == "." || path == "" {
		arr, ok := d.root.([]any)
		if !ok {
			return 0, fmt.Errorf("err not an array")
		}
		return len(arr), nil
	}
	val, err := d.get(path)
	if err != nil {
		return 0, err
	}
	arr, ok := val.([]any)
	if !ok {
		return 0, fmt.Errorf("err not an array")
	}
	return len(arr), nil
}

func (d *JSONDocument) numIncrBy(path string, delta float64) (float64, error) {
	parts, err := parsePath(path)
	if err != nil {
		return 0, err
	}

	if len(parts) == 1 {
		return 0, fmt.Errorf("err cannot operate on root document")
	}

	var current float64
	val, err := d.get(path)
	if err != nil {
		val = float64(0)
		current = 0
	} else {
		switch v := val.(type) {
		case float64:
			current = v
		case int:
			current = float64(v)
		case int64:
			current = float64(v)
		default:
			return 0, fmt.Errorf("err existing key has wrong type")
		}
	}

	newVal := current + delta

	err = d.set(path, newVal)
	if err != nil {
		return 0, err
	}

	return newVal, nil
}

func (d *JSONDocument) numMultBy(path string, factor float64) (float64, error) {
	val, err := d.get(path)
	if err != nil {
		return 0, err
	}

	var current float64
	switch v := val.(type) {
	case float64:
		current = v
	case int:
		current = float64(v)
	case int64:
		current = float64(v)
	default:
		return 0, fmt.Errorf("err existing key has wrong type")
	}

	newVal := current * factor

	err = d.set(path, newVal)
	if err != nil {
		return 0, err
	}

	return newVal, nil
}

func (d *JSONDocument) objKeys(path string) ([]string, error) {
	val, err := d.get(path)
	if err != nil {
		return nil, err
	}
	m, ok := val.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("err not an object")
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys, nil
}

func (d *JSONDocument) objLen(path string) (int, error) {
	val, err := d.get(path)
	if err != nil {
		return 0, err
	}
	m, ok := val.(map[string]any)
	if !ok {
		return 0, fmt.Errorf("err not an object")
	}
	return len(m), nil
}

func (d *JSONDocument) strAppend(path string, suffix string) (int, error) {
	val, err := d.get(path)
	if err != nil {
		return 0, err
	}
	s, ok := val.(string)
	if !ok {
		return 0, fmt.Errorf("err existing key has wrong type")
	}

	newStr := s + suffix
	err = d.set(path, newStr)
	if err != nil {
		return 0, err
	}

	return len(newStr), nil
}

func (d *JSONDocument) strLen(path string) (int, error) {
	val, err := d.get(path)
	if err != nil {
		return 0, err
	}
	s, ok := val.(string)
	if !ok {
		return 0, fmt.Errorf("err existing key has wrong type")
	}
	return len(s), nil
}

func writeJSONValue(conn redcon.Conn, val any) {
	if val == nil {
		conn.WriteNull()
		return
	}
	data, err := json.Marshal(val)
	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}
	conn.WriteBulk(data)
}

func writeRESPValue(conn redcon.Conn, val any) {
	if val == nil {
		conn.WriteNull()
		return
	}
	switch v := val.(type) {
	case bool:
		if v {
			conn.WriteInt(1)
		} else {
			conn.WriteInt(0)
		}
	case float64:
		s := strconv.FormatFloat(v, 'f', -1, 64)
		conn.WriteBulkString(s)
	case string:
		conn.WriteBulkString(v)
	case []any:
		conn.WriteArray(len(v))
		for _, elem := range v {
			writeRESPValue(conn, elem)
		}
	case map[string]any:
		conn.WriteArray(len(v) * 2)
		for k, elem := range v {
			conn.WriteBulkString(k)
			writeRESPValue(conn, elem)
		}
	default:
		data, _ := json.Marshal(val)
		conn.WriteBulk(data)
	}
}

func readJSONDocument(conn redcon.Conn, db *badger.DB, key []byte) (*JSONDocument, error) {
	prefix := rawKeyPrefix(key, currentDb(conn))

	var doc *JSONDocument
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(prefix)
		if err != nil {
			return err
		}
		if item.UserMeta() != byte(RedisJSON) {
			return errWrongType
		}
		var data []byte
		err = item.Value(func(val []byte) error {
			data = append([]byte{}, val...)
			return nil
		})
		if err != nil {
			return err
		}
		doc, err = newJSONDocument(data)
		return err
	})

	return doc, err
}

var errWrongType = fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")

type fphaType int

const (
	fphaNone fphaType = iota
	fphaFP16
	fphaBF16
	fphaFP32
	fphaFP64
)

func parseFPHA(s string) (fphaType, error) {
	switch strings.ToUpper(s) {
	case "FP16":
		return fphaFP16, nil
	case "BF16":
		return fphaBF16, nil
	case "FP32":
		return fphaFP32, nil
	case "FP64":
		return fphaFP64, nil
	default:
		return fphaNone, fmt.Errorf("unsupported FP type: %s", s)
	}
}

func validateFPHA(v any, ft fphaType) error {
	switch val := v.(type) {
	case float64:
		abs := math.Abs(val)
		switch ft {
		case fphaFP64:
			return nil
		case fphaFP32, fphaBF16:
			if val != 0 && abs < float64(math.SmallestNonzeroFloat32) {
				return fmt.Errorf("value out of range")
			}
			if abs > float64(math.MaxFloat32) {
				return fmt.Errorf("value out of range")
			}
		case fphaFP16:
			const fp16Max = 65504.0
			const fp16Min = 6.1035e-5
			if val != 0 && abs < fp16Min {
				return fmt.Errorf("value out of range")
			}
			if abs > fp16Max {
				return fmt.Errorf("value out of range")
			}
		}
		return nil
	case map[string]any:
		for _, vv := range val {
			if err := validateFPHA(vv, ft); err != nil {
				return err
			}
		}
	case []any:
		for _, vv := range val {
			if err := validateFPHA(vv, ft); err != nil {
				return err
			}
		}
	}
	return nil
}

func handleJSONSet(conn redcon.Conn, db *badger.DB, cmd redcon.Command) {
	if len(cmd.Args) < 4 {
		conn.WriteError("ERR wrong number of arguments for 'json.set' command")
		return
	}

	key := cmd.Args[1]
	path := string(cmd.Args[2])

	var value any
	if err := json.Unmarshal(cmd.Args[3], &value); err != nil {
		conn.WriteError("ERR invalid JSON")
		return
	}

	nx := false
	xx := false
	var ft fphaType = fphaNone
	for i := 4; i < len(cmd.Args); i++ {
		flag := strings.ToUpper(string(cmd.Args[i]))
		switch flag {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "FPHA":
			if i+1 >= len(cmd.Args) {
				conn.WriteError("ERR syntax error")
				return
			}
			i++
			parsed, err := parseFPHA(string(cmd.Args[i]))
			if err != nil {
				conn.WriteError("ERR syntax error")
				return
			}
			ft = parsed
		default:
			conn.WriteError("ERR syntax error")
			return
		}
	}
	if nx && xx {
		conn.WriteError("ERR NX and XX are mutually exclusive")
		return
	}
	if ft != fphaNone {
		if err := validateFPHA(value, ft); err != nil {
			conn.WriteError(err.Error())
			return
		}
	}

	prefix := rawKeyPrefix(key, currentDb(conn))

	err := db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(prefix)

		var doc *JSONDocument
		keyExists := false

		if err == nil {
			if item.UserMeta() != byte(RedisJSON) {
				return errWrongType
			}
			var data []byte
			err = item.Value(func(val []byte) error {
				data = append([]byte{}, val...)
				return nil
			})
			if err != nil {
				return err
			}
			doc, err = newJSONDocument(data)
			if err != nil {
				return fmt.Errorf("ERR existing JSON document is corrupted")
			}
			keyExists = true
		} else if err == badger.ErrKeyNotFound {
			doc = newEmptyJSONDocument()
		} else {
			return err
		}

		if path == "$" || path == "." {
			if nx && keyExists {
				return fmt.Errorf("skip")
			}
			if xx && !keyExists {
				return fmt.Errorf("skip")
			}
			doc.root = value
		} else {
			if nx {
				_, err := doc.get(path)
				if err == nil {
					return fmt.Errorf("skip")
				}
			}
			if xx {
				_, err := doc.get(path)
				if err != nil {
					return fmt.Errorf("skip")
				}
			}
			if err := doc.set(path, value); err != nil {
				return err
			}
		}

		data, err := doc.serialize()
		if err != nil {
			return err
		}

		e := badger.NewEntry(prefix, data).WithMeta(byte(RedisJSON))
		return txn.SetEntry(e)
	})

	if err != nil {
		if err.Error() == "skip" {
			conn.WriteNull()
		} else if err.Error() == "WRONGTYPE Operation against a key holding the wrong kind of value" {
			conn.WriteError(err.Error())
		} else {
			conn.WriteError("ERR " + err.Error())
		}
		return
	}

	conn.WriteString("OK")
}

func handleJSONGet(conn redcon.Conn, db *badger.DB, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for 'json.get' command")
		return
	}

	key := cmd.Args[1]
	doc, err := readJSONDocument(conn, db, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			conn.WriteNull()
		} else if err == errWrongType {
			conn.WriteError(err.Error())
		} else {
			conn.WriteError("ERR " + err.Error())
		}
		return
	}

	if len(cmd.Args) == 2 {
		writeJSONValue(conn, doc.root)
		return
	}

	paths := cmd.Args[2:]
	if len(paths) == 1 {
		path := string(paths[0])
		val, err := doc.get(path)
		if err != nil {
			conn.WriteNull()
			return
		}
		writeJSONValue(conn, val)
		return
	}

	result := make(map[string]any, len(paths))
	for _, p := range paths {
		pathStr := string(p)
		val, err := doc.get(pathStr)
		if err != nil {
			result[pathStr] = nil
		} else {
			result[pathStr] = val
		}
	}

	data, _ := json.Marshal(result)
	conn.WriteBulk(data)
}

func handleJSONDel(conn redcon.Conn, db *badger.DB, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for 'json.del' command")
		return
	}

	key := cmd.Args[1]
	prefix := rawKeyPrefix(key, currentDb(conn))

	if len(cmd.Args) == 2 {
		err := db.Update(func(txn *badger.Txn) error {
			item, err := txn.Get(prefix)
			if err != nil {
				if err == badger.ErrKeyNotFound {
					return nil
				}
				return err
			}
			if item.UserMeta() != byte(RedisJSON) {
				return errWrongType
			}
			return txn.Delete(prefix)
		})
		if err != nil {
			if err == errWrongType {
				conn.WriteError(err.Error())
			} else {
				conn.WriteError("ERR " + err.Error())
			}
			return
		}
		conn.WriteInt(1)
		return
	}

	deleted := 0
	err := db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(prefix)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}
		if item.UserMeta() != byte(RedisJSON) {
			return errWrongType
		}
		var data []byte
		err = item.Value(func(val []byte) error {
			data = append([]byte{}, val...)
			return nil
		})
		if err != nil {
			return err
		}
		doc, err := newJSONDocument(data)
		if err != nil {
			return err
		}

		for i := 2; i < len(cmd.Args); i++ {
			pathStr := string(cmd.Args[i])
			err := doc.delete(pathStr)
			if err == nil {
				deleted++
			}
		}

		if deleted > 0 {
			newData, err := doc.serialize()
			if err != nil {
				return err
			}
			e := badger.NewEntry(prefix, newData).WithMeta(byte(RedisJSON))
			return txn.SetEntry(e)
		}
		return nil
	})
	if err != nil {
		if err == errWrongType {
			conn.WriteError(err.Error())
		} else {
			conn.WriteError("ERR " + err.Error())
		}
		return
	}
	conn.WriteInt(deleted)
}

func handleJSONType(conn redcon.Conn, db *badger.DB, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for 'json.type' command")
		return
	}

	key := cmd.Args[1]
	doc, err := readJSONDocument(conn, db, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			conn.WriteNull()
		} else if err == errWrongType {
			conn.WriteError(err.Error())
		} else {
			conn.WriteError("ERR " + err.Error())
		}
		return
	}

	path := "$"
	if len(cmd.Args) >= 3 {
		path = string(cmd.Args[2])
	}

	typeName, err := doc.typeOf(path)
	if err != nil {
		conn.WriteNull()
		return
	}
	conn.WriteBulkString(typeName)
}

func handleJSONArrAppend(conn redcon.Conn, db *badger.DB, cmd redcon.Command) {
	if len(cmd.Args) < 4 {
		conn.WriteError("ERR wrong number of arguments for 'json.arrappend' command")
		return
	}

	key := cmd.Args[1]
	path := string(cmd.Args[2])

	values := make([]any, 0, len(cmd.Args)-3)
	for i := 3; i < len(cmd.Args); i++ {
		var v any
		if err := json.Unmarshal(cmd.Args[i], &v); err != nil {
			conn.WriteError("ERR invalid JSON")
			return
		}
		values = append(values, v)
	}

	prefix := rawKeyPrefix(key, currentDb(conn))
	var newLen int

	err := db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(prefix)
		if err != nil {
			return err
		}
		if item.UserMeta() != byte(RedisJSON) {
			return errWrongType
		}
		var data []byte
		err = item.Value(func(val []byte) error {
			data = append([]byte{}, val...)
			return nil
		})
		if err != nil {
			return err
		}
		doc, err := newJSONDocument(data)
		if err != nil {
			return err
		}

		newLen, err = doc.arrAppend(path, values...)
		if err != nil {
			return err
		}

		newData, err := doc.serialize()
		if err != nil {
			return err
		}
		e := badger.NewEntry(prefix, newData).WithMeta(byte(RedisJSON))
		return txn.SetEntry(e)
	})

	if err != nil {
		if err == badger.ErrKeyNotFound {
			conn.WriteNull()
		} else if err == errWrongType {
			conn.WriteError(err.Error())
		} else {
			conn.WriteError("ERR " + err.Error())
		}
		return
	}

	conn.WriteInt(newLen)
}

func handleJSONArrIndex(conn redcon.Conn, db *badger.DB, cmd redcon.Command) {
	if len(cmd.Args) < 4 {
		conn.WriteError("ERR wrong number of arguments for 'json.arrindex' command")
		return
	}

	key := cmd.Args[1]
	path := string(cmd.Args[2])

	var value any
	if err := json.Unmarshal(cmd.Args[3], &value); err != nil {
		conn.WriteError("ERR invalid JSON")
		return
	}

	doc, err := readJSONDocument(conn, db, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			conn.WriteNull()
		} else if err == errWrongType {
			conn.WriteError(err.Error())
		} else {
			conn.WriteError("ERR " + err.Error())
		}
		return
	}

	idx, err := doc.arrIndex(path, value)
	if err != nil {
		conn.WriteInt(-1)
		return
	}
	conn.WriteInt(idx)
}

func handleJSONArrLen(conn redcon.Conn, db *badger.DB, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for 'json.arrlen' command")
		return
	}

	key := cmd.Args[1]
	path := "$"
	if len(cmd.Args) >= 3 {
		path = string(cmd.Args[2])
	}

	doc, err := readJSONDocument(conn, db, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			conn.WriteNull()
		} else if err == errWrongType {
			conn.WriteError(err.Error())
		} else {
			conn.WriteError("ERR " + err.Error())
		}
		return
	}

	length, err := doc.arrLen(path)
	if err != nil {
		conn.WriteNull()
		return
	}
	conn.WriteInt(length)
}

func handleJSONNumIncrBy(conn redcon.Conn, db *badger.DB, cmd redcon.Command) {
	if len(cmd.Args) < 4 {
		conn.WriteError("ERR wrong number of arguments for 'json.numincrby' command")
		return
	}

	key := cmd.Args[1]
	path := string(cmd.Args[2])

	delta, err := strconv.ParseFloat(string(cmd.Args[3]), 64)
	if err != nil {
		conn.WriteError("ERR value is not a number")
		return
	}

	prefix := rawKeyPrefix(key, currentDb(conn))
	var newVal float64

	err = db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(prefix)
		if err != nil {
			return err
		}
		if item.UserMeta() != byte(RedisJSON) {
			return errWrongType
		}
		var data []byte
		err = item.Value(func(val []byte) error {
			data = append([]byte{}, val...)
			return nil
		})
		if err != nil {
			return err
		}
		doc, err := newJSONDocument(data)
		if err != nil {
			return err
		}

		newVal, err = doc.numIncrBy(path, delta)
		if err != nil {
			return err
		}

		newData, err := doc.serialize()
		if err != nil {
			return err
		}
		e := badger.NewEntry(prefix, newData).WithMeta(byte(RedisJSON))
		return txn.SetEntry(e)
	})

	if err != nil {
		if err == badger.ErrKeyNotFound {
			conn.WriteNull()
		} else if err == errWrongType {
			conn.WriteError(err.Error())
		} else {
			conn.WriteError("ERR " + err.Error())
		}
		return
	}

	conn.WriteBulkString(strconv.FormatFloat(newVal, 'f', -1, 64))
}

func handleJSONNumMultBy(conn redcon.Conn, db *badger.DB, cmd redcon.Command) {
	if len(cmd.Args) < 4 {
		conn.WriteError("ERR wrong number of arguments for 'json.nummultby' command")
		return
	}

	key := cmd.Args[1]
	path := string(cmd.Args[2])

	factor, err := strconv.ParseFloat(string(cmd.Args[3]), 64)
	if err != nil {
		conn.WriteError("ERR value is not a number")
		return
	}

	prefix := rawKeyPrefix(key, currentDb(conn))
	var newVal float64

	err = db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(prefix)
		if err != nil {
			return err
		}
		if item.UserMeta() != byte(RedisJSON) {
			return errWrongType
		}
		var data []byte
		err = item.Value(func(val []byte) error {
			data = append([]byte{}, val...)
			return nil
		})
		if err != nil {
			return err
		}
		doc, err := newJSONDocument(data)
		if err != nil {
			return err
		}

		newVal, err = doc.numMultBy(path, factor)
		if err != nil {
			return err
		}

		newData, err := doc.serialize()
		if err != nil {
			return err
		}
		e := badger.NewEntry(prefix, newData).WithMeta(byte(RedisJSON))
		return txn.SetEntry(e)
	})

	if err != nil {
		if err == badger.ErrKeyNotFound {
			conn.WriteNull()
		} else if err == errWrongType {
			conn.WriteError(err.Error())
		} else {
			conn.WriteError("ERR " + err.Error())
		}
		return
	}

	conn.WriteBulkString(strconv.FormatFloat(newVal, 'f', -1, 64))
}

func handleJSONObjKeys(conn redcon.Conn, db *badger.DB, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for 'json.objkeys' command")
		return
	}

	key := cmd.Args[1]
	path := "$"
	if len(cmd.Args) >= 3 {
		path = string(cmd.Args[2])
	}

	doc, err := readJSONDocument(conn, db, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			conn.WriteNull()
		} else if err == errWrongType {
			conn.WriteError(err.Error())
		} else {
			conn.WriteError("ERR " + err.Error())
		}
		return
	}

	keys, err := doc.objKeys(path)
	if err != nil {
		conn.WriteNull()
		return
	}

	conn.WriteArray(len(keys))
	for _, k := range keys {
		conn.WriteBulkString(k)
	}
}

func handleJSONObjLen(conn redcon.Conn, db *badger.DB, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for 'json.objlen' command")
		return
	}

	key := cmd.Args[1]
	path := "$"
	if len(cmd.Args) >= 3 {
		path = string(cmd.Args[2])
	}

	doc, err := readJSONDocument(conn, db, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			conn.WriteNull()
		} else if err == errWrongType {
			conn.WriteError(err.Error())
		} else {
			conn.WriteError("ERR " + err.Error())
		}
		return
	}

	length, err := doc.objLen(path)
	if err != nil {
		conn.WriteNull()
		return
	}
	conn.WriteInt(length)
}

func handleJSONStrAppend(conn redcon.Conn, db *badger.DB, cmd redcon.Command) {
	if len(cmd.Args) < 4 {
		conn.WriteError("ERR wrong number of arguments for 'json.strappend' command")
		return
	}

	key := cmd.Args[1]
	path := "$"
	valueIdx := 3

	if len(cmd.Args) == 4 {
		path = string(cmd.Args[2])
		valueIdx = 3
	} else if len(cmd.Args) == 3 {
		valueIdx = 2
	}

	var suffix string
	if err := json.Unmarshal(cmd.Args[valueIdx], &suffix); err != nil {
		conn.WriteError("ERR invalid JSON string")
		return
	}

	prefix := rawKeyPrefix(key, currentDb(conn))
	var newLen int

	err := db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(prefix)
		if err != nil {
			return err
		}
		if item.UserMeta() != byte(RedisJSON) {
			return errWrongType
		}
		var data []byte
		err = item.Value(func(val []byte) error {
			data = append([]byte{}, val...)
			return nil
		})
		if err != nil {
			return err
		}
		doc, err := newJSONDocument(data)
		if err != nil {
			return err
		}

		newLen, err = doc.strAppend(path, suffix)
		if err != nil {
			return err
		}

		newData, err := doc.serialize()
		if err != nil {
			return err
		}
		e := badger.NewEntry(prefix, newData).WithMeta(byte(RedisJSON))
		return txn.SetEntry(e)
	})

	if err != nil {
		if err == badger.ErrKeyNotFound {
			conn.WriteNull()
		} else if err == errWrongType {
			conn.WriteError(err.Error())
		} else {
			conn.WriteError("ERR " + err.Error())
		}
		return
	}

	conn.WriteInt(newLen)
}

func handleJSONStrLen(conn redcon.Conn, db *badger.DB, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for 'json.strlen' command")
		return
	}

	key := cmd.Args[1]
	path := "$"
	if len(cmd.Args) >= 3 {
		path = string(cmd.Args[2])
	}

	doc, err := readJSONDocument(conn, db, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			conn.WriteNull()
		} else if err == errWrongType {
			conn.WriteError(err.Error())
		} else {
			conn.WriteError("ERR " + err.Error())
		}
		return
	}

	length, err := doc.strLen(path)
	if err != nil {
		conn.WriteNull()
		return
	}
	conn.WriteInt(length)
}

func handleJSONMGet(conn redcon.Conn, db *badger.DB, cmd redcon.Command) {
	if len(cmd.Args) < 3 {
		conn.WriteError("ERR wrong number of arguments for 'json.mget' command")
		return
	}

	path := string(cmd.Args[len(cmd.Args)-1])
	keys := cmd.Args[1 : len(cmd.Args)-1]

	conn.WriteArray(len(keys))

	for _, key := range keys {
		doc, err := readJSONDocument(conn, db, key)
		if err != nil {
			conn.WriteNull()
			continue
		}

		val, err := doc.get(path)
		if err != nil {
			conn.WriteNull()
			continue
		}

		writeJSONValue(conn, val)
	}
}

func handleJSONResp(conn redcon.Conn, db *badger.DB, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for 'json.resp' command")
		return
	}

	key := cmd.Args[1]
	doc, err := readJSONDocument(conn, db, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			conn.WriteNull()
		} else if err == errWrongType {
			conn.WriteError(err.Error())
		} else {
			conn.WriteError("ERR " + err.Error())
		}
		return
	}

	var val any
	if len(cmd.Args) >= 3 {
		path := string(cmd.Args[2])
		val, err = doc.get(path)
		if err != nil {
			conn.WriteNull()
			return
		}
	} else {
		val = doc.root
	}

	writeRESPValue(conn, val)
}

func handleJSONClear(conn redcon.Conn, db *badger.DB, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for 'json.clear' command")
		return
	}

	key := cmd.Args[1]
	path := "$"
	if len(cmd.Args) >= 3 {
		path = string(cmd.Args[2])
	}

	prefix := rawKeyPrefix(key, currentDb(conn))
	var cleared int

	err := db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(prefix)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}
		if item.UserMeta() != byte(RedisJSON) {
			return errWrongType
		}
		var data []byte
		err = item.Value(func(val []byte) error {
			data = append([]byte{}, val...)
			return nil
		})
		if err != nil {
			return err
		}
		doc, err := newJSONDocument(data)
		if err != nil {
			return err
		}

		if path == "$" || path == "." {
			doc.root = make(map[string]any)
			cleared = 1
		} else {
			val, err := doc.get(path)
			if err != nil {
				return nil
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
				return err
			}
			e := badger.NewEntry(prefix, newData).WithMeta(byte(RedisJSON))
			return txn.SetEntry(e)
		}
		return nil
	})

	if err != nil {
		if err == errWrongType {
			conn.WriteError(err.Error())
		} else {
			conn.WriteError("ERR " + err.Error())
		}
		return
	}
	conn.WriteInt(cleared)
}

func handleJSONArrPop(conn redcon.Conn, db *badger.DB, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for 'json.arrpop' command")
		return
	}

	key := cmd.Args[1]
	path := "$"
	idx := -1

	if len(cmd.Args) >= 3 {
		path = string(cmd.Args[2])
	}
	if len(cmd.Args) >= 4 {
		var err error
		idx, err = strconv.Atoi(string(cmd.Args[3]))
		if err != nil {
			conn.WriteError("ERR value is not an integer or out of range")
			return
		}
	}

	prefix := rawKeyPrefix(key, currentDb(conn))
	var popped any

	err := db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(prefix)
		if err != nil {
			return err
		}
		if item.UserMeta() != byte(RedisJSON) {
			return errWrongType
		}
		var data []byte
		err = item.Value(func(val []byte) error {
			data = append([]byte{}, val...)
			return nil
		})
		if err != nil {
			return err
		}
		doc, err := newJSONDocument(data)
		if err != nil {
			return err
		}

		val, err := doc.get(path)
		if err != nil {
			return fmt.Errorf("err path does not exist")
		}
		arr, ok := val.([]any)
		if !ok {
			return fmt.Errorf("err not an array")
		}
		if len(arr) == 0 {
			return fmt.Errorf("skip")
		}

		popIdx := idx
		if popIdx < 0 {
			popIdx = len(arr) + popIdx
		}
		if popIdx < 0 || popIdx >= len(arr) {
			return fmt.Errorf("err index out of range")
		}

		popped = arr[popIdx]
		arr = append(arr[:popIdx], arr[popIdx+1:]...)

		parts, err := parsePath(path)
		if err != nil {
			return err
		}
		if len(parts) == 1 {
			doc.root = arr
		} else {
			parent, lastPart, err := ensureParent(doc.root, parts)
			if err != nil {
				return err
			}
			switch lastPart.typ {
			case partKey:
				parent.(map[string]any)[lastPart.key] = arr
			case partIndex:
				parent.([]any)[lastPart.idx] = arr
			}
		}

		newData, err := doc.serialize()
		if err != nil {
			return err
		}
		e := badger.NewEntry(prefix, newData).WithMeta(byte(RedisJSON))
		return txn.SetEntry(e)
	})

	if err != nil {
		if err == badger.ErrKeyNotFound {
			conn.WriteNull()
		} else if err == errWrongType {
			conn.WriteError(err.Error())
		} else if err.Error() == "skip" {
			conn.WriteNull()
		} else {
			conn.WriteError("ERR " + err.Error())
		}
		return
	}

	writeJSONValue(conn, popped)
}

func handleJSONArrTrim(conn redcon.Conn, db *badger.DB, cmd redcon.Command) {
	if len(cmd.Args) < 4 {
		conn.WriteError("ERR wrong number of arguments for 'json.arrtrim' command")
		return
	}

	key := cmd.Args[1]
	path := string(cmd.Args[2])

	start, err := strconv.Atoi(string(cmd.Args[3]))
	if err != nil {
		conn.WriteError("ERR value is not an integer or out of range")
		return
	}
	stop := -1
	if len(cmd.Args) >= 5 {
		stop, err = strconv.Atoi(string(cmd.Args[4]))
		if err != nil {
			conn.WriteError("ERR value is not an integer or out of range")
			return
		}
	}

	prefix := rawKeyPrefix(key, currentDb(conn))
	var newLen int

	err = db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(prefix)
		if err != nil {
			return err
		}
		if item.UserMeta() != byte(RedisJSON) {
			return errWrongType
		}
		var data []byte
		err = item.Value(func(val []byte) error {
			data = append([]byte{}, val...)
			return nil
		})
		if err != nil {
			return err
		}
		doc, err := newJSONDocument(data)
		if err != nil {
			return err
		}

		val, err := doc.get(path)
		if err != nil {
			return fmt.Errorf("err path does not exist")
		}
		arr, ok := val.([]any)
		if !ok {
			return fmt.Errorf("err not an array")
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

		parts, err := parsePath(path)
		if err != nil {
			return err
		}
		if len(parts) == 1 {
			doc.root = arr
		} else {
			parent, lastPart, err := ensureParent(doc.root, parts)
			if err != nil {
				return err
			}
			switch lastPart.typ {
			case partKey:
				parent.(map[string]any)[lastPart.key] = arr
			case partIndex:
				parent.([]any)[lastPart.idx] = arr
			}
		}

		newLen = len(arr)
		newData, err := doc.serialize()
		if err != nil {
			return err
		}
		e := badger.NewEntry(prefix, newData).WithMeta(byte(RedisJSON))
		return txn.SetEntry(e)
	})

	if err != nil {
		if err == badger.ErrKeyNotFound {
			conn.WriteNull()
		} else if err == errWrongType {
			conn.WriteError(err.Error())
		} else {
			conn.WriteError("ERR " + err.Error())
		}
		return
	}

	conn.WriteInt(newLen)
}

func handleJSONArrInsert(conn redcon.Conn, db *badger.DB, cmd redcon.Command) {
	if len(cmd.Args) < 5 {
		conn.WriteError("ERR wrong number of arguments for 'json.arrinsert' command")
		return
	}

	key := cmd.Args[1]
	path := string(cmd.Args[2])
	index, err := strconv.Atoi(string(cmd.Args[3]))
	if err != nil {
		conn.WriteError("ERR value is not an integer or out of range")
		return
	}

	values := make([]any, 0, len(cmd.Args)-4)
	for i := 4; i < len(cmd.Args); i++ {
		var v any
		if err := json.Unmarshal(cmd.Args[i], &v); err != nil {
			conn.WriteError("ERR invalid JSON")
			return
		}
		values = append(values, v)
	}

	prefix := rawKeyPrefix(key, currentDb(conn))
	var newLen int

	err = db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(prefix)
		if err != nil {
			return err
		}
		if item.UserMeta() != byte(RedisJSON) {
			return errWrongType
		}
		var data []byte
		err = item.Value(func(val []byte) error {
			data = append([]byte{}, val...)
			return nil
		})
		if err != nil {
			return err
		}
		doc, err := newJSONDocument(data)
		if err != nil {
			return err
		}

		val, err := doc.get(path)
		if err != nil {
			return fmt.Errorf("err path does not exist")
		}
		arr, ok := val.([]any)
		if !ok {
			return fmt.Errorf("err not an array")
		}

		if index < 0 || index > len(arr) {
			return fmt.Errorf("err index out of range")
		}

		newArr := make([]any, 0, len(arr)+len(values))
		newArr = append(newArr, arr[:index]...)
		newArr = append(newArr, values...)
		newArr = append(newArr, arr[index:]...)

		parts, err := parsePath(path)
		if err != nil {
			return err
		}
		if len(parts) == 1 {
			doc.root = newArr
		} else {
			parent, lastPart, err := ensureParent(doc.root, parts)
			if err != nil {
				return err
			}
			switch lastPart.typ {
			case partKey:
				parent.(map[string]any)[lastPart.key] = newArr
			case partIndex:
				parent.([]any)[lastPart.idx] = newArr
			}
		}

		newLen = len(newArr)
		newData, err := doc.serialize()
		if err != nil {
			return err
		}
		e := badger.NewEntry(prefix, newData).WithMeta(byte(RedisJSON))
		return txn.SetEntry(e)
	})

	if err != nil {
		if err == badger.ErrKeyNotFound {
			conn.WriteNull()
		} else if err == errWrongType {
			conn.WriteError(err.Error())
		} else {
			conn.WriteError("ERR " + err.Error())
		}
		return
	}

	conn.WriteInt(newLen)
}
