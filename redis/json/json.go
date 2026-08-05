// Package json implements the RedisJSON commands on top of the kv store.
// A JSON document is stored as a single public key holding its serialized
// bytes, tagged with common.RedisJSON metadata. Documents are loaded,
// modified and re-serialized atomically within a single kv transaction.
package json

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/hardpointlabs/invar/kv"
	"github.com/hardpointlabs/invar/redis/common"
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

var errWrongType = fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")

// errSkip is a sentinel used inside JSON transactions to signal "do nothing, return null".
var errSkip = fmt.Errorf("skip")

// writeJSONErr writes the appropriate RESP error response for a JSON command error.
func writeJSONErr(conn redcon.Conn, err error) {
	if err == kv.ErrKeyNotFound || err == errSkip {
		conn.WriteNull()
	} else if err == errWrongType {
		conn.WriteError(err.Error())
	} else {
		conn.WriteError("ERR " + err.Error())
	}
}

// getDoc loads the JSON document at key. It returns kv.ErrKeyNotFound when the
// key is missing and errWrongType when it holds a non-JSON value.
func getDoc(tx kv.Tx, s *common.Session, key []byte) (*JSONDocument, error) {
	item, err := tx.Get(s.PublicKey(key))
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
	return newJSONDocument(data)
}

// updateDoc loads the JSON document at key, runs fn against it, then
// serializes and saves it back within the same transaction.
func updateDoc(tx kv.Tx, s *common.Session, key []byte, fn func(*JSONDocument) error) error {
	doc, err := getDoc(tx, s, key)
	if err != nil {
		return err
	}
	if err := fn(doc); err != nil {
		return err
	}
	newData, err := doc.serialize()
	if err != nil {
		return err
	}
	return tx.Set(s.NewPublicEntry(key, newData).Metadata(byte(common.RedisJSON)))
}

// setArrayResult writes the given array back into the document at path,
// handling both root-level and nested paths.
func setArrayResult(doc *JSONDocument, path string, arr []any) error {
	parts, err := parsePath(path)
	if err != nil {
		return err
	}
	if len(parts) == 1 {
		doc.root = arr
		return nil
	}
	parent, lastPart, err := ensureParent(doc.root, parts)
	if err != nil {
		return err
	}
	switch lastPart.typ {
	case partKey:
		m, ok := parent.(map[string]any)
		if !ok {
			return fmt.Errorf("err existing key has wrong type")
		}
		m[lastPart.key] = arr
	case partIndex:
		a, ok := parent.([]any)
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
		a[idx] = arr
	default:
		return fmt.Errorf("err unexpected path part")
	}
	return nil
}

// fphaType identifies the floating-point half-precision format requested via
// the JSON.SET FPHA option.
type fphaType int

const (
	FphaNone fphaType = iota
	FphaFP16
	FphaBF16
	FphaFP32
	FphaFP64
)

// ParseFPHA parses a FPHA flag value.
func ParseFPHA(s string) (fphaType, error) {
	switch strings.ToUpper(s) {
	case "FP16":
		return FphaFP16, nil
	case "BF16":
		return FphaBF16, nil
	case "FP32":
		return FphaFP32, nil
	case "FP64":
		return FphaFP64, nil
	default:
		return FphaNone, fmt.Errorf("unsupported FP type: %s", s)
	}
}

// ValidateFPHA checks that value is representable under the given FPHA format.
func ValidateFPHA(v any, ft fphaType) error {
	switch val := v.(type) {
	case float64:
		abs := math.Abs(val)
		switch ft {
		case FphaFP64:
			return nil
		case FphaFP32, FphaBF16:
			if val != 0 && abs < float64(math.SmallestNonzeroFloat32) {
				return fmt.Errorf("value out of range")
			}
			if abs > float64(math.MaxFloat32) {
				return fmt.Errorf("value out of range")
			}
		case FphaFP16:
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
			if err := ValidateFPHA(vv, ft); err != nil {
				return err
			}
		}
	case []any:
		for _, vv := range val {
			if err := ValidateFPHA(vv, ft); err != nil {
				return err
			}
		}
	}
	return nil
}
