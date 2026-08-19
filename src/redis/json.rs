//! RedisJSON commands: `JSON.SET`, `JSON.GET`, `JSON.DEL`, `JSON.TYPE`,
//! `JSON.ARRAPPEND`, `JSON.ARRINDEX`, `JSON.ARRLEN`, `JSON.NUMINCRBY`,
//! `JSON.NUMMULTBY`, `JSON.OBJKEYS`, `JSON.OBJLEN`, `JSON.STRAPPEND`,
//! `JSON.STRLEN`, `JSON.MGET`, `JSON.RESP`, `JSON.CLEAR`, `JSON.ARRPOP`,
//! `JSON.ARRTRIM`, and `JSON.ARRINSERT`.
//!
//! Port of the Go `redis/json` package. A JSON document is stored as a single
//! public-key entry holding its serialized bytes, stamped with
//! `ValueType::Json` metadata. Documents are loaded, modified, and
//! re-serialized atomically within a single kv transaction.
//!
//! The serialized (wire) form deliberately mirrors Go's `encoding/json`
//! output: integral floats print without a decimal point, `1e-6 <= |x| < 1e21`
//! prints in fixed notation, cheaper values use scientific notation with a
//! `+` exponent sign, and strings escape the HTML-sensitive characters
//! (`<`, `>`, `&`) plus U+2028/U+2029 like Go's encoder.

use std::collections::BTreeMap;

use bytes::Bytes;
use kv::kv::{BoxFuture, Entry, Error as KvError, Tx};

use crate::common::op::{err_resp, DbError, DbOp, DbResult, QueuedOp, WireOp};
use crate::common::session::Session;
use crate::common::ValueType;
use crate::resp::RespValue;

/// The metadata byte stamped on every JSON document entry.
const TYPE_JSON: u8 = ValueType::Json as u8;

// --- document model -------------------------------------------------------

/// A parsed JSON document. Numbers are stored as `f64`, mirroring Go's use of
/// `float64` for every JSON number after `json.Unmarshal`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum JValue {
    Null,
    Bool(bool),
    Number(f64),
    Str(String),
    Arr(Vec<JValue>),
    Obj(BTreeMap<String, JValue>),
}

/// Converts a `serde_json::Value` (the parse output) into our document model.
fn from_serde(v: serde_json::Value) -> JValue {
    match v {
        serde_json::Value::Null => JValue::Null,
        serde_json::Value::Bool(b) => JValue::Bool(b),
        serde_json::Value::Number(n) => JValue::Number(n.as_f64().unwrap_or(f64::NAN)),
        serde_json::Value::String(s) => JValue::Str(s),
        serde_json::Value::Array(items) => JValue::Arr(items.into_iter().map(from_serde).collect()),
        serde_json::Value::Object(m) => {
            JValue::Obj(m.into_iter().map(|(k, v)| (k, from_serde(v))).collect())
        }
    }
}

/// Parses raw JSON bytes into our document model; `None` on invalid JSON.
pub(crate) fn parse_json(bytes: &[u8]) -> Option<JValue> {
    serde_json::from_slice(bytes).ok().map(from_serde)
}

/// Parses raw JSON bytes that must decode to a JSON string, returning that
/// string. Used by `JSON.STRAPPEND`, mirroring Go's `json.Unmarshal` into a
/// `*string`.
pub fn parse_json_string(bytes: &[u8]) -> Option<String> {
    match serde_json::from_slice::<serde_json::Value>(bytes).ok()? {
        serde_json::Value::String(s) => Some(s),
        _ => None,
    }
}

/// Formats `f` the way Go's `encoding/json` encoder does for stored values.
///
/// Go writes `strconv` shortest fixed notation when `1e-6 <= |x| < 1e21`
/// (including zero), otherwise shortest scientific notation with a `+` on
/// positive exponents (the `e-09`→`e-9` cleanup is already how Rust prints).
fn go_json_number(f: f64) -> String {
    let abs = f.abs();
    if abs != 0.0 && (abs < 1e-6 || abs >= 1e21) {
        let s = format!("{f:e}");
        if let Some(dot_e) = s.find('e') {
            let (mant, exp) = s.split_at(dot_e);
            let exp = &exp[1..];
            let sign = if exp.starts_with('-') { "" } else { "+" };
            return format!("{mant}e{sign}{exp}");
        }
        s
    } else {
        format!("{f}")
    }
}

/// Appends `s` as a Go-escaped JSON string: HTML-sensitive characters,
/// control characters below `0x20` (with `\n`/`\r`/`\t` short forms), and
/// U+2028/U+2029.
fn push_json_string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\u0008"),
            '\u{c}' => out.push_str("\\u000c"),
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '&' => out.push_str("\\u0026"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            _ if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Serializes a document value with Go-compatible formatting.
fn serialize_value(v: &JValue) -> String {
    match v {
        JValue::Null => "null".to_string(),
        JValue::Bool(true) => "true".to_string(),
        JValue::Bool(false) => "false".to_string(),
        JValue::Number(f) => go_json_number(*f),
        JValue::Str(s) => {
            let mut out = String::new();
            push_json_string(&mut out, s);
            out
        }
        JValue::Arr(items) => {
            let mut out = String::from("[");
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&serialize_value(item));
            }
            out.push(']');
            out
        }
        JValue::Obj(map) => {
            let mut out = String::from("{");
            for (i, (k, val)) in map.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                push_json_string(&mut out, k);
                out.push(':');
                out.push_str(&serialize_value(val));
            }
            out.push('}');
            out
        }
    }
}

/// Formats a float the way Go's `redis/common.FormatFloat` does (used by the
/// `JSON.NUMINCRBY`/`JSON.NUMMULTBY` wire replies and `JSON.RESP` numbers);
/// `inf`/`-inf` for the infinities, otherwise shortest round-trip digits.
fn format_float(v: f64) -> String {
    if v > 0.0 && v.is_infinite() {
        return "inf".to_string();
    }
    if v < 0.0 && v.is_infinite() {
        return "-inf".to_string();
    }
    format!("{v}")
}

/// The JSON type name for a value, as returned by `JSON.TYPE`.
fn json_type_name(v: &JValue) -> &'static str {
    match v {
        JValue::Null => "null",
        JValue::Bool(_) => "boolean",
        JValue::Number(_) => "number",
        JValue::Str(_) => "string",
        JValue::Arr(_) => "array",
        JValue::Obj(_) => "object",
    }
}

// --- path engine ----------------------------------------------------------

/// One element of a parsed JSON path.
#[derive(Debug, Clone, PartialEq)]
enum Part {
    Root,
    Key(String),
    Index(i64),
    Wildcard,
    Recursive,
}

/// Parses a JSONPath-style path, mirroring Go's `parsePath` including its
/// exact error strings.
fn parse_path(s: &str) -> Result<Vec<Part>, String> {
    if s.is_empty() {
        return Err("err path cannot be empty".to_string());
    }

    let b = s.as_bytes();
    let mut parts = Vec::new();
    let mut i = 0usize;

    if b[0] == b'$' {
        parts.push(Part::Root);
        i = 1;
    } else if b[0] == b'.' || b[0] == b'[' {
        parts.push(Part::Root);
    } else {
        return Err("err path must start with $".to_string());
    }

    while i < b.len() {
        let ch = b[i];
        match ch {
            b'.' => {
                i += 1;
                if i < b.len() && b[i] == b'.' {
                    parts.push(Part::Recursive);
                    i += 1;
                    // After `..`, consume the following key/wildcard (no dot
                    // needed).
                    if i < b.len() && b[i] == b'*' {
                        parts.push(Part::Wildcard);
                        i += 1;
                    } else if i < b.len() && b[i] == b'[' {
                        // bracket will be handled by next iteration
                    } else if i < b.len() {
                        let start = i;
                        while i < b.len() && !matches!(b[i], b'.' | b'[' | b'*') {
                            i += 1;
                        }
                        if start < i {
                            parts.push(Part::Key(s[start..i].to_string()));
                        }
                    }
                } else if i < b.len() && b[i] == b'*' {
                    parts.push(Part::Wildcard);
                    i += 1;
                } else {
                    let start = i;
                    while i < b.len() && !matches!(b[i], b'.' | b'[' | b'*') {
                        i += 1;
                    }
                    if i == start {
                        return Err("err empty key in path".to_string());
                    }
                    parts.push(Part::Key(s[start..i].to_string()));
                }
            }
            b'[' => {
                i += 1;
                if i < b.len() && b[i] == b'*' {
                    parts.push(Part::Wildcard);
                    i += 1;
                    if i >= b.len() || b[i] != b']' {
                        return Err("err invalid path".to_string());
                    }
                    i += 1;
                } else if i < b.len() && b[i] == b'"' {
                    i += 1;
                    let start = i;
                    while i < b.len() && b[i] != b'"' {
                        if b[i] == b'\\' {
                            i += 1;
                        }
                        i += 1;
                    }
                    if i >= b.len() {
                        return Err("err unclosed string in path".to_string());
                    }
                    parts.push(Part::Key(s[start..i].to_string()));
                    i += 1;
                    if i < b.len() && b[i] == b']' {
                        i += 1;
                    } else {
                        return Err("err expected ] after string key".to_string());
                    }
                } else {
                    let start = i;
                    while i < b.len() && b[i] != b']' {
                        i += 1;
                    }
                    if i >= b.len() {
                        return Err("err unclosed bracket in path".to_string());
                    }
                    let idx_str = &s[start..i];
                    if idx_str.is_empty() {
                        return Err("err empty brackets in path".to_string());
                    }
                    let idx: i64 = idx_str
                        .parse()
                        .map_err(|_| format!("err invalid array index: {}", idx_str))?;
                    parts.push(Part::Index(idx));
                    i += 1;
                }
            }
            b'*' => {
                parts.push(Part::Wildcard);
                i += 1;
            }
            _ => return Err(format!("err unexpected character '{}' in path", ch as char)),
        }
    }

    Ok(parts)
}

/// Recursively resolves `parts` starting at `depth` against `data`, pushing
/// every match into `results`. Mirrors Go's `resolveRecursive`.
fn resolve_recursive(
    data: &JValue,
    parts: &[Part],
    depth: usize,
    results: &mut Vec<JValue>,
) -> Result<(), String> {
    if depth >= parts.len() {
        results.push(data.clone());
        return Ok(());
    }

    match &parts[depth] {
        Part::Root => resolve_recursive(data, parts, depth + 1, results),
        Part::Recursive => {
            if depth + 1 >= parts.len() {
                results.push(data.clone());
            } else {
                resolve_recursive(data, parts, depth + 1, results)?;
            }
            match data {
                JValue::Obj(m) => {
                    for child in m.values() {
                        resolve_recursive(child, parts, depth, results)?;
                    }
                }
                JValue::Arr(a) => {
                    for child in a {
                        resolve_recursive(child, parts, depth, results)?;
                    }
                }
                _ => {}
            }
            Ok(())
        }
        Part::Wildcard => match data {
            JValue::Obj(m) => {
                for child in m.values() {
                    resolve_recursive(child, parts, depth + 1, results)?;
                }
                Ok(())
            }
            JValue::Arr(a) => {
                for child in a {
                    resolve_recursive(child, parts, depth + 1, results)?;
                }
                Ok(())
            }
            _ => Err("err cannot wildcard on scalar value".to_string()),
        },
        Part::Key(k) => match data {
            JValue::Obj(m) => match m.get(k) {
                Some(v) => resolve_recursive(v, parts, depth + 1, results),
                None => Err("err path does not exist".to_string()),
            },
            _ => Err("err path does not exist".to_string()),
        },
        Part::Index(idx) => match data {
            JValue::Arr(a) => {
                let mut i = *idx;
                if i < 0 {
                    i = a.len() as i64 + i;
                }
                if i < 0 || i as usize >= a.len() {
                    return Err("err index out of range".to_string());
                }
                resolve_recursive(&a[i as usize], parts, depth + 1, results)
            }
            _ => Err("err not an array".to_string()),
        },
    }
}

/// Resolves every match of `parts` against `data`, mirroring Go's
/// `resolveValue`.
fn resolve_value(data: &JValue, parts: &[Part]) -> Result<Vec<JValue>, String> {
    let mut results = Vec::new();
    resolve_recursive(data, parts, 0, &mut results)?;
    Ok(results)
}

/// Resolves `parts` and returns its single match, mirroring Go's
/// `resolveSingle`.
fn resolve_single(data: &JValue, parts: &[Part]) -> Result<JValue, String> {
    let results = resolve_value(data, parts)?;
    if results.len() != 1 {
        return Err("err ambiguous path".to_string());
    }
    Ok(results.into_iter().next().expect("len == 1"))
}

/// Navigates `data` (which must contain at least two parts) so that the
/// container named by the final part is returned, creating intermediate
/// objects on the way. Mirrors Go's `ensureParent`.
fn ensure_parent<'a>(data: &'a mut JValue, parts: &[Part]) -> Result<&'a mut JValue, String> {
    let last = parts.len() - 1;
    let mut current = data;
    for part in &parts[1..last] {
        match part {
            Part::Key(k) => match current {
                JValue::Obj(m) => {
                    if !m.contains_key(k) {
                        m.insert(k.clone(), JValue::Obj(BTreeMap::new()));
                    }
                    current = m
                        .get_mut(k)
                        .expect("inserted or present key is always aliased");
                }
                _ => return Err("err existing key has wrong type".to_string()),
            },
            Part::Index(idx) => match current {
                JValue::Arr(a) => {
                    let mut i = *idx;
                    if i < 0 {
                        i = a.len() as i64 + i;
                    }
                    if i < 0 || i as usize >= a.len() {
                        return Err("err index out of range".to_string());
                    }
                    current = &mut a[i as usize];
                }
                _ => return Err("err not an array".to_string()),
            },
            Part::Wildcard | Part::Recursive | Part::Root => {
                return Err("err wildcard/recursive paths not supported for set".to_string());
            }
        }
    }
    Ok(current)
}

// --- document -------------------------------------------------------------

/// An immutable-ish JSON document living at one public key. Mutating methods
/// operate in place on `root`; the stored bytes are only rewritten by the
/// calling op after the whole transaction commits.
struct JsonDocument {
    root: JValue,
}

impl JsonDocument {
    /// A fresh empty document whose root is `{}`.
    fn empty() -> Self {
        Self {
            root: JValue::Obj(BTreeMap::new()),
        }
    }

    /// Parses raw stored bytes. Corrupt data is a hard error (unreachable
    /// through the wire path, which validates before writing).
    fn from_bytes(raw: &[u8]) -> Result<Self, String> {
        let v: serde_json::Value =
            serde_json::from_slice(raw).map_err(|e| format!("invalid JSON document: {e}"))?;
        Ok(Self {
            root: from_serde(v),
        })
    }

    /// Serializes the document with Go-compatible formatting.
    fn serialize(&self) -> Vec<u8> {
        serialize_value(&self.root).into_bytes()
    }

    /// `get(path)` — a single-match path resolves to its value; a path with
    /// no matches errors; one with several returns the matches as an array.
    fn get(&self, path: &str) -> Result<JValue, String> {
        let parts = parse_path(path)?;
        if parts.len() == 1 {
            return Ok(self.root.clone());
        }
        let results = resolve_value(&self.root, &parts)?;
        if results.is_empty() {
            return Err("err path does not exist".to_string());
        }
        if results.len() == 1 {
            return Ok(results.into_iter().next().expect("len == 1"));
        }
        Ok(JValue::Arr(results))
    }

    /// `set(path, value)` — replaces the whole document for a root path;
    /// otherwise assigns into the container at `path`.
    fn set(&mut self, path: &str, value: JValue) -> Result<(), String> {
        let parts = parse_path(path)?;
        if parts.len() == 1 {
            self.root = value;
            return Ok(());
        }
        for p in &parts[1..] {
            if matches!(p, Part::Wildcard | Part::Recursive) {
                return Err("err wildcard/recursive paths not supported for set".to_string());
            }
        }
        let parent = ensure_parent(&mut self.root, &parts)?;
        match parts.last().expect("len > 1") {
            Part::Key(k) => match parent {
                JValue::Obj(m) => {
                    m.insert(k.clone(), value);
                    Ok(())
                }
                _ => Err("err existing key has wrong type".to_string()),
            },
            Part::Index(idx) => match parent {
                JValue::Arr(a) => {
                    let mut i = *idx;
                    if i < 0 {
                        i = a.len() as i64 + i;
                    }
                    if i < 0 || i as usize >= a.len() {
                        return Err("err index out of range".to_string());
                    }
                    a[i as usize] = value;
                    Ok(())
                }
                _ => Err("err not an array".to_string()),
            },
            _ => Err("err unexpected path part".to_string()),
        }
    }

    /// `set` at a pre-parsed part list (used by `delete` of an array index).
    fn set_at_parts(&mut self, parts: &[Part], value: JValue) -> Result<(), String> {
        if parts.len() <= 1 {
            self.root = value;
            return Ok(());
        }
        for p in &parts[1..] {
            if matches!(p, Part::Wildcard | Part::Recursive) {
                return Err("err wildcard/recursive paths not supported for set".to_string());
            }
        }
        let parent = ensure_parent(&mut self.root, parts)?;
        match parts.last().expect("len > 1") {
            Part::Key(k) => match parent {
                JValue::Obj(m) => {
                    m.insert(k.clone(), value);
                    Ok(())
                }
                _ => Err("err existing key has wrong type".to_string()),
            },
            Part::Index(idx) => match parent {
                JValue::Arr(a) => {
                    let mut i = *idx;
                    if i < 0 {
                        i = a.len() as i64 + i;
                    }
                    if i < 0 || i as usize >= a.len() {
                        return Err("err index out of range".to_string());
                    }
                    a[i as usize] = value;
                    Ok(())
                }
                _ => Err("err not an array".to_string()),
            },
            _ => Err("err unexpected path part".to_string()),
        }
    }

    /// `delete(path)` — removes the value at `path`; a root path clears the
    /// document to JSON `null`.
    fn delete(&mut self, path: &str) -> Result<(), String> {
        let parts = parse_path(path)?;
        if parts.len() == 1 {
            self.root = JValue::Null;
            return Ok(());
        }
        for p in &parts[1..] {
            if matches!(p, Part::Wildcard | Part::Recursive) {
                return Err("err wildcard/recursive paths not supported for delete".to_string());
            }
        }

        match parts.last().expect("len > 1") {
            Part::Key(k) => {
                let parent = ensure_parent(&mut self.root, &parts)?;
                match parent {
                    JValue::Obj(m) => {
                        if !m.contains_key(k) {
                            return Err("err path does not exist".to_string());
                        }
                        m.remove(k);
                        Ok(())
                    }
                    _ => Err("err existing key has wrong type".to_string()),
                }
            }
            Part::Index(idx) => {
                let array_parts = &parts[..parts.len() - 1];
                let arr = match resolve_single(&self.root, array_parts)? {
                    JValue::Arr(a) => a,
                    _ => return Err("err not an array".to_string()),
                };
                let mut i = *idx;
                if i < 0 {
                    i = arr.len() as i64 + i;
                }
                if i < 0 || i as usize >= arr.len() {
                    return Err("err index out of range".to_string());
                }
                let mut new_arr = arr;
                new_arr.remove(i as usize);
                self.set_at_parts(array_parts, JValue::Arr(new_arr))
            }
            Part::Root | Part::Wildcard | Part::Recursive => {
                Err("err unexpected path part".to_string())
            }
        }
    }

    fn type_of(&self, path: &str) -> Result<String, String> {
        if path == "$" || path == "." || path.is_empty() {
            return Ok(json_type_name(&self.root).to_string());
        }
        let val = self.get(path)?;
        Ok(json_type_name(&val).to_string())
    }

    fn arr_append(&mut self, path: &str, values: Vec<JValue>) -> Result<usize, String> {
        let parts = parse_path(path)?;
        if parts.len() == 1 {
            return Err("err cannot append to root document".to_string());
        }
        for p in &parts[1..] {
            if matches!(p, Part::Wildcard | Part::Recursive) {
                return Err("err wildcard/recursive paths not supported".to_string());
            }
        }
        let parent = ensure_parent(&mut self.root, &parts)?;
        match parts.last().expect("len > 1") {
            Part::Key(k) => match parent {
                JValue::Obj(m) => {
                    let existing = m.get_mut(k);
                    let mut arr = match existing {
                        Some(JValue::Arr(a)) => std::mem::take(a),
                        Some(_) => return Err("err existing key has wrong type".to_string()),
                        None => Vec::new(),
                    };
                    arr.extend(values.clone());
                    let len = arr.len();
                    m.insert(k.clone(), JValue::Arr(arr));
                    Ok(len)
                }
                _ => Err("err existing key has wrong type".to_string()),
            },
            Part::Index(idx) => match parent {
                JValue::Arr(a) => {
                    let mut i = *idx;
                    if i < 0 {
                        i = a.len() as i64 + i;
                    }
                    if i < 0 || i as usize >= a.len() {
                        return Err("err index out of range".to_string());
                    }
                    match &mut a[i as usize] {
                        JValue::Arr(inner) => {
                            inner.extend(values.clone());
                            Ok(inner.len())
                        }
                        _ => Err("err existing key has wrong type".to_string()),
                    }
                }
                _ => Err("err not an array".to_string()),
            },
            _ => Err("err unexpected path part".to_string()),
        }
    }

    fn arr_index(&self, path: &str, value: &JValue) -> Result<i64, String> {
        let val = self.get(path)?;
        let arr = match val {
            JValue::Arr(a) => a,
            _ => return Err("err not an array".to_string()),
        };
        let needle = serialize_value(value);
        for (i, elem) in arr.iter().enumerate() {
            if serialize_value(elem) == needle {
                return Ok(i as i64);
            }
        }
        Ok(-1)
    }

    fn arr_len(&self, path: &str) -> Result<usize, String> {
        if path == "$" || path == "." || path.is_empty() {
            return match &self.root {
                JValue::Arr(a) => Ok(a.len()),
                _ => Err("err not an array".to_string()),
            };
        }
        let val = self.get(path)?;
        match val {
            JValue::Arr(a) => Ok(a.len()),
            _ => Err("err not an array".to_string()),
        }
    }

    fn num_incr_by(&mut self, path: &str, delta: f64) -> Result<f64, String> {
        let parts = parse_path(path)?;
        if parts.len() == 1 {
            return Err("err cannot operate on root document".to_string());
        }
        let current = match self.get(path) {
            Err(_) => 0.0,
            Ok(JValue::Number(n)) => n,
            Ok(_) => return Err("err existing key has wrong type".to_string()),
        };
        let new_val = current + delta;
        self.set(path, JValue::Number(new_val))?;
        Ok(new_val)
    }

    fn num_mult_by(&mut self, path: &str, factor: f64) -> Result<f64, String> {
        let current = match self.get(path) {
            Err(e) => return Err(e),
            Ok(JValue::Number(n)) => n,
            Ok(_) => return Err("err existing key has wrong type".to_string()),
        };
        let new_val = current * factor;
        self.set(path, JValue::Number(new_val))?;
        Ok(new_val)
    }

    fn obj_keys(&self, path: &str) -> Result<Vec<String>, String> {
        let val = self.get(path)?;
        match val {
            JValue::Obj(m) => {
                let mut keys: Vec<String> = m.keys().cloned().collect();
                keys.sort();
                Ok(keys)
            }
            _ => Err("err not an object".to_string()),
        }
    }

    fn obj_len(&self, path: &str) -> Result<usize, String> {
        let val = self.get(path)?;
        match val {
            JValue::Obj(m) => Ok(m.len()),
            _ => Err("err not an object".to_string()),
        }
    }

    fn str_append(&mut self, path: &str, suffix: &str) -> Result<usize, String> {
        let val = self.get(path)?;
        let new_str = match val {
            JValue::Str(s) => s + suffix,
            _ => return Err("err existing key has wrong type".to_string()),
        };
        let len = new_str.len();
        self.set(path, JValue::Str(new_str))?;
        Ok(len)
    }

    fn str_len(&self, path: &str) -> Result<usize, String> {
        let val = self.get(path)?;
        match val {
            JValue::Str(s) => Ok(s.len()),
            _ => Err("err existing key has wrong type".to_string()),
        }
    }

    /// Replaces the array at `path` in place (root or nested), mirroring Go's
    /// `setArrayResult`.
    fn set_array_result(&mut self, path: &str, arr: JValue) -> Result<(), String> {
        let parts = parse_path(path)?;
        if parts.len() == 1 {
            self.root = arr;
            return Ok(());
        }
        let parent = ensure_parent(&mut self.root, &parts)?;
        match parts.last().expect("len > 1") {
            Part::Key(k) => match parent {
                JValue::Obj(m) => {
                    m.insert(k.clone(), arr);
                    Ok(())
                }
                _ => Err("err existing key has wrong type".to_string()),
            },
            Part::Index(idx) => match parent {
                JValue::Arr(a) => {
                    let mut i = *idx;
                    if i < 0 {
                        i = a.len() as i64 + i;
                    }
                    if i < 0 || i as usize >= a.len() {
                        return Err("err index out of range".to_string());
                    }
                    a[i as usize] = arr;
                    Ok(())
                }
                _ => Err("err not an array".to_string()),
            },
            _ => Err("err unexpected path part".to_string()),
        }
    }
}

// --- FPHA -----------------------------------------------------------------

/// The floating-point precision requested by the `JSON.SET` `FPHA` option.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum FphaType {
    None,
    Fp16,
    Bf16,
    Fp32,
    Fp64,
}

/// Parses an `FPHA` flag value; `None` when unsupported (the caller replies
/// `ERR syntax error`).
pub fn parse_fpha(s: &[u8]) -> Option<FphaType> {
    let up = s.to_ascii_uppercase();
    match up.as_slice() {
        b"FP16" => Some(FphaType::Fp16),
        b"BF16" => Some(FphaType::Bf16),
        b"FP32" => Some(FphaType::Fp32),
        b"FP64" => Some(FphaType::Fp64),
        _ => None,
    }
}

/// Checks that every float in `v` is representable under `ft`, returning the
/// bare error message the listener echoes verbatim (`value out of range`).
pub(crate) fn validate_fpha(v: &JValue, ft: FphaType) -> Result<(), String> {
    match v {
        JValue::Number(n) => validate_fpha_float(*n, ft),
        JValue::Arr(a) => {
            for item in a {
                validate_fpha(item, ft)?;
            }
            Ok(())
        }
        JValue::Obj(m) => {
            for item in m.values() {
                validate_fpha(item, ft)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_fpha_float(val: f64, ft: FphaType) -> Result<(), String> {
    let abs = val.abs();
    match ft {
        FphaType::Fp64 => Ok(()),
        FphaType::Fp32 | FphaType::Bf16 => {
            // math.SmallestNonzeroFloat32 / math.MaxFloat32.
            if val != 0.0 && abs < 1.401298464324817e-45 {
                return Err("value out of range".to_string());
            }
            if abs > 3.4028234663852886e+38 {
                return Err("value out of range".to_string());
            }
            Ok(())
        }
        FphaType::Fp16 => {
            const FP16_MAX: f64 = 65504.0;
            const FP16_MIN: f64 = 6.1035e-5;
            if val != 0.0 && abs < FP16_MIN {
                return Err("value out of range".to_string());
            }
            if abs > FP16_MAX {
                return Err("value out of range".to_string());
            }
            Ok(())
        }
        FphaType::None => Ok(()),
    }
}

// --- op plumbing ----------------------------------------------------------

/// Result of a JSON `DbOp`, boxed as the opaque [`DbResult`] and rendered by
/// [`JsonWire`].
enum JsonResult {
    /// `+OK` (a successful `JSON.SET`).
    Ok,
    /// RESP null (`$-1`), used for documented null replies.
    Null,
    /// An integer reply.
    Int(i64),
    /// A bulk string reply using `common.FormatFloat` formatting.
    Float(f64),
    /// A bulk string reply with a literal string (e.g. a JSON type name).
    Str(String),
    /// A serialized JSON value (or RESP null for a JSON `null`).
    Json(JValue),
    /// An array of bulk strings.
    KeyList(Vec<String>),
    /// An array where each element is a serialized JSON value or null.
    JsonList(Vec<Option<JValue>>),
    /// A value rendered via the `JSON.RESP` conversion rules.
    Resp(JValue),
}

/// Shared wire half for every JSON command.
struct JsonWire;

impl WireOp for JsonWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Err(e) => err_resp(&e),
            Ok(res) => match res.downcast::<JsonResult>() {
                Ok(outcome) => render_json_result(outcome),
                Err(_) => RespValue::Error(Bytes::from_static(
                    b"ERR internal error: bad json result",
                )),
            },
        }
    }
}

fn render_json_result(outcome: Box<JsonResult>) -> RespValue {
    match *outcome {
        JsonResult::Ok => RespValue::SimpleString(Bytes::from_static(b"OK")),
        JsonResult::Null => RespValue::BulkString(None),
        JsonResult::Int(n) => RespValue::Integer(n),
        JsonResult::Float(f) => RespValue::BulkString(Some(Bytes::from(format_float(f)))),
        JsonResult::Str(s) => RespValue::BulkString(Some(Bytes::from(s))),
        JsonResult::Json(v) => match v {
            JValue::Null => RespValue::BulkString(None),
            v => RespValue::BulkString(Some(Bytes::from(serialize_value(&v)))),
        },
        JsonResult::KeyList(keys) => RespValue::Array(Some(
            keys.into_iter()
                .map(|k| RespValue::BulkString(Some(Bytes::from(k))))
                .collect(),
        )),
        JsonResult::JsonList(vals) => RespValue::Array(Some(
            vals.into_iter()
                .map(|opt| match opt {
                    Some(JValue::Null) | None => RespValue::BulkString(None),
                    Some(v) => RespValue::BulkString(Some(Bytes::from(serialize_value(&v)))),
                })
                .collect(),
        )),
        JsonResult::Resp(v) => to_resp_value(&v),
    }
}

fn boxed(r: JsonResult) -> DbResult {
    Box::new(r)
}

/// Converts a JSON value to [`RespValue`] the way Go's `writeRESPValue` does:
/// booleans become 1/0 integers, numbers bulk strings, objects flat arrays of
/// key/value pairs, and `null` a RESP null.
fn to_resp_value(v: &JValue) -> RespValue {
    match v {
        JValue::Null => RespValue::BulkString(None),
        JValue::Bool(b) => RespValue::Integer(if *b { 1 } else { 0 }),
        JValue::Number(f) => RespValue::BulkString(Some(Bytes::from(format_float(*f)))),
        JValue::Str(s) => RespValue::BulkString(Some(Bytes::from(s.clone()))),
        JValue::Arr(items) => RespValue::Array(Some(items.iter().map(to_resp_value).collect())),
        JValue::Obj(m) => {
            let mut out = Vec::with_capacity(m.len() * 2);
            for (k, val) in m {
                out.push(RespValue::BulkString(Some(Bytes::from(k.clone()))));
                out.push(to_resp_value(val));
            }
            RespValue::Array(Some(out))
        }
    }
}

/// Loads the JSON document at a public key, mapping a missing key to
/// `Loaded::Missing` and a key holding another value type to
/// `Loaded::WrongType`.
enum Loaded {
    Missing,
    WrongType,
    Doc(JsonDocument),
}

async fn load_doc(tx: &dyn Tx, key: &[u8]) -> Result<Loaded, DbError> {
    let item = match tx.get(key).await {
        Ok(item) => item,
        Err(KvError::KeyNotFound) => return Ok(Loaded::Missing),
        Err(e) => return Err(e.into()),
    };
    if item.metadata() != TYPE_JSON {
        return Ok(Loaded::WrongType);
    }
    let doc = JsonDocument::from_bytes(item.value()).map_err(DbError::Redis)?;
    Ok(Loaded::Doc(doc))
}

/// Applies a mutating operation and writes the serialized document back.
async fn write_doc(
    tx: &dyn Tx,
    key: Vec<u8>,
    mut doc: JsonDocument,
    f: impl FnOnce(&mut JsonDocument) -> Result<(), String>,
) -> Result<(), DbError> {
    f(&mut doc).map_err(DbError::Redis)?;
    tx.set(Entry::new(key, doc.serialize()).metadata(TYPE_JSON))?;
    Ok(())
}

// --- ops ------------------------------------------------------------------

/// `JSON.SET key path value [NX|XX] [FPHA type]`.
pub(crate) fn set(
    session: &Session,
    key: &[u8],
    path: &[u8],
    value: JValue,
    nx: bool,
    xx: bool,
) -> QueuedOp {
    let key = session.public_key(key);
    let path = String::from_utf8_lossy(path).into_owned();
    QueuedOp {
        db_op: Box::new(SetOp { key, path, value, nx, xx }),
        wire_op: Box::new(JsonWire),
        is_mutating: true,
        allowed_in_tx: true,
    }
}

struct SetOp {
    key: Vec<u8>,
    path: String,
    value: JValue,
    nx: bool,
    xx: bool,
}

impl DbOp for SetOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let path = self.path.clone();
        let value = self.value.clone();
        let nx = self.nx;
        let xx = self.xx;
        Box::pin(async move {
            let loaded = load_doc(tx, key.as_slice()).await?;
            let key_exists = matches!(loaded, Loaded::Doc(_));
            let mut doc = match loaded {
                Loaded::Missing => JsonDocument::empty(),
                Loaded::Doc(doc) => doc,
                Loaded::WrongType => return Err(DbError::WrongType),
            };

            if path == "$" || path == "." {
                if nx && key_exists {
                    return Ok(boxed(JsonResult::Null));
                }
                if xx && !key_exists {
                    return Ok(boxed(JsonResult::Null));
                }
                doc.root = value;
            } else {
                if nx && doc.get(&path).is_ok() {
                    return Ok(boxed(JsonResult::Null));
                }
                if xx && doc.get(&path).is_err() {
                    return Ok(boxed(JsonResult::Null));
                }
                doc.set(&path, value).map_err(DbError::Redis)?;
            }

            tx.set(Entry::new(key, doc.serialize()).metadata(TYPE_JSON))?;
            Ok(boxed(JsonResult::Ok))
        })
    }
}

/// `JSON.GET key [path ...]` — the whole document, the value at a single
/// path, or an object of path→value for several paths.
pub fn get(session: &Session, key: &[u8], paths: Vec<String>) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(GetOp {
            key: session.public_key(key),
            paths,
        }),
        wire_op: Box::new(JsonWire),
        is_mutating: false,
        allowed_in_tx: true,
    }
}

struct GetOp {
    key: Vec<u8>,
    paths: Vec<String>,
}

impl DbOp for GetOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let paths = self.paths.clone();
        Box::pin(async move {
            let doc = match load_doc(tx, key.as_slice()).await? {
                Loaded::Missing => return Ok(boxed(JsonResult::Null)),
                Loaded::WrongType => return Err(DbError::WrongType),
                Loaded::Doc(doc) => doc,
            };

            if paths.is_empty() {
                return Ok(boxed(JsonResult::Json(doc.root.clone())));
            }
            if paths.len() == 1 {
                return match doc.get(&paths[0]) {
                    Ok(val) => Ok(boxed(JsonResult::Json(val))),
                    Err(_) => Ok(boxed(JsonResult::Null)),
                };
            }

            let mut result = BTreeMap::new();
            for p in &paths {
                result.insert(
                    p.clone(),
                    doc.get(p).unwrap_or(JValue::Null),
                );
            }
            Ok(boxed(JsonResult::Json(JValue::Obj(result))))
        })
    }
}

/// `JSON.DEL key [path ...]`.
pub fn del(session: &Session, key: &[u8], paths: Vec<String>) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(DelOp {
            key: session.public_key(key),
            paths,
        }),
        wire_op: Box::new(JsonWire),
        is_mutating: true,
        allowed_in_tx: true,
    }
}

struct DelOp {
    key: Vec<u8>,
    paths: Vec<String>,
}

impl DbOp for DelOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let paths = self.paths.clone();
        Box::pin(async move {
            if paths.is_empty() {
                // Delete the whole key; reports 1 even when missing.
                match tx.get(&key).await {
                    Ok(item) => {
                        if item.metadata() != TYPE_JSON {
                            return Err(DbError::WrongType);
                        }
                        tx.delete(&key)?;
                        return Ok(boxed(JsonResult::Int(1)));
                    }
                    Err(KvError::KeyNotFound) => return Ok(boxed(JsonResult::Int(1))),
                    Err(e) => return Err(e.into()),
                }
            }

            let mut doc = match load_doc(tx, key.as_slice()).await? {
                Loaded::Missing => return Ok(boxed(JsonResult::Int(0))),
                Loaded::WrongType => return Err(DbError::WrongType),
                Loaded::Doc(doc) => doc,
            };

            let mut deleted = 0usize;
            for path in &paths {
                if doc.get(&path).is_err() {
                    continue;
                }
                if doc.delete(path).is_ok() {
                    deleted += 1;
                }
            }
            if deleted > 0 {
                write_doc(tx, key, doc, |_| Ok(())).await?;
            }
            Ok(boxed(JsonResult::Int(deleted as i64)))
        })
    }
}

/// `JSON.TYPE key [path]`.
pub fn json_type(session: &Session, key: &[u8], path: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(TypeOp {
            key: session.public_key(key),
            path: String::from_utf8_lossy(path).into_owned(),
        }),
        wire_op: Box::new(JsonWire),
        is_mutating: false,
        allowed_in_tx: true,
    }
}

struct TypeOp {
    key: Vec<u8>,
    path: String,
}

impl DbOp for TypeOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let path = self.path.clone();
        Box::pin(async move {
            let doc = match load_doc(tx, key.as_slice()).await? {
                Loaded::Missing => return Ok(boxed(JsonResult::Null)),
                Loaded::WrongType => return Err(DbError::WrongType),
                Loaded::Doc(doc) => doc,
            };
            match doc.type_of(&path) {
                Ok(name) => Ok(boxed(JsonResult::Str(name))),
                Err(_) => Ok(boxed(JsonResult::Null)),
            }
        })
    }
}

/// `JSON.ARRAPPEND key path value [value ...]`.
pub(crate) fn arr_append(session: &Session, key: &[u8], path: &[u8], values: Vec<JValue>) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(ArrAppendOp {
            key: session.public_key(key),
            path: String::from_utf8_lossy(path).into_owned(),
            values,
        }),
        wire_op: Box::new(JsonWire),
        is_mutating: true,
        allowed_in_tx: true,
    }
}

struct ArrAppendOp {
    key: Vec<u8>,
    path: String,
    values: Vec<JValue>,
}

impl DbOp for ArrAppendOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let path = self.path.clone();
        let values = self.values.clone();
        Box::pin(async move {
            let doc = match load_doc(tx, key.as_slice()).await? {
                Loaded::Missing => return Ok(boxed(JsonResult::Null)),
                Loaded::WrongType => return Err(DbError::WrongType),
                Loaded::Doc(doc) => doc,
            };
            let mut doc = doc;
            let len = doc.arr_append(&path, values).map_err(DbError::Redis)?;
            write_doc(tx, key, doc, |_| Ok(())).await?;
            Ok(boxed(JsonResult::Int(len as i64)))
        })
    }
}

/// `JSON.ARRINDEX key path value`.
pub(crate) fn arr_index(session: &Session, key: &[u8], path: &[u8], value: JValue) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(ArrIndexOp {
            key: session.public_key(key),
            path: String::from_utf8_lossy(path).into_owned(),
            value,
        }),
        wire_op: Box::new(JsonWire),
        is_mutating: false,
        allowed_in_tx: true,
    }
}

struct ArrIndexOp {
    key: Vec<u8>,
    path: String,
    value: JValue,
}

impl DbOp for ArrIndexOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let path = self.path.clone();
        let value = self.value.clone();
        Box::pin(async move {
            let doc = match load_doc(tx, key.as_slice()).await? {
                Loaded::Missing => return Ok(boxed(JsonResult::Null)),
                Loaded::WrongType => return Err(DbError::WrongType),
                Loaded::Doc(doc) => doc,
            };
            let idx = doc.arr_index(&path, &value).unwrap_or(-1);
            Ok(boxed(JsonResult::Int(idx)))
        })
    }
}

/// `JSON.ARRLEN key [path]`.
pub fn arr_len(session: &Session, key: &[u8], path: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(ArrLenOp {
            key: session.public_key(key),
            path: String::from_utf8_lossy(path).into_owned(),
        }),
        wire_op: Box::new(JsonWire),
        is_mutating: false,
        allowed_in_tx: true,
    }
}

struct ArrLenOp {
    key: Vec<u8>,
    path: String,
}

impl DbOp for ArrLenOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let path = self.path.clone();
        Box::pin(async move {
            let doc = match load_doc(tx, key.as_slice()).await? {
                Loaded::Missing => return Ok(boxed(JsonResult::Null)),
                Loaded::WrongType => return Err(DbError::WrongType),
                Loaded::Doc(doc) => doc,
            };
            let len = match doc.arr_len(&path) {
                Ok(len) => len,
                Err(_) => return Ok(boxed(JsonResult::Null)),
            };
            Ok(boxed(JsonResult::Int(len as i64)))
        })
    }
}

/// `JSON.NUMINCRBY key path number`.
pub fn num_incr_by(session: &Session, key: &[u8], path: &[u8], delta: f64) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(NumIncrByOp {
            key: session.public_key(key),
            path: String::from_utf8_lossy(path).into_owned(),
            delta,
        }),
        wire_op: Box::new(JsonWire),
        is_mutating: true,
        allowed_in_tx: true,
    }
}

struct NumIncrByOp {
    key: Vec<u8>,
    path: String,
    delta: f64,
}

impl DbOp for NumIncrByOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let path = self.path.clone();
        let delta = self.delta;
        Box::pin(async move {
            let doc = match load_doc(tx, key.as_slice()).await? {
                Loaded::Missing => return Ok(boxed(JsonResult::Null)),
                Loaded::WrongType => return Err(DbError::WrongType),
                Loaded::Doc(doc) => doc,
            };
            let mut doc = doc;
            let new_val = doc.num_incr_by(&path, delta).map_err(DbError::Redis)?;
            write_doc(tx, key, doc, |_| Ok(())).await?;
            Ok(boxed(JsonResult::Float(new_val)))
        })
    }
}

/// `JSON.NUMMULTBY key path number`.
pub fn num_mult_by(session: &Session, key: &[u8], path: &[u8], factor: f64) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(NumMultByOp {
            key: session.public_key(key),
            path: String::from_utf8_lossy(path).into_owned(),
            factor,
        }),
        wire_op: Box::new(JsonWire),
        is_mutating: true,
        allowed_in_tx: true,
    }
}

struct NumMultByOp {
    key: Vec<u8>,
    path: String,
    factor: f64,
}

impl DbOp for NumMultByOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let path = self.path.clone();
        let factor = self.factor;
        Box::pin(async move {
            let doc = match load_doc(tx, key.as_slice()).await? {
                Loaded::Missing => return Ok(boxed(JsonResult::Null)),
                Loaded::WrongType => return Err(DbError::WrongType),
                Loaded::Doc(doc) => doc,
            };
            let mut doc = doc;
            let new_val = doc.num_mult_by(&path, factor).map_err(DbError::Redis)?;
            write_doc(tx, key, doc, |_| Ok(())).await?;
            Ok(boxed(JsonResult::Float(new_val)))
        })
    }
}

/// `JSON.OBJKEYS key [path]`.
pub fn obj_keys(session: &Session, key: &[u8], path: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(ObjKeysOp {
            key: session.public_key(key),
            path: String::from_utf8_lossy(path).into_owned(),
        }),
        wire_op: Box::new(JsonWire),
        is_mutating: false,
        allowed_in_tx: true,
    }
}

struct ObjKeysOp {
    key: Vec<u8>,
    path: String,
}

impl DbOp for ObjKeysOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let path = self.path.clone();
        Box::pin(async move {
            let doc = match load_doc(tx, key.as_slice()).await? {
                Loaded::Missing => return Ok(boxed(JsonResult::Null)),
                Loaded::WrongType => return Err(DbError::WrongType),
                Loaded::Doc(doc) => doc,
            };
            match doc.obj_keys(&path) {
                Ok(keys) => Ok(boxed(JsonResult::KeyList(keys))),
                Err(_) => Ok(boxed(JsonResult::Null)),
            }
        })
    }
}

/// `JSON.OBJLEN key [path]`.
pub fn obj_len(session: &Session, key: &[u8], path: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(ObjLenOp {
            key: session.public_key(key),
            path: String::from_utf8_lossy(path).into_owned(),
        }),
        wire_op: Box::new(JsonWire),
        is_mutating: false,
        allowed_in_tx: true,
    }
}

struct ObjLenOp {
    key: Vec<u8>,
    path: String,
}

impl DbOp for ObjLenOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let path = self.path.clone();
        Box::pin(async move {
            let doc = match load_doc(tx, key.as_slice()).await? {
                Loaded::Missing => return Ok(boxed(JsonResult::Null)),
                Loaded::WrongType => return Err(DbError::WrongType),
                Loaded::Doc(doc) => doc,
            };
            let len = match doc.obj_len(&path) {
                Ok(len) => len,
                Err(_) => return Ok(boxed(JsonResult::Null)),
            };
            Ok(boxed(JsonResult::Int(len as i64)))
        })
    }
}

/// `JSON.STRAPPEND key [path] value`.
pub fn str_append(session: &Session, key: &[u8], path: &[u8], suffix: String) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(StrAppendOp {
            key: session.public_key(key),
            path: String::from_utf8_lossy(path).into_owned(),
            suffix,
        }),
        wire_op: Box::new(JsonWire),
        is_mutating: true,
        allowed_in_tx: true,
    }
}

struct StrAppendOp {
    key: Vec<u8>,
    path: String,
    suffix: String,
}

impl DbOp for StrAppendOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let path = self.path.clone();
        let suffix = self.suffix.clone();
        Box::pin(async move {
            let doc = match load_doc(tx, key.as_slice()).await? {
                Loaded::Missing => return Ok(boxed(JsonResult::Null)),
                Loaded::WrongType => return Err(DbError::WrongType),
                Loaded::Doc(doc) => doc,
            };
            let mut doc = doc;
            let len = doc.str_append(&path, &suffix).map_err(DbError::Redis)?;
            write_doc(tx, key, doc, |_| Ok(())).await?;
            Ok(boxed(JsonResult::Int(len as i64)))
        })
    }
}

/// `JSON.STRLEN key [path]`.
pub fn str_len(session: &Session, key: &[u8], path: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(StrLenOp {
            key: session.public_key(key),
            path: String::from_utf8_lossy(path).into_owned(),
        }),
        wire_op: Box::new(JsonWire),
        is_mutating: false,
        allowed_in_tx: true,
    }
}

struct StrLenOp {
    key: Vec<u8>,
    path: String,
}

impl DbOp for StrLenOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let path = self.path.clone();
        Box::pin(async move {
            let doc = match load_doc(tx, key.as_slice()).await? {
                Loaded::Missing => return Ok(boxed(JsonResult::Null)),
                Loaded::WrongType => return Err(DbError::WrongType),
                Loaded::Doc(doc) => doc,
            };
            let len = match doc.str_len(&path) {
                Ok(len) => len,
                Err(_) => return Ok(boxed(JsonResult::Null)),
            };
            Ok(boxed(JsonResult::Int(len as i64)))
        })
    }
}

/// `JSON.MGET key [key ...] path`.
pub fn mget(session: &Session, keys: Vec<Vec<u8>>, path: String) -> QueuedOp {
    let keys: Vec<Vec<u8>> = keys.into_iter().map(|k| session.public_key(&k)).collect();
    QueuedOp {
        db_op: Box::new(MGetOp { keys, path }),
        wire_op: Box::new(JsonWire),
        is_mutating: false,
        allowed_in_tx: true,
    }
}

struct MGetOp {
    keys: Vec<Vec<u8>>,
    path: String,
}

impl DbOp for MGetOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let keys = self.keys.clone();
        let path = self.path.clone();
        Box::pin(async move {
            let mut results = Vec::with_capacity(keys.len());
            for key in keys {
                match load_doc(tx, key.as_slice()).await {
                    Ok(Loaded::Missing) | Ok(Loaded::WrongType) => {
                        results.push(None);
                    }
                    Ok(Loaded::Doc(doc)) => match doc.get(&path) {
                        Ok(v) => results.push(Some(v)),
                        Err(_) => results.push(None),
                    },
                    Err(e) => return Err(e),
                }
            }
            Ok(boxed(JsonResult::JsonList(results)))
        })
    }
}

/// `JSON.RESP key [path]`.
pub fn resp(session: &Session, key: &[u8], path: String) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(RespOp {
            key: session.public_key(key),
            path,
        }),
        wire_op: Box::new(JsonWire),
        is_mutating: false,
        allowed_in_tx: true,
    }
}

struct RespOp {
    key: Vec<u8>,
    path: String,
}

impl DbOp for RespOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let path = self.path.clone();
        Box::pin(async move {
            let doc = match load_doc(tx, key.as_slice()).await? {
                Loaded::Missing => return Ok(boxed(JsonResult::Null)),
                Loaded::WrongType => return Err(DbError::WrongType),
                Loaded::Doc(doc) => doc,
            };
            let val = if path.is_empty() {
                Ok(doc.root.clone())
            } else {
                doc.get(&path)
            };
            match val {
                Ok(v) => Ok(boxed(JsonResult::Resp(v))),
                Err(_) => Ok(boxed(JsonResult::Null)),
            }
        })
    }
}

/// `JSON.CLEAR key [path]`.
pub fn clear(session: &Session, key: &[u8], path: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(ClearOp {
            key: session.public_key(key),
            path: String::from_utf8_lossy(path).into_owned(),
        }),
        wire_op: Box::new(JsonWire),
        is_mutating: true,
        allowed_in_tx: true,
    }
}

struct ClearOp {
    key: Vec<u8>,
    path: String,
}

impl DbOp for ClearOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let path = self.path.clone();
        Box::pin(async move {
            let doc = match load_doc(tx, key.as_slice()).await? {
                Loaded::Missing => return Ok(boxed(JsonResult::Int(0))),
                Loaded::WrongType => return Err(DbError::WrongType),
                Loaded::Doc(doc) => doc,
            };
            let mut doc = doc;
            let cleared = if path == "$" || path == "." {
                doc.root = JValue::Obj(BTreeMap::new());
                1
            } else {
                let val = match doc.get(&path) {
                    Ok(v) => v,
                    Err(_) => return Ok(boxed(JsonResult::Int(0))),
                };
                match val {
                    JValue::Arr(a) if !a.is_empty() => {
                        doc.set(&path, JValue::Arr(Vec::new())).map_err(DbError::Redis)?;
                        1
                    }
                    JValue::Obj(m) if !m.is_empty() => {
                        doc.set(&path, JValue::Obj(BTreeMap::new())).map_err(DbError::Redis)?;
                        1
                    }
                    JValue::Arr(_) | JValue::Obj(_) => 0,
                    _ => {
                        doc.set(&path, JValue::Obj(BTreeMap::new())).map_err(DbError::Redis)?;
                        1
                    }
                }
            };
            if cleared > 0 {
                write_doc(tx, key, doc, |_| Ok(())).await?;
            }
            Ok(boxed(JsonResult::Int(cleared)))
        })
    }
}

/// `JSON.ARRPOP key [path [index]]`.
pub fn arr_pop(session: &Session, key: &[u8], path: &[u8], idx: i64) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(ArrPopOp {
            key: session.public_key(key),
            path: String::from_utf8_lossy(path).into_owned(),
            idx,
        }),
        wire_op: Box::new(JsonWire),
        is_mutating: true,
        allowed_in_tx: true,
    }
}

struct ArrPopOp {
    key: Vec<u8>,
    path: String,
    idx: i64,
}

impl DbOp for ArrPopOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let path = self.path.clone();
        let idx = self.idx;
        Box::pin(async move {
            let doc = match load_doc(tx, key.as_slice()).await? {
                Loaded::Missing => return Ok(boxed(JsonResult::Null)),
                Loaded::WrongType => return Err(DbError::WrongType),
                Loaded::Doc(doc) => doc,
            };
            let mut doc = doc;
            let val = doc
                .get(&path)
                .map_err(|_| DbError::Redis("err path does not exist".to_string()))?;
            let mut arr = match val {
                JValue::Arr(a) => a,
                _ => return Err(DbError::Redis("err not an array".to_string())),
            };
            if arr.is_empty() {
                return Ok(boxed(JsonResult::Null));
            }
            let mut pop_idx = idx;
            if pop_idx < 0 {
                pop_idx = arr.len() as i64 + pop_idx;
            }
            if pop_idx < 0 || pop_idx >= arr.len() as i64 {
                return Err(DbError::Redis("err index out of range".to_string()));
            }
            let popped = arr.remove(pop_idx as usize);
            doc.set_array_result(&path, JValue::Arr(arr)).map_err(DbError::Redis)?;
            write_doc(tx, key, doc, |_| Ok(())).await?;
            Ok(boxed(JsonResult::Json(popped)))
        })
    }
}

/// `JSON.ARRTRIM key path start stop`.
pub fn arr_trim(session: &Session, key: &[u8], path: &[u8], start: i64, stop: i64) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(ArrTrimOp {
            key: session.public_key(key),
            path: String::from_utf8_lossy(path).into_owned(),
            start,
            stop,
        }),
        wire_op: Box::new(JsonWire),
        is_mutating: true,
        allowed_in_tx: true,
    }
}

struct ArrTrimOp {
    key: Vec<u8>,
    path: String,
    start: i64,
    stop: i64,
}

impl DbOp for ArrTrimOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let path = self.path.clone();
        let start = self.start;
        let stop = self.stop;
        Box::pin(async move {
            let doc = match load_doc(tx, key.as_slice()).await? {
                Loaded::Missing => return Ok(boxed(JsonResult::Null)),
                Loaded::WrongType => return Err(DbError::WrongType),
                Loaded::Doc(doc) => doc,
            };
            let mut doc = doc;
            let val = doc
                .get(&path)
                .map_err(|_| DbError::Redis("err path does not exist".to_string()))?;
            let arr = match val {
                JValue::Arr(a) => a,
                _ => return Err(DbError::Redis("err not an array".to_string())),
            };
            let len = arr.len() as i64;
            let mut s = if start < 0 { len + start } else { start };
            let mut e = if stop < 0 { len + stop } else { stop };
            if s < 0 {
                s = 0;
            }
            if e >= len {
                e = len - 1;
            }
            let new_arr: Vec<JValue> = if s > e || s >= len {
                Vec::new()
            } else {
                arr[s as usize..(e + 1) as usize].to_vec()
            };
            let new_len = new_arr.len();
            doc.set_array_result(&path, JValue::Arr(new_arr)).map_err(DbError::Redis)?;
            write_doc(tx, key, doc, |_| Ok(())).await?;
            Ok(boxed(JsonResult::Int(new_len as i64)))
        })
    }
}

/// `JSON.ARRINSERT key path index value [value ...]`.
pub(crate) fn arr_insert(session: &Session, key: &[u8], path: &[u8], index: i64, values: Vec<JValue>) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(ArrInsertOp {
            key: session.public_key(key),
            path: String::from_utf8_lossy(path).into_owned(),
            index,
            values,
        }),
        wire_op: Box::new(JsonWire),
        is_mutating: true,
        allowed_in_tx: true,
    }
}

struct ArrInsertOp {
    key: Vec<u8>,
    path: String,
    index: i64,
    values: Vec<JValue>,
}

impl DbOp for ArrInsertOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let path = self.path.clone();
        let index = self.index;
        let values = self.values.clone();
        Box::pin(async move {
            let doc = match load_doc(tx, key.as_slice()).await? {
                Loaded::Missing => return Ok(boxed(JsonResult::Null)),
                Loaded::WrongType => return Err(DbError::WrongType),
                Loaded::Doc(doc) => doc,
            };
            let mut doc = doc;
            let val = doc
                .get(&path)
                .map_err(|_| DbError::Redis("err path does not exist".to_string()))?;
            let arr = match val {
                JValue::Arr(a) => a,
                _ => return Err(DbError::Redis("err not an array".to_string())),
            };
            if index < 0 || index > arr.len() as i64 {
                return Err(DbError::Redis("err index out of range".to_string()));
            }
            let mut new_arr = Vec::with_capacity(arr.len() + values.len());
            new_arr.extend_from_slice(&arr[..index as usize]);
            new_arr.extend_from_slice(&values);
            new_arr.extend_from_slice(&arr[index as usize..]);
            let new_len = new_arr.len();
            doc.set_array_result(&path, JValue::Arr(new_arr)).map_err(DbError::Redis)?;
            write_doc(tx, key, doc, |_| Ok(())).await?;
            Ok(boxed(JsonResult::Int(new_len as i64)))
        })
    }
}