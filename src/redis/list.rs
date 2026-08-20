//! Redis list commands: `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`,
//! `LINDEX`, `LSET`, `LREM`, `LTRIM`, `LINSERT`, `LPUSHX` and `RPUSHX`.
//!
//! Port of the Go `redis/list` package. A list is a doubly-linked chain of
//! random-keyed nodes persisted under private keys, fronted by a small
//! sentinel entry under the public key:
//!
//! * The **sentinel** (public key, metadata `ValueType::List`) holds the list
//!   size (4-byte big-endian) plus the 16-byte head and tail node keys — 36
//!   bytes total. See [`read_sentinel`]/[`write_sentinel`].
//! * Each **node** lives under the private key derived from
//!   `-<db>:<name>:<nodeKey>` (built from the captured session prefix by
//!   [`node_key`]) and stores `value || prev(16) || next(16)`, where a
//!   link of all-zero bytes means "no link". A value is never rewritten: only
//!   the pages of the chain that changed are written on persist.
//!
//! Node keys are 16 random bytes ([`random_key`]) so a push never collides
//! with an existing key and existing nodes keep their storage keys. When a
//! list empties out the public sentinel is deleted (mirroring Go, orphaned
//! node entries are left behind).
//!
//! Every command returns a [`QueuedOp`] with a [`DbOp`] half that loads and
//! mutates the list inside the session-managed transaction and a [`WireOp`]
//! half that renders the result to RESP.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use kv::kv::{BoxFuture, Entry, Error as KvError, Tx};

use crate::common::op::{err_resp, DbError, DbOp, DbResult, QueuedOp, WireOp};
use crate::common::session::Session;
use crate::common::ValueType;
use crate::resp::RespValue;

/// Byte length of a node key.
const KEY_LENGTH: usize = 16;
/// Bytes of the sentinel entry: size (4) + head (16) + tail (16).
const SENTINEL_LENGTH: usize = 36;
/// Metadata type byte stamped on every list entry, matching `ValueType::List`.
const TYPE_LIST: u8 = ValueType::List as u8;

/// Anything the wire side of a crashed `DbOp` is allowed to claim if the
/// result shape is unexpected (a "can't happen" guard).
fn internal_error() -> RespValue {
    RespValue::Error(Bytes::from_static(b"ERR internal error"))
}

/// Monotonic counter mixed into [`random_key`] so successive calls in the
/// same nanosecond still differ.
static NODE_KEY_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generates a 16-byte random node key, mirroring Go's `randomKey`. A
/// xorshift sequence seeded from the wall clock and a process-local counter
/// guarantees freshness across calls; all-zero output (which would read as a
/// missing link) is forced nonzero.
fn random_key() -> [u8; KEY_LENGTH] {
    let epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let count = NODE_KEY_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut state = epoch ^ count.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    let mut next = || {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        state.wrapping_mul(0x2545_f491_bf4d_2d2d)
    };
    let mut key = [0u8; KEY_LENGTH];
    key[0..8].copy_from_slice(&next().to_be_bytes());
    key[8..16].copy_from_slice(&next().to_be_bytes());
    if is_zero_key(&key) {
        key[0] = 1;
    }
    key
}

/// Whether every byte of `key` is zero — the on-disk "no link" marker.
fn is_zero_key(key: &[u8]) -> bool {
    key.iter().all(|&b| b == 0)
}

/// Converts a 16-byte link read off disk into an `Option` key, treating
/// all-zero bytes as "no link".
fn to_opt_key(bytes: &[u8]) -> Option<[u8; KEY_LENGTH]> {
    if is_zero_key(bytes) {
        None
    } else {
        let mut key = [0u8; KEY_LENGTH];
        key.copy_from_slice(bytes);
        Some(key)
    }
}

/// A single element of a list chain.
struct ListNode {
    key: [u8; KEY_LENGTH],
    value: Vec<u8>,
    prev: Option<[u8; KEY_LENGTH]>,
    next: Option<[u8; KEY_LENGTH]>,
}

/// A node key plus its resolved prev/next links, produced while loading.
type ResolvedLinks = (
    [u8; KEY_LENGTH],
    Option<[u8; KEY_LENGTH]>,
    Option<[u8; KEY_LENGTH]>,
);

/// A doubly-linked list, the Rust analogue of Go's `linkedList`. Nodes are
/// keyed by their random keys in a map; head/tail keys delimit the chain.
struct Linked {
    size: u32,
    head: Option<[u8; KEY_LENGTH]>,
    tail: Option<[u8; KEY_LENGTH]>,
    nodes: HashMap<[u8; KEY_LENGTH], ListNode>,
}

impl Linked {
    fn new_empty() -> Self {
        Self {
            size: 0,
            head: None,
            tail: None,
            nodes: HashMap::new(),
        }
    }

    /// The node keys in chain order, head to tail.
    fn ordered_keys(&self) -> Vec<[u8; KEY_LENGTH]> {
        let mut keys = Vec::with_capacity(self.size as usize);
        let mut cur = self.head;
        while let Some(k) = cur {
            keys.push(k);
            cur = self.nodes.get(&k).and_then(|n| n.next);
        }
        keys
    }

    /// Prepends `value`, returning the new size.
    fn add_first(&mut self, value: Vec<u8>) -> u32 {
        let key = random_key();
        let next = self.head;
        if let Some(old) = next {
            if let Some(n) = self.nodes.get_mut(&old) {
                n.prev = Some(key);
            }
        } else {
            self.tail = Some(key);
        }
        self.nodes.insert(
            key,
            ListNode {
                key,
                value,
                prev: None,
                next,
            },
        );
        self.head = Some(key);
        self.size += 1;
        self.size
    }

    /// Appends `value`, returning the new size.
    fn add_last(&mut self, value: Vec<u8>) -> u32 {
        let key = random_key();
        let prev = self.tail;
        if let Some(old) = prev {
            if let Some(n) = self.nodes.get_mut(&old) {
                n.next = Some(key);
            }
        } else {
            self.head = Some(key);
        }
        self.nodes.insert(
            key,
            ListNode {
                key,
                value,
                prev,
                next: None,
            },
        );
        self.tail = Some(key);
        self.size += 1;
        self.size
    }

    /// Removes and returns the head element, if any.
    fn remove_first(&mut self) -> Option<Vec<u8>> {
        let head = self.head?;
        let node = self.nodes.remove(&head)?;
        let value = node.value;
        let new_head = node.next;
        self.head = new_head;
        match new_head {
            Some(nh) => {
                if let Some(n) = self.nodes.get_mut(&nh) {
                    n.prev = None;
                }
            }
            None => self.tail = None,
        }
        self.size = self.size.saturating_sub(1);
        Some(value)
    }

    /// Removes and returns the tail element, if any.
    fn remove_last(&mut self) -> Option<Vec<u8>> {
        let tail = self.tail?;
        let node = self.nodes.remove(&tail)?;
        let value = node.value;
        let new_tail = node.prev;
        self.tail = new_tail;
        match new_tail {
            Some(nt) => {
                if let Some(n) = self.nodes.get_mut(&nt) {
                    n.next = None;
                }
            }
            None => self.head = None,
        }
        self.size = self.size.saturating_sub(1);
        Some(value)
    }

    /// Removes matching elements from the head for `count == 0` (all) or
    /// `count > 0` (up to `count`), returning how many were removed.
    fn remove_matching(&mut self, count: i64, value: &[u8]) -> i64 {
        let mut removed = 0i64;
        let mut new_head: Option<[u8; KEY_LENGTH]> = None;
        let mut prev: Option<[u8; KEY_LENGTH]> = None;
        let mut cur = self.head;
        while let Some(k) = cur {
            let next = self.nodes.get(&k).and_then(|n| n.next);
            let is_match = self.nodes.get(&k).is_some_and(|n| n.value == value)
                && (count == 0 || removed < count);
            if is_match {
                if let Some(p) = prev {
                    if let Some(n) = self.nodes.get_mut(&p) {
                        n.next = next;
                    }
                }
                if let Some(nx) = next {
                    if let Some(n) = self.nodes.get_mut(&nx) {
                        n.prev = prev;
                    }
                }
                self.nodes.remove(&k);
                removed += 1;
                self.size = self.size.saturating_sub(1);
            } else {
                if new_head.is_none() {
                    new_head = Some(k);
                }
                prev = Some(k);
            }
            cur = next;
        }
        self.head = new_head;
        self.tail = prev;
        removed
    }

    /// Removes the last `-count` matching elements (scanning from the tail),
    /// returning how many were removed.
    fn remove_matching_from_tail(&mut self, count: i64, value: &[u8]) -> i64 {
        let keys = self.ordered_keys();
        let target = (-count) as usize;
        let mut removed = 0usize;
        for &k in keys.iter().rev() {
            if removed >= target {
                break;
            }
            if self.nodes.get(&k).is_none_or(|n| n.value != value) {
                continue;
            }
            let (prev, next) = {
                let n = self.nodes.get(&k).expect("key present");
                (n.prev, n.next)
            };
            if let Some(p) = prev {
                if let Some(n) = self.nodes.get_mut(&p) {
                    n.next = next;
                }
            }
            if let Some(nx) = next {
                if let Some(n) = self.nodes.get_mut(&nx) {
                    n.prev = prev;
                }
            }
            self.nodes.remove(&k);
            removed += 1;
            self.size = self.size.saturating_sub(1);
        }
        self.head = keys.iter().copied().find(|k| self.nodes.contains_key(k));
        self.tail = keys
            .iter()
            .rev()
            .copied()
            .find(|k| self.nodes.contains_key(k));
        removed as i64
    }

    /// Keeps only the elements at indices `[start, stop]` (inclusive),
    /// relinking the survivors.
    fn trim_to(&mut self, start: usize, stop: usize) {
        let keys = self.ordered_keys();
        let mut new_head: Option<[u8; KEY_LENGTH]> = None;
        let mut new_tail: Option<[u8; KEY_LENGTH]> = None;
        let mut prev: Option<[u8; KEY_LENGTH]> = None;
        for (i, &k) in keys.iter().enumerate() {
            if i < start || i > stop {
                self.nodes.remove(&k);
                continue;
            }
            if let Some(n) = self.nodes.get_mut(&k) {
                n.prev = prev;
                n.next = None;
            }
            if let Some(p) = prev {
                if let Some(n) = self.nodes.get_mut(&p) {
                    n.next = Some(k);
                }
            }
            if new_head.is_none() {
                new_head = Some(k);
            }
            prev = Some(k);
            new_tail = Some(k);
        }
        self.head = new_head;
        self.tail = new_tail;
        self.size = (stop - start + 1) as u32;
    }

    /// Inserts `value` before or after the first node holding `pivot`,
    /// returning whether the pivot was found.
    fn insert(&mut self, before: bool, pivot: &[u8], value: Vec<u8>) -> bool {
        let Some(pivot_key) = self
            .ordered_keys()
            .iter()
            .copied()
            .find(|&k| self.nodes.get(&k).is_some_and(|n| n.value == pivot))
        else {
            return false;
        };
        let (pivot_prev, pivot_next) = {
            let p = self.nodes.get(&pivot_key).expect("pivot present");
            (p.prev, p.next)
        };
        let new_key = random_key();
        self.nodes.insert(
            new_key,
            ListNode {
                key: new_key,
                value,
                prev: if before { pivot_prev } else { Some(pivot_key) },
                next: if before { Some(pivot_key) } else { pivot_next },
            },
        );
        if before {
            if let Some(pp) = pivot_prev {
                if let Some(n) = self.nodes.get_mut(&pp) {
                    n.next = Some(new_key);
                }
            } else {
                self.head = Some(new_key);
            }
            if let Some(p) = self.nodes.get_mut(&pivot_key) {
                p.prev = Some(new_key);
            }
        } else {
            if let Some(pn) = pivot_next {
                if let Some(n) = self.nodes.get_mut(&pn) {
                    n.prev = Some(new_key);
                }
            } else {
                self.tail = Some(new_key);
            }
            if let Some(p) = self.nodes.get_mut(&pivot_key) {
                p.next = Some(new_key);
            }
        }
        self.size += 1;
        true
    }
}

// --- Storage layout ---

/// Builds the private storage key of a node from the captured
/// `-<db>:<name>` prefix and the node key, i.e. `-<db>:<name>:<nodeKey>`.
fn node_key(node_prefix: &[u8], node_key: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(node_prefix.len() + 1 + KEY_LENGTH);
    key.extend_from_slice(node_prefix);
    key.push(b':');
    key.extend_from_slice(node_key);
    key
}

/// Reads the list sentinel (size + head + tail), verifying the entry is a
/// list. A missing key or a value too short to be a sentinel maps to
/// [`KvError::KeyNotFound`]; a key holding any other type is
/// [`DbError::WrongType`].
async fn read_sentinel(
    tx: &dyn Tx,
    public_key: &[u8],
) -> Result<(u32, [u8; KEY_LENGTH], [u8; KEY_LENGTH]), DbError> {
    let item = tx.get(public_key).await?;
    if item.metadata() != TYPE_LIST {
        return Err(DbError::WrongType);
    }
    let val = item.value();
    if val.len() < SENTINEL_LENGTH {
        return Err(DbError::Kv(KvError::KeyNotFound));
    }
    let size = u32::from_be_bytes(val[0..4].try_into().expect("slice in range"));
    let mut head = [0u8; KEY_LENGTH];
    head.copy_from_slice(&val[4..20]);
    let mut tail = [0u8; KEY_LENGTH];
    tail.copy_from_slice(&val[20..36]);
    Ok((size, head, tail))
}

/// Writes the list sentinel entry with list metadata.
fn write_sentinel(
    tx: &dyn Tx,
    public_key: &[u8],
    head: Option<[u8; KEY_LENGTH]>,
    tail: Option<[u8; KEY_LENGTH]>,
    size: u32,
) -> Result<(), DbError> {
    let mut buf = [0u8; SENTINEL_LENGTH];
    buf[0..4].copy_from_slice(&size.to_be_bytes());
    if let Some(h) = head {
        buf[4..20].copy_from_slice(&h);
    }
    if let Some(t) = tail {
        buf[20..36].copy_from_slice(&t);
    }
    tx.set(Entry::new(public_key.to_vec(), buf.to_vec()).metadata(TYPE_LIST))
        .map_err(DbError::from)
}

/// Loads a list from the store, mirroring Go's `loadList`: traverse from the
/// head following next links (guarding against cycles) and link each node's
/// prev/next against the keys actually present.
async fn load_list(tx: &dyn Tx, public_key: &[u8], node_prefix: &[u8]) -> Result<Linked, DbError> {
    let (size, head_key, tail_key) = read_sentinel(tx, public_key).await?;
    let mut ll = Linked::new_empty();
    ll.size = size;

    let mut current = head_key;
    while !is_zero_key(&current) {
        if ll.nodes.contains_key(&current) {
            break; // cycle detection
        }
        let item = tx.get(&node_key(node_prefix, &current)).await?;
        let val = item.value();
        if val.len() < 2 * KEY_LENGTH {
            return Err(DbError::Kv(KvError::KeyNotFound));
        }
        let split = val.len() - 2 * KEY_LENGTH;
        let node = ListNode {
            key: current,
            value: val[..split].to_vec(),
            prev: to_opt_key(&val[split..split + KEY_LENGTH]),
            next: to_opt_key(&val[split + KEY_LENGTH..]),
        };
        let next = node.next;
        ll.nodes.insert(current, node);
        match next {
            Some(nx) => current = nx,
            None => break,
        }
    }

    // Only resolve links that point at nodes we actually hold. Computed in a
    // separate pass because the looks-up and the writes borrow the map
    // disjunctively.
    let mut resolve: Vec<ResolvedLinks> = Vec::with_capacity(ll.nodes.len());
    for (k, node) in &ll.nodes {
        let prev = node.prev.filter(|pk| ll.nodes.contains_key(pk));
        let next = node.next.filter(|nk| ll.nodes.contains_key(nk));
        resolve.push((*k, prev, next));
    }
    for (k, prev, next) in resolve {
        if let Some(node) = ll.nodes.get_mut(&k) {
            node.prev = prev;
            node.next = next;
        }
    }

    ll.head = ll.nodes.contains_key(&head_key).then_some(head_key);
    ll.tail = ll.nodes.contains_key(&tail_key).then_some(tail_key);
    Ok(ll)
}

/// Persists the sentinel and every live node, mirroring Go's `persistList`.
fn persist_list(
    tx: &dyn Tx,
    public_key: &[u8],
    node_prefix: &[u8],
    ll: &Linked,
) -> Result<(), DbError> {
    write_sentinel(tx, public_key, ll.head, ll.tail, ll.size)?;
    for node in ll.nodes.values() {
        let mut buf = Vec::with_capacity(node.value.len() + 2 * KEY_LENGTH);
        buf.extend_from_slice(&node.value);
        buf.extend_from_slice(&node.prev.unwrap_or([0u8; KEY_LENGTH]));
        buf.extend_from_slice(&node.next.unwrap_or([0u8; KEY_LENGTH]));
        tx.set(Entry::new(node_key(node_prefix, &node.key), buf).metadata(TYPE_LIST))?;
    }
    Ok(())
}

// --- Command functions ---

/// `LPUSH key value [value ...]` — prepends values, returning the new length.
pub fn lpush(session: &Session, key: &[u8], values: &[Bytes]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(PushOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
            values: values.iter().map(|b| b.to_vec()).collect(),
            at_tail: false,
            only_if_exists: false,
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `RPUSH key value [value ...]` — appends values, returning the new length.
pub fn rpush(session: &Session, key: &[u8], values: &[Bytes]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(PushOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
            values: values.iter().map(|b| b.to_vec()).collect(),
            at_tail: true,
            only_if_exists: false,
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `LPUSHX key value` — prepends only if the key exists, returning the new
/// length (0 if the key is absent).
pub fn lpushx(session: &Session, key: &[u8], value: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(PushOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
            values: vec![value.to_vec()],
            at_tail: false,
            only_if_exists: true,
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `RPUSHX key value` — appends only if the key exists, returning the new
/// length (0 if the key is absent).
pub fn rpushx(session: &Session, key: &[u8], value: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(PushOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
            values: vec![value.to_vec()],
            at_tail: true,
            only_if_exists: true,
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `LPOP key` — removes and returns the head element, or nil if missing.
pub fn lpop(session: &Session, key: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(PopOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
            from_tail: false,
        }),
        wire_op: Box::new(NullableBulkWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `RPOP key` — removes and returns the tail element, or nil if missing.
pub fn rpop(session: &Session, key: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(PopOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
            from_tail: true,
        }),
        wire_op: Box::new(NullableBulkWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `LLEN key` — returns the list length, 0 if missing.
pub fn llen(session: &Session, key: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(LLenOp {
            public_key: session.public_key(key),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `LRANGE key start stop` — returns the elements in the inclusive range,
/// with negative indices counted from the tail.
pub fn lrange(session: &Session, key: &[u8], start: i64, stop: i64) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(LRangeOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
            start,
            stop,
        }),
        wire_op: Box::new(BulkArrayWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `LINDEX key index` — returns the element at `index` (negative counts from
/// the tail), or nil if out of range or missing.
pub fn lindex(session: &Session, key: &[u8], index: i64) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(LIndexOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
            index,
        }),
        wire_op: Box::new(NullableBulkWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `LSET key index value` — sets the element at `index` (negative counts from
/// the tail).
pub fn lset(session: &Session, key: &[u8], index: i64, value: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(LSetOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
            index,
            value: value.to_vec(),
        }),
        wire_op: Box::new(LSetWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `LREM key count value` — removes matching elements; `count` 0 removes all,
/// positive removes from the head, negative from the tail.
pub fn lrem(session: &Session, key: &[u8], count: i64, value: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(LRemOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
            count,
            value: value.to_vec(),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `LTRIM key start stop` — keeps only the elements in the inclusive range.
pub fn ltrim(session: &Session, key: &[u8], start: i64, stop: i64) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(LTrimOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
            start,
            stop,
        }),
        wire_op: Box::new(OkWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `LINSERT key BEFORE|AFTER pivot value` — inserts relative to the first
/// occurrence of `pivot`, returning the new length or -1 if absent.
pub fn linsert(
    session: &Session,
    key: &[u8],
    before: bool,
    pivot: &[u8],
    value: &[u8],
) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(LInsertOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
            before,
            pivot: pivot.to_vec(),
            value: value.to_vec(),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

// --- DbOp halves ---

struct PushOp {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
    values: Vec<Vec<u8>>,
    at_tail: bool,
    only_if_exists: bool,
}

impl DbOp for PushOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        let node_prefix = self.node_prefix.clone();
        let values = self.values.clone();
        let at_tail = self.at_tail;
        let only_if_exists = self.only_if_exists;
        Box::pin(async move {
            let mut ll = match load_list(tx, &public_key, &node_prefix).await {
                Ok(ll) => ll,
                Err(DbError::Kv(KvError::KeyNotFound)) => {
                    if only_if_exists {
                        let result: DbResult = Box::new(0i64);
                        return Ok(result);
                    }
                    Linked::new_empty()
                }
                Err(e) => return Err(e),
            };
            for value in &values {
                if at_tail {
                    ll.add_last(value.clone());
                } else {
                    ll.add_first(value.clone());
                }
            }
            persist_list(tx, &public_key, &node_prefix, &ll)?;
            let result: DbResult = Box::new(ll.size as i64);
            Ok(result)
        })
    }
}

struct PopOp {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
    from_tail: bool,
}

impl DbOp for PopOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        let node_prefix = self.node_prefix.clone();
        let from_tail = self.from_tail;
        Box::pin(async move {
            let mut ll = match load_list(tx, &public_key, &node_prefix).await {
                Ok(ll) => ll,
                Err(DbError::Kv(KvError::KeyNotFound)) => {
                    let result: DbResult = Box::new(None::<Vec<u8>>);
                    return Ok(result);
                }
                Err(e) => return Err(e),
            };
            let value = if from_tail {
                ll.remove_last()
            } else {
                ll.remove_first()
            };
            if ll.size == 0 {
                tx.delete(&public_key)?;
            } else {
                persist_list(tx, &public_key, &node_prefix, &ll)?;
            }
            let result: DbResult = Box::new(value);
            Ok(result)
        })
    }
}

struct LLenOp {
    public_key: Vec<u8>,
}

impl DbOp for LLenOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        Box::pin(async move {
            match read_sentinel(tx, &public_key).await {
                Ok((size, _, _)) => {
                    let result: DbResult = Box::new(size as i64);
                    Ok(result)
                }
                Err(DbError::Kv(KvError::KeyNotFound)) => {
                    let result: DbResult = Box::new(0i64);
                    Ok(result)
                }
                Err(e) => Err(e),
            }
        })
    }
}

struct LRangeOp {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
    start: i64,
    stop: i64,
}

impl DbOp for LRangeOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        let node_prefix = self.node_prefix.clone();
        let start = self.start;
        let stop = self.stop;
        Box::pin(async move {
            let ll = match load_list(tx, &public_key, &node_prefix).await {
                Ok(ll) => ll,
                Err(DbError::Kv(KvError::KeyNotFound)) => {
                    let empty: Vec<Vec<u8>> = Vec::new();
                    let result: DbResult = Box::new(empty);
                    return Ok(result);
                }
                Err(e) => return Err(e),
            };
            let size = ll.size as i64;
            let mut start = start;
            let mut stop = stop;
            if start < 0 {
                start += size;
            }
            if stop < 0 {
                stop += size;
            }
            if start < 0 {
                start = 0;
            }
            if stop >= size {
                stop = size - 1;
            }
            if start > stop || start >= size {
                let empty: Vec<Vec<u8>> = Vec::new();
                let result: DbResult = Box::new(empty);
                return Ok(result);
            }
            let keys = ll.ordered_keys();
            let mut out = Vec::new();
            for i in start..=stop {
                if let Some(k) = keys.get(i as usize).copied() {
                    if let Some(n) = ll.nodes.get(&k) {
                        out.push(n.value.clone());
                    }
                }
            }
            let result: DbResult = Box::new(out);
            Ok(result)
        })
    }
}

struct LIndexOp {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
    index: i64,
}

impl DbOp for LIndexOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        let node_prefix = self.node_prefix.clone();
        let index = self.index;
        Box::pin(async move {
            let ll = match load_list(tx, &public_key, &node_prefix).await {
                Ok(ll) => ll,
                Err(DbError::Kv(KvError::KeyNotFound)) => {
                    let result: DbResult = Box::new(None::<Vec<u8>>);
                    return Ok(result);
                }
                Err(e) => return Err(e),
            };
            let size = ll.size as i64;
            let mut index = index;
            if index < 0 {
                index += size;
            }
            if index < 0 || index >= size {
                let result: DbResult = Box::new(None::<Vec<u8>>);
                return Ok(result);
            }
            let mut cur = ll.head;
            for _ in 0..index {
                cur = match cur {
                    Some(k) => ll.nodes.get(&k).and_then(|n| n.next),
                    None => None,
                };
            }
            let value = match cur {
                Some(k) => ll.nodes.get(&k).map(|n| n.value.clone()),
                None => None,
            };
            let result: DbResult = Box::new(value);
            Ok(result)
        })
    }
}

struct LSetOp {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
    index: i64,
    value: Vec<u8>,
}

impl DbOp for LSetOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        let node_prefix = self.node_prefix.clone();
        let index = self.index;
        let value = self.value.clone();
        Box::pin(async move {
            let mut ll = match load_list(tx, &public_key, &node_prefix).await {
                Ok(ll) => ll,
                Err(DbError::Kv(KvError::KeyNotFound)) => {
                    return Err(DbError::Kv(KvError::KeyNotFound))
                }
                Err(e) => return Err(e),
            };
            let size = ll.size as i64;
            let mut index = index;
            if index < 0 {
                index += size;
            }
            if index < 0 || index >= size {
                return Err(DbError::Kv(KvError::KeyNotFound));
            }
            let mut cur = ll.head;
            for _ in 0..index {
                cur = match cur {
                    Some(k) => ll.nodes.get(&k).and_then(|n| n.next),
                    None => None,
                };
            }
            let Some(k) = cur else {
                return Err(DbError::Kv(KvError::KeyNotFound));
            };
            ll.nodes.get_mut(&k).expect("node present").value = value;
            persist_list(tx, &public_key, &node_prefix, &ll)?;
            let result: DbResult = Box::new(());
            Ok(result)
        })
    }
}

struct LRemOp {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
    count: i64,
    value: Vec<u8>,
}

impl DbOp for LRemOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        let node_prefix = self.node_prefix.clone();
        let count = self.count;
        let value = self.value.clone();
        Box::pin(async move {
            let mut ll = match load_list(tx, &public_key, &node_prefix).await {
                Ok(ll) => ll,
                Err(DbError::Kv(KvError::KeyNotFound)) => {
                    let result: DbResult = Box::new(0i64);
                    return Ok(result);
                }
                Err(e) => return Err(e),
            };
            let removed = if count < 0 {
                ll.remove_matching_from_tail(count, &value)
            } else {
                ll.remove_matching(count, &value)
            };
            if ll.size == 0 {
                tx.delete(&public_key)?;
            } else {
                persist_list(tx, &public_key, &node_prefix, &ll)?;
            }
            let result: DbResult = Box::new(removed);
            Ok(result)
        })
    }
}

struct LTrimOp {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
    start: i64,
    stop: i64,
}

impl DbOp for LTrimOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        let node_prefix = self.node_prefix.clone();
        let start = self.start;
        let stop = self.stop;
        Box::pin(async move {
            let mut ll = match load_list(tx, &public_key, &node_prefix).await {
                Ok(ll) => ll,
                Err(DbError::Kv(KvError::KeyNotFound)) => {
                    let result: DbResult = Box::new(());
                    return Ok(result);
                }
                Err(e) => return Err(e),
            };
            let size = ll.size as i64;
            let mut start = start;
            let mut stop = stop;
            if start < 0 {
                start += size;
            }
            if stop < 0 {
                stop += size;
            }
            if start < 0 {
                start = 0;
            }
            if stop >= size {
                stop = size - 1;
            }
            if start > stop || start >= size {
                tx.delete(&public_key)?;
                let result: DbResult = Box::new(());
                return Ok(result);
            }
            ll.trim_to(start as usize, stop as usize);
            persist_list(tx, &public_key, &node_prefix, &ll)?;
            let result: DbResult = Box::new(());
            Ok(result)
        })
    }
}

struct LInsertOp {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
    before: bool,
    pivot: Vec<u8>,
    value: Vec<u8>,
}

impl DbOp for LInsertOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        let node_prefix = self.node_prefix.clone();
        let before = self.before;
        let pivot = self.pivot.clone();
        let value = self.value.clone();
        Box::pin(async move {
            let mut ll = match load_list(tx, &public_key, &node_prefix).await {
                Ok(ll) => ll,
                Err(DbError::Kv(KvError::KeyNotFound)) => {
                    let result: DbResult = Box::new(-1i64);
                    return Ok(result);
                }
                Err(e) => return Err(e),
            };
            if !ll.insert(before, &pivot, value) {
                let result: DbResult = Box::new(-1i64);
                return Ok(result);
            }
            persist_list(tx, &public_key, &node_prefix, &ll)?;
            let result: DbResult = Box::new(ll.size as i64);
            Ok(result)
        })
    }
}

// --- WireOp halves ---

/// Replies an integer result.
struct IntWire;

impl WireOp for IntWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<i64>() {
                Ok(value) => RespValue::Integer(*value),
                Err(_) => internal_error(),
            },
            Err(e) => err_resp(&e),
        }
    }
}

/// Replies a null bulk string for `None`, a bulk string otherwise.
struct NullableBulkWire;

impl WireOp for NullableBulkWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<Option<Vec<u8>>>() {
                Ok(boxed) => match *boxed {
                    Some(value) => RespValue::BulkString(Some(Bytes::from(value))),
                    None => RespValue::BulkString(None),
                },
                Err(_) => internal_error(),
            },
            Err(e) => err_resp(&e),
        }
    }
}

/// Replies an array of bulk strings.
struct BulkArrayWire;

impl WireOp for BulkArrayWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<Vec<Vec<u8>>>() {
                Ok(boxed) => RespValue::Array(Some(
                    boxed
                        .iter()
                        .map(|v| RespValue::BulkString(Some(Bytes::copy_from_slice(v))))
                        .collect(),
                )),
                Err(_) => internal_error(),
            },
            Err(e) => err_resp(&e),
        }
    }
}

/// Replies `+OK` on success, the error otherwise.
struct OkWire;

impl WireOp for OkWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(_) => RespValue::SimpleString(Bytes::from_static(b"OK")),
            Err(e) => err_resp(&e),
        }
    }
}

/// Replies `+OK` on success; a missing key (or out-of-range index) becomes
/// `ERR no such key` to match Go, any other error is rendered normally.
struct LSetWire;

impl WireOp for LSetWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(_) => RespValue::SimpleString(Bytes::from_static(b"OK")),
            Err(DbError::Kv(KvError::KeyNotFound)) => {
                RespValue::Error(Bytes::from_static(b"ERR no such key"))
            }
            Err(e) => err_resp(&e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::test_session;

    /// Runs one op through its own transaction and commits if it mutates,
    /// then renders the reply (the unit-test equivalent of Go's
    /// `kvs.Update`/`kvs.Read` calls).
    async fn exec(session: &Session, op: QueuedOp) -> RespValue {
        let store = session.store();
        let tx = store.begin(op.is_mutating).await.expect("tx");
        let outcome = op.db_op.run(&*tx).await;
        if op.is_mutating {
            tx.commit().await.expect("commit");
        }
        op.wire_op.reply(outcome)
    }

    /// Loads the list stored under `key` for direct inspection.
    async fn load(session: &Session, key: &[u8]) -> Linked {
        let store = session.store();
        let tx = store.begin(false).await.expect("read tx");
        let public_key = session.public_key(key);
        let node_prefix = session.private_key(key);
        let ll = load_list(&*tx, &public_key, &node_prefix)
            .await
            .expect("load");
        drop(tx);
        ll
    }

    /// The element values in chain order.
    fn values(ll: &Linked) -> Vec<Vec<u8>> {
        let mut out = Vec::with_capacity(ll.size as usize);
        let mut cur = ll.head;
        while let Some(k) = cur {
            let node = ll.nodes.get(&k).expect("node present");
            out.push(node.value.clone());
            cur = node.next;
        }
        out
    }

    fn expect_int(replies: &[RespValue]) -> i64 {
        match &replies[0] {
            RespValue::Integer(n) => *n,
            other => panic!("expected integer, got {other:?}"),
        }
    }

    fn expect_bulk(replies: &[RespValue]) -> Option<Bytes> {
        match &replies[0] {
            RespValue::BulkString(b) => b.clone(),
            other => panic!("expected bulk string, got {other:?}"),
        }
    }

    fn expect_bulk_array(replies: &[RespValue]) -> Vec<Bytes> {
        match &replies[0] {
            RespValue::Array(Some(items)) => items
                .iter()
                .map(|r| match r {
                    RespValue::BulkString(b) => b.clone().expect("non-null element"),
                    other => panic!("expected bulk string element, got {other:?}"),
                })
                .collect(),
            other => panic!("expected array, got {other:?}"),
        }
    }

    fn expect_error(replies: &[RespValue]) -> Bytes {
        match &replies[0] {
            RespValue::Error(e) => e.clone(),
            other => panic!("expected error, got {other:?}"),
        }
    }

    /// Builds a list with the given values, mirroring Go's `makeNewList`.
    fn make_list(values: &[&[u8]]) -> Linked {
        let mut ll = Linked::new_empty();
        for v in values {
            ll.add_last(v.to_vec());
        }
        ll
    }

    #[test]
    fn make_new_list_structure() {
        let ll = make_list(&[b"value1", b"value2", b"value3"]);
        assert_eq!(ll.size, 3);
        let head = ll.nodes.get(&ll.head.expect("head")).expect("node");
        assert_eq!(head.value, b"value1");
        assert_eq!(head.prev, None);
        let tail = ll.nodes.get(&ll.tail.expect("tail")).expect("node");
        assert_eq!(tail.value, b"value3");
        assert_eq!(tail.next, None);
    }

    #[test]
    fn iteration_visits_every_node() {
        let ll = make_list(&[b"value1", b"value2", b"value3"]);
        let mut count = 0;
        for &k in &ll.ordered_keys() {
            let node = ll.nodes.get(&k).expect("node present");
            assert!(!node.value.is_empty());
            count += 1;
        }
        assert_eq!(count, 3);
    }

    #[test]
    fn add_first_links_head() {
        let mut ll = make_list(&[b"value1", b"value2"]);
        assert_eq!(ll.add_first(b"newhead".to_vec()), 3);
        let head = ll.nodes.get(&ll.head.expect("head")).expect("node");
        assert_eq!(head.value, b"newhead");
        let next = ll.nodes.get(&head.next.expect("next")).expect("node");
        assert_eq!(next.value, b"value1");
    }

    #[test]
    fn add_last_links_tail() {
        let mut ll = make_list(&[b"value1", b"value2"]);
        assert_eq!(ll.add_last(b"newtail".to_vec()), 3);
        let tail = ll.nodes.get(&ll.tail.expect("tail")).expect("node");
        assert_eq!(tail.value, b"newtail");
        let prev = ll.nodes.get(&tail.prev.expect("prev")).expect("node");
        assert_eq!(prev.value, b"value2");
    }

    #[test]
    fn remove_first_pops_head() {
        let mut ll = make_list(&[b"value1", b"value2", b"value3"]);
        assert_eq!(ll.remove_first().as_deref(), Some(&b"value1"[..]));
        assert_eq!(ll.size, 2);
        let head = ll.nodes.get(&ll.head.expect("head")).expect("node");
        assert_eq!(head.value, b"value2");
    }

    #[test]
    fn remove_last_pops_tail() {
        let mut ll = make_list(&[b"value1", b"value2", b"value3"]);
        assert_eq!(ll.remove_last().as_deref(), Some(&b"value3"[..]));
        assert_eq!(ll.size, 2);
        let tail = ll.nodes.get(&ll.tail.expect("tail")).expect("node");
        assert_eq!(tail.value, b"value2");
    }

    #[test]
    fn random_keys_are_unique_and_nonzero() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..1000 {
            let key = random_key();
            assert!(!is_zero_key(&key));
            assert!(seen.insert(key), "duplicate key");
        }
    }

    #[tokio::test]
    async fn lpush_rpush_persist_and_reload() {
        let session = test_session();
        let key = b"mylist";

        let replies = exec(
            &session,
            lpush(
                &session,
                key,
                &[Bytes::from_static(b"b"), Bytes::from_static(b"a")],
            ),
        )
        .await;
        assert_eq!(expect_int(&[replies]), 2);
        let ll = load(&session, key).await;
        assert_eq!(ll.size, 2);
        let head = ll.nodes.get(&ll.head.expect("head")).expect("node");
        assert_eq!(head.value, b"a");

        let replies = exec(
            &session,
            rpush(
                &session,
                key,
                &[Bytes::from_static(b"c"), Bytes::from_static(b"d")],
            ),
        )
        .await;
        assert_eq!(expect_int(&[replies]), 4);
        let ll = load(&session, key).await;
        assert_eq!(ll.size, 4);
        assert_eq!(
            values(&ll),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]
        );
    }

    #[tokio::test]
    async fn lpop_rpop_remove_ends() {
        let session = test_session();
        let key = b"mylist";
        exec(
            &session,
            lpush(
                &session,
                key,
                &[
                    Bytes::from_static(b"c"),
                    Bytes::from_static(b"b"),
                    Bytes::from_static(b"a"),
                ],
            ),
        )
        .await;

        let replies = exec(&session, lpop(&session, key)).await;
        assert_eq!(expect_bulk(&[replies]).as_deref(), Some(&b"a"[..]));
        let replies = exec(&session, rpop(&session, key)).await;
        assert_eq!(expect_bulk(&[replies]).as_deref(), Some(&b"c"[..]));
        let ll = load(&session, key).await;
        assert_eq!(values(&ll), vec![b"b".to_vec()]);
    }

    #[tokio::test]
    async fn lpop_missing_key_is_null() {
        let session = test_session();
        let replies = exec(&session, lpop(&session, b"nonexistent")).await;
        assert_eq!(expect_bulk(&[replies]), None);
    }

    #[tokio::test]
    async fn llen_reports_size() {
        let session = test_session();
        let replies = exec(&session, llen(&session, b"nonexistent")).await;
        assert_eq!(expect_int(&[replies]), 0);

        exec(
            &session,
            lpush(
                &session,
                b"mylist",
                &[Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            ),
        )
        .await;
        let replies = exec(&session, llen(&session, b"mylist")).await;
        assert_eq!(expect_int(&[replies]), 2);
    }

    #[tokio::test]
    async fn lrange_full_and_partial() {
        let session = test_session();
        let key = b"mylist";
        exec(
            &session,
            rpush(
                &session,
                key,
                &[
                    Bytes::from_static(b"a"),
                    Bytes::from_static(b"b"),
                    Bytes::from_static(b"c"),
                    Bytes::from_static(b"d"),
                    Bytes::from_static(b"e"),
                ],
            ),
        )
        .await;

        let replies = exec(&session, lrange(&session, key, 0, -1)).await;
        let got = expect_bulk_array(&[replies]);
        assert_eq!(got.len(), 5);
        assert_eq!(got[0].as_ref(), b"a");

        let replies = exec(&session, lrange(&session, key, 1, 3)).await;
        let got = expect_bulk_array(&[replies]);
        assert_eq!(
            got,
            vec![
                Bytes::from_static(b"b"),
                Bytes::from_static(b"c"),
                Bytes::from_static(b"d")
            ]
        );

        // Negative indices counted from the tail.
        let replies = exec(&session, lrange(&session, key, -2, -1)).await;
        let got = expect_bulk_array(&[replies]);
        assert_eq!(
            got,
            vec![Bytes::from_static(b"d"), Bytes::from_static(b"e")]
        );
    }

    #[tokio::test]
    async fn lrange_missing_key_is_empty() {
        let session = test_session();
        let replies = exec(&session, lrange(&session, b"nonexistent", 0, -1)).await;
        assert_eq!(expect_bulk_array(&[replies]).len(), 0);
    }

    #[tokio::test]
    async fn lindex_valid_negative_and_out_of_range() {
        let session = test_session();
        let key = b"mylist";
        exec(
            &session,
            rpush(
                &session,
                key,
                &[
                    Bytes::from_static(b"a"),
                    Bytes::from_static(b"b"),
                    Bytes::from_static(b"c"),
                ],
            ),
        )
        .await;

        let replies = exec(&session, lindex(&session, key, 1)).await;
        assert_eq!(expect_bulk(&[replies]).as_deref(), Some(&b"b"[..]));
        let replies = exec(&session, lindex(&session, key, -1)).await;
        assert_eq!(expect_bulk(&[replies]).as_deref(), Some(&b"c"[..]));
        let replies = exec(&session, lindex(&session, key, 10)).await;
        assert_eq!(expect_bulk(&[replies]), None);
        let replies = exec(&session, lindex(&session, key, -10)).await;
        assert_eq!(expect_bulk(&[replies]), None);
    }

    #[tokio::test]
    async fn lset_updates_and_errors_on_out_of_range() {
        let session = test_session();
        let key = b"mylist";
        exec(
            &session,
            rpush(
                &session,
                key,
                &[
                    Bytes::from_static(b"a"),
                    Bytes::from_static(b"b"),
                    Bytes::from_static(b"c"),
                ],
            ),
        )
        .await;

        let replies = exec(&session, lset(&session, key, 1, b"x")).await;
        assert_eq!(replies, RespValue::SimpleString(Bytes::from_static(b"OK")));
        let replies = exec(&session, lindex(&session, key, 1)).await;
        assert_eq!(expect_bulk(&[replies]).as_deref(), Some(&b"x"[..]));

        let replies = exec(&session, lset(&session, key, 10, b"x")).await;
        assert_eq!(
            expect_error(&[replies]),
            Bytes::from_static(b"ERR no such key")
        );
        let replies = exec(&session, lset(&session, b"nokey", 0, b"x")).await;
        assert_eq!(
            expect_error(&[replies]),
            Bytes::from_static(b"ERR no such key")
        );
    }

    #[tokio::test]
    async fn lrem_removes_all_matches() {
        let session = test_session();
        let key = b"mylist";
        exec(
            &session,
            rpush(
                &session,
                key,
                &[
                    Bytes::from_static(b"a"),
                    Bytes::from_static(b"b"),
                    Bytes::from_static(b"b"),
                    Bytes::from_static(b"c"),
                ],
            ),
        )
        .await;

        let replies = exec(&session, lrem(&session, key, 0, b"b")).await;
        assert_eq!(expect_int(&[replies]), 2);
        let replies = exec(&session, lrange(&session, key, 0, -1)).await;
        let got = expect_bulk_array(&[replies]);
        assert_eq!(
            got,
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"c")]
        );
    }

    #[tokio::test]
    async fn lrem_positive_and_negative_counts() {
        let session = test_session();
        let key = b"mylist";
        exec(
            &session,
            rpush(
                &session,
                key,
                &[
                    Bytes::from_static(b"a"),
                    Bytes::from_static(b"b"),
                    Bytes::from_static(b"b"),
                    Bytes::from_static(b"c"),
                    Bytes::from_static(b"b"),
                ],
            ),
        )
        .await;

        // Positive count removes from the head.
        let replies = exec(&session, lrem(&session, key, 1, b"b")).await;
        assert_eq!(expect_int(&[replies]), 1);
        let replies = exec(&session, lrange(&session, key, 0, -1)).await;
        assert_eq!(
            expect_bulk_array(&[replies]),
            vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
                Bytes::from_static(b"c"),
                Bytes::from_static(b"b")
            ]
        );

        // Negative count removes from the tail.
        let replies = exec(&session, lrem(&session, key, -1, b"b")).await;
        assert_eq!(expect_int(&[replies]), 1);
        let replies = exec(&session, lrange(&session, key, 0, -1)).await;
        assert_eq!(
            expect_bulk_array(&[replies]),
            vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
                Bytes::from_static(b"c")
            ]
        );
    }

    #[tokio::test]
    async fn lrem_missing_key_is_zero() {
        let session = test_session();
        let replies = exec(&session, lrem(&session, b"nokey", 0, b"x")).await;
        assert_eq!(expect_int(&[replies]), 0);
    }

    #[tokio::test]
    async fn ltrim_keeps_range() {
        let session = test_session();
        let key = b"mylist";
        exec(
            &session,
            rpush(
                &session,
                key,
                &[
                    Bytes::from_static(b"a"),
                    Bytes::from_static(b"b"),
                    Bytes::from_static(b"c"),
                    Bytes::from_static(b"d"),
                    Bytes::from_static(b"e"),
                ],
            ),
        )
        .await;

        let replies = exec(&session, ltrim(&session, key, 1, 3)).await;
        assert_eq!(replies, RespValue::SimpleString(Bytes::from_static(b"OK")));
        let replies = exec(&session, lrange(&session, key, 0, -1)).await;
        assert_eq!(
            expect_bulk_array(&[replies]),
            vec![
                Bytes::from_static(b"b"),
                Bytes::from_static(b"c"),
                Bytes::from_static(b"d")
            ]
        );
    }

    #[tokio::test]
    async fn ltrim_out_of_range_deletes_list() {
        let session = test_session();
        let key = b"mylist";
        exec(
            &session,
            rpush(
                &session,
                key,
                &[Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            ),
        )
        .await;
        let replies = exec(&session, ltrim(&session, key, 5, 10)).await;
        assert_eq!(replies, RespValue::SimpleString(Bytes::from_static(b"OK")));
        let replies = exec(&session, llen(&session, key)).await;
        assert_eq!(expect_int(&[replies]), 0);
    }

    #[tokio::test]
    async fn linsert_before_after_and_missing_pivot() {
        let session = test_session();
        let key = b"mylist";
        exec(
            &session,
            rpush(
                &session,
                key,
                &[Bytes::from_static(b"a"), Bytes::from_static(b"c")],
            ),
        )
        .await;

        let replies = exec(&session, linsert(&session, key, true, b"c", b"b")).await;
        assert_eq!(expect_int(&[replies]), 3);
        let replies = exec(&session, lrange(&session, key, 0, -1)).await;
        assert_eq!(
            expect_bulk_array(&[replies]),
            vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
                Bytes::from_static(b"c")
            ]
        );

        let replies = exec(&session, linsert(&session, key, false, b"c", b"d")).await;
        assert_eq!(expect_int(&[replies]), 4);
        let replies = exec(&session, lrange(&session, key, 0, -1)).await;
        assert_eq!(
            expect_bulk_array(&[replies]),
            vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
                Bytes::from_static(b"c"),
                Bytes::from_static(b"d")
            ]
        );

        let replies = exec(&session, linsert(&session, key, true, b"zzz", b"x")).await;
        assert_eq!(expect_int(&[replies]), -1);
        let replies = exec(&session, linsert(&session, b"nokey", true, b"a", b"x")).await;
        assert_eq!(expect_int(&[replies]), -1);
    }

    #[tokio::test]
    async fn lpushx_rpushx_noop_on_missing_key() {
        let session = test_session();
        let key = b"mylist";

        let replies = exec(&session, lpushx(&session, key, b"a")).await;
        assert_eq!(expect_int(&[replies]), 0);

        exec(&session, lpush(&session, key, &[Bytes::from_static(b"a")])).await;
        let replies = exec(&session, lpushx(&session, key, b"x")).await;
        assert_eq!(expect_int(&[replies]), 2);

        let replies = exec(&session, rpushx(&session, b"other", b"a")).await;
        assert_eq!(expect_int(&[replies]), 0);
        let replies = exec(&session, rpushx(&session, key, b"y")).await;
        assert_eq!(expect_int(&[replies]), 3);
    }

    #[tokio::test]
    async fn delete_on_empty_after_pop() {
        let session = test_session();
        let key = b"mylist";
        exec(&session, rpush(&session, key, &[Bytes::from_static(b"a")])).await;

        let replies = exec(&session, lpop(&session, key)).await;
        assert_eq!(expect_bulk(&[replies]).as_deref(), Some(&b"a"[..]));
        let replies = exec(&session, llen(&session, key)).await;
        assert_eq!(expect_int(&[replies]), 0);
    }

    #[tokio::test]
    async fn wrong_type_on_string_key() {
        let session = test_session();
        let key = b"strkey";
        exec(&session, crate::strings::set(&session, key, b"hi")).await;

        let replies = exec(&session, lpush(&session, key, &[Bytes::from_static(b"a")])).await;
        assert_eq!(
            expect_error(&[replies]),
            Bytes::from_static(
                b"WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
        let replies = exec(&session, llen(&session, key)).await;
        assert_eq!(
            expect_error(&[replies]),
            Bytes::from_static(
                b"WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
        let replies = exec(&session, lpushx(&session, key, b"a")).await;
        assert_eq!(
            expect_error(&[replies]),
            Bytes::from_static(
                b"WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
    }

    #[tokio::test]
    async fn sentinel_layout_matches_go() {
        // A single pushed element yields the expected on-disk sentinel bytes
        // (size 1, head key == tail key) and a node entry under the derived
        // private key.
        let session = test_session();
        let key = b"mylist";
        exec(&session, rpush(&session, key, &[Bytes::from_static(b"v")])).await;

        let store = session.store();
        let tx = store.begin(false).await.expect("read tx");
        let item = tx.get(&session.public_key(key)).await.expect("sentinel");
        assert_eq!(item.metadata(), TYPE_LIST);
        let val = item.value();
        assert_eq!(val.len(), SENTINEL_LENGTH);
        assert_eq!(u32::from_be_bytes(val[0..4].try_into().unwrap()), 1);
        let head_key = &val[4..20];
        let tail_key = &val[20..36];
        assert_eq!(head_key, tail_key);
        assert!(!is_zero_key(head_key));

        let mut node_storage_key = session.private_key(key);
        node_storage_key.push(b':');
        node_storage_key.extend_from_slice(head_key);
        let node_item = tx.get(&node_storage_key).await.expect("node");
        assert_eq!(node_item.metadata(), TYPE_LIST);
        assert_eq!(&node_item.value()[..1], b"v");
        drop(tx);
    }
}
