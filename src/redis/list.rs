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
//!
//! # Where the entries live (RFC 0001)
//!
//! The `-<db>:<name>` prefix in the layout above is the *legacy* one, derived
//! from the key's name, which is how values written by earlier releases are
//! stored. A value created by this release records a **namespace** instead
//! (see [`crate::common::namespace`]): its sentinel carries the prefix its
//! entries live under (`+<db>:` and a random id), so the name plays no part in
//! where the data is stored and the value can be renamed without moving it.
//! The suffixes appended to the prefix are the same either way.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use kv::kv::{BoxFuture, Entry, Error as KvError, Tx};

use crate::common::namespace::{
    self, base_type, resolve_prefix, resolve_prefix_for_write, ElementPrefix,
};
use crate::common::op::{err_resp, DbError, DbOp, DbResult, QueuedOp, WireOp};
use crate::common::session::Session;
use crate::common::ValueType;
use crate::resp::RespValue;
use smallvec::smallvec;

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
    /// Nodes taken out of the chain since the list was loaded. Their stored
    /// entries must be deleted when the list is persisted, or they are left
    /// behind forever.
    removed: Vec<[u8; KEY_LENGTH]>,
    /// Nodes created or whose value/links changed since load. Only these are
    /// written by [`persist_list`]; every other loaded node is already stored
    /// exactly as it is in memory.
    dirty: HashSet<[u8; KEY_LENGTH]>,
}

impl Linked {
    fn new_empty() -> Self {
        Self {
            size: 0,
            head: None,
            tail: None,
            nodes: HashMap::new(),
            removed: Vec::new(),
            dirty: HashSet::new(),
        }
    }

    /// Sets a loaded node's `prev` link, marking it dirty if it changed.
    fn set_prev(&mut self, key: [u8; KEY_LENGTH], prev: Option<[u8; KEY_LENGTH]>) {
        if let Some(n) = self.nodes.get_mut(&key) {
            if n.prev != prev {
                n.prev = prev;
                self.dirty.insert(key);
            }
        }
    }

    /// Sets the link pointing away from the walk's start: `next` when walking
    /// forward, `prev` when walking backward.
    fn set_fwd(&mut self, key: [u8; KEY_LENGTH], v: Option<[u8; KEY_LENGTH]>, fwd: bool) {
        if fwd {
            self.set_next(key, v)
        } else {
            self.set_prev(key, v)
        }
    }

    /// Sets the link pointing back towards the walk's start.
    fn set_back(&mut self, key: [u8; KEY_LENGTH], v: Option<[u8; KEY_LENGTH]>, fwd: bool) {
        self.set_fwd(key, v, !fwd)
    }

    /// Sets a loaded node's `next` link, marking it dirty if it changed.
    fn set_next(&mut self, key: [u8; KEY_LENGTH], next: Option<[u8; KEY_LENGTH]>) {
        if let Some(n) = self.nodes.get_mut(&key) {
            if n.next != next {
                n.next = next;
                self.dirty.insert(key);
            }
        }
    }

    /// Deletes the stored entries of every node removed from this list.
    fn delete_removed_nodes(&self, tx: &dyn Tx, node_prefix: &[u8]) -> Result<(), DbError> {
        for key in &self.removed {
            tx.delete(&node_key(node_prefix, key))?;
        }
        Ok(())
    }

    /// Deletes the stored entries of every node of this list, live or removed.
    fn delete_all_nodes(&self, tx: &dyn Tx, node_prefix: &[u8]) -> Result<(), DbError> {
        self.delete_removed_nodes(tx, node_prefix)?;
        for key in self.nodes.keys() {
            tx.delete(&node_key(node_prefix, key))?;
        }
        Ok(())
    }

    /// The node keys in chain order, head to tail (fully loaded lists only).
    #[cfg(test)]
    fn ordered_keys(&self) -> Vec<[u8; KEY_LENGTH]> {
        let mut keys = Vec::with_capacity(self.size as usize);
        let mut cur = self.head;
        while let Some(k) = cur {
            keys.push(k);
            cur = self.nodes.get(&k).and_then(|n| n.next);
        }
        keys
    }

    /// Prepends `value`, returning the new size. The current head, if any,
    /// must be loaded.
    fn add_first(&mut self, value: Vec<u8>) -> u32 {
        let key = random_key();
        let next = self.head;
        if let Some(old) = next {
            self.set_prev(old, Some(key));
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
        self.dirty.insert(key);
        self.head = Some(key);
        self.size += 1;
        self.size
    }

    /// Appends `value`, returning the new size. The current tail, if any,
    /// must be loaded.
    fn add_last(&mut self, value: Vec<u8>) -> u32 {
        let key = random_key();
        let prev = self.tail;
        if let Some(old) = prev {
            self.set_next(old, Some(key));
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
        self.dirty.insert(key);
        self.tail = Some(key);
        self.size += 1;
        self.size
    }

    /// Removes and returns the head element, if any. The head and its
    /// successor must be loaded.
    fn remove_first(&mut self) -> Option<Vec<u8>> {
        let head = self.head?;
        let node = self.nodes.remove(&head)?;
        self.removed.push(head);
        let value = node.value;
        let new_head = node.next;
        self.head = new_head;
        match new_head {
            Some(nh) => self.set_prev(nh, None),
            None => self.tail = None,
        }
        self.size = self.size.saturating_sub(1);
        Some(value)
    }

    /// Removes and returns the tail element, if any. The tail and its
    /// predecessor must be loaded.
    fn remove_last(&mut self) -> Option<Vec<u8>> {
        let tail = self.tail?;
        let node = self.nodes.remove(&tail)?;
        self.removed.push(tail);
        let value = node.value;
        let new_tail = node.prev;
        self.tail = new_tail;
        match new_tail {
            Some(nt) => self.set_next(nt, None),
            None => self.head = None,
        }
        self.size = self.size.saturating_sub(1);
        Some(value)
    }

    /// Inserts `value` before or after the loaded node `pivot_key`. The
    /// pivot's neighbour on the insertion side must be loaded too, if any.
    fn insert_at(&mut self, pivot_key: [u8; KEY_LENGTH], before: bool, value: Vec<u8>) {
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
        self.dirty.insert(new_key);
        if before {
            match pivot_prev {
                Some(pp) => self.set_next(pp, Some(new_key)),
                None => self.head = Some(new_key),
            }
            self.set_prev(pivot_key, Some(new_key));
        } else {
            match pivot_next {
                Some(pn) => self.set_prev(pn, Some(new_key)),
                None => self.tail = Some(new_key),
            }
            self.set_next(pivot_key, Some(new_key));
        }
        self.size += 1;
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

/// Re-keys every node of the list whose sentinel lives at `public_key` from
/// `from_prefix` to `to_prefix` (both the prefix of the node keys, without the
/// `:<node key>` suffix). Nodes are found by walking the chain from the
/// sentinel's head, because a prefix scan would also match the entries of
/// unrelated keys such as `<name>:other`. Links store only the 16-byte node
/// key, so node contents are copied unchanged.
pub(crate) async fn move_nodes(
    tx: &dyn Tx,
    public_key: &[u8],
    from_prefix: &[u8],
    to_prefix: &[u8],
) -> Result<(), DbError> {
    let (size, head, _tail) = read_sentinel(tx, public_key).await?;
    let mut current = head;
    for _ in 0..size {
        if current == [0u8; KEY_LENGTH] {
            break;
        }
        let from = node_key(from_prefix, &current);
        let item = tx.get(&from).await?;
        let value = item.value();
        if value.len() < 2 * KEY_LENGTH {
            return Err(DbError::Kv(KvError::KeyNotFound));
        }
        let mut next = [0u8; KEY_LENGTH];
        next.copy_from_slice(&value[value.len() - KEY_LENGTH..]);
        tx.set(
            Entry::new(node_key(to_prefix, &current), value.to_vec()).metadata(item.metadata()),
        )?;
        tx.delete(&from)?;
        current = next;
    }
    Ok(())
}

/// Deletes every node of the list whose sentinel lives at `public_key`, for
/// `DEL`. Nodes are found by walking the chain from the sentinel's head,
/// because a prefix scan on `-<db>:<name>:` would also match the entries of
/// unrelated keys such as `<name>:other`.
pub(crate) async fn delete_nodes(
    tx: &dyn Tx,
    public_key: &[u8],
    node_prefix: &[u8],
) -> Result<(), DbError> {
    let (size, head, _tail) = read_sentinel(tx, public_key).await?;
    let mut current = head;
    for _ in 0..size {
        if current == [0u8; KEY_LENGTH] {
            break;
        }
        let key = node_key(node_prefix, &current);
        let item = tx.get(&key).await?;
        let value = item.value();
        if value.len() < 2 * KEY_LENGTH {
            return Err(DbError::Kv(KvError::KeyNotFound));
        }
        let mut next = [0u8; KEY_LENGTH];
        next.copy_from_slice(&value[value.len() - KEY_LENGTH..]);
        tx.delete(&key)?;
        current = next;
    }
    Ok(())
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
    if base_type(item.metadata()) != TYPE_LIST {
        return Err(DbError::WrongType);
    }
    let val = namespace::payload(&item)?;
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

/// Writes the list sentinel entry with list metadata. `prefix` is the prefix
/// the list's nodes are stored under; the sentinel records it when it is a
/// namespace and keeps the key's TTL.
async fn write_sentinel(
    tx: &dyn Tx,
    public_key: &[u8],
    head: Option<[u8; KEY_LENGTH]>,
    tail: Option<[u8; KEY_LENGTH]>,
    size: u32,
    prefix: &ElementPrefix,
) -> Result<(), DbError> {
    let mut buf = [0u8; SENTINEL_LENGTH];
    buf[0..4].copy_from_slice(&size.to_be_bytes());
    if let Some(h) = head {
        buf[4..20].copy_from_slice(&h);
    }
    if let Some(t) = tail {
        buf[20..36].copy_from_slice(&t);
    }
    namespace::write_sentinel(tx, public_key, buf.to_vec(), TYPE_LIST, prefix).await
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
            if node.prev != prev || node.next != next {
                node.prev = prev;
                node.next = next;
                ll.dirty.insert(k);
            }
        }
    }

    ll.head = ll.nodes.contains_key(&head_key).then_some(head_key);
    ll.tail = ll.nodes.contains_key(&tail_key).then_some(tail_key);
    Ok(ll)
}

/// Reads and decodes one node. A missing or truncated entry is
/// [`KvError::KeyNotFound`], as in [`load_list`].
async fn read_node(
    tx: &dyn Tx,
    node_prefix: &[u8],
    key: [u8; KEY_LENGTH],
) -> Result<ListNode, DbError> {
    let item = tx.get(&node_key(node_prefix, &key)).await?;
    let val = item.value();
    if val.len() < 2 * KEY_LENGTH {
        return Err(DbError::Kv(KvError::KeyNotFound));
    }
    let split = val.len() - 2 * KEY_LENGTH;
    Ok(ListNode {
        key,
        value: val[..split].to_vec(),
        prev: to_opt_key(&val[split..split + KEY_LENGTH]),
        next: to_opt_key(&val[split + KEY_LENGTH..]),
    })
}

/// Loads only the nodes at one end of the list: up to `depth` nodes starting
/// from the head (following `next`) or the tail (following `prev`). The
/// sentinel already records both end keys, so operations touching only the
/// ends need not walk the whole chain. `depth` 0 loads just the sentinel
/// (size, head, tail). `depth` 1 suffices for a push (the old end node gets a
/// new link); a pop needs 2 (the node being removed and its neighbour).
/// The result must only be mutated at the loaded end.
async fn load_end(
    tx: &dyn Tx,
    public_key: &[u8],
    node_prefix: &[u8],
    from_tail: bool,
    depth: usize,
) -> Result<Linked, DbError> {
    let (size, head_key, tail_key) = read_sentinel(tx, public_key).await?;
    let mut ll = Linked::new_empty();
    ll.size = size;
    ll.head = to_opt_key(&head_key);
    ll.tail = to_opt_key(&tail_key);

    let mut current = if from_tail { ll.tail } else { ll.head };
    for _ in 0..depth {
        let Some(key) = current else { break };
        let node = read_node(tx, node_prefix, key).await?;
        current = if from_tail { node.prev } else { node.next };
        if ll.nodes.insert(key, node).is_some() {
            break; // cycle
        }
    }
    Ok(ll)
}

/// Reads the node at `index` (which must be below `ll.size`), walking from
/// whichever end is nearer, like Redis. `ll` need only carry the sentinel.
async fn node_at(
    tx: &dyn Tx,
    node_prefix: &[u8],
    ll: &Linked,
    index: usize,
) -> Result<ListNode, DbError> {
    let back = ll.size as usize - 1 - index;
    let (mut cur, steps, fwd) = if index <= back {
        (ll.head, index, true)
    } else {
        (ll.tail, back, false)
    };
    for i in 0..=steps {
        let key = cur.ok_or(DbError::Kv(KvError::KeyNotFound))?;
        let node = read_node(tx, node_prefix, key).await?;
        if i == steps {
            return Ok(node);
        }
        cur = if fwd { node.next } else { node.prev };
    }
    Err(DbError::Kv(KvError::KeyNotFound))
}

/// Reads the values at indices `start..=stop` (already clamped to the list),
/// walking from whichever end makes for fewer skipped nodes.
async fn values_in_range(
    tx: &dyn Tx,
    node_prefix: &[u8],
    ll: &Linked,
    start: usize,
    stop: usize,
) -> Result<Vec<Vec<u8>>, DbError> {
    let len = stop - start + 1;
    let back = ll.size as usize - 1 - stop;
    let fwd = start <= back;
    let (mut cur, skip) = if fwd {
        (ll.head, start)
    } else {
        (ll.tail, back)
    };
    let mut out = Vec::with_capacity(len);
    for i in 0..skip + len {
        let key = cur.ok_or(DbError::Kv(KvError::KeyNotFound))?;
        let node = read_node(tx, node_prefix, key).await?;
        cur = if fwd { node.next } else { node.prev };
        if i >= skip {
            out.push(node.value);
        }
    }
    if !fwd {
        out.reverse();
    }
    Ok(out)
}

/// Walks forward from the head until a node holds `value`, returning its
/// position and the node. Stops at the first match.
async fn find_first(
    tx: &dyn Tx,
    node_prefix: &[u8],
    ll: &Linked,
    value: &[u8],
) -> Result<Option<(usize, ListNode)>, DbError> {
    let mut cur = ll.head;
    for pos in 0..ll.size as usize {
        let Some(key) = cur else { break };
        let node = read_node(tx, node_prefix, key).await?;
        if node.value == value {
            return Ok(Some((pos, node)));
        }
        cur = node.next;
    }
    Ok(None)
}

/// Removes elements equal to `value`, walking from the head (`fwd`) or the
/// tail and stopping once `limit` have been removed (`None` removes all).
/// Only the visited nodes, and the one node after the last visited, are
/// loaded. `ll` need only carry the sentinel. Returns how many were removed.
async fn remove_matching_walk(
    tx: &dyn Tx,
    node_prefix: &[u8],
    ll: &mut Linked,
    fwd: bool,
    limit: Option<usize>,
    value: &[u8],
) -> Result<i64, DbError> {
    let total = ll.size as usize;
    let mut cur = if fwd { ll.head } else { ll.tail };
    // Last retained node on the start side, and the first one.
    let mut prev_kept: Option<[u8; KEY_LENGTH]> = None;
    let mut first_kept: Option<[u8; KEY_LENGTH]> = None;
    let mut removed = 0usize;
    let mut visited = 0usize;
    while let Some(k) = cur {
        if visited >= total || limit.is_some_and(|l| removed >= l) {
            break;
        }
        visited += 1;
        let node = read_node(tx, node_prefix, k).await?;
        let onward = if fwd { node.next } else { node.prev };
        let is_match = node.value == value;
        ll.nodes.insert(k, node);
        // Re-point at the previous retained node (a no-op unless a removal
        // happened in between).
        ll.set_back(k, prev_kept, fwd);
        if is_match {
            if let Some(p) = prev_kept {
                ll.set_fwd(p, onward, fwd);
            }
            ll.nodes.remove(&k);
            ll.removed.push(k);
            ll.size = ll.size.saturating_sub(1);
            removed += 1;
        } else {
            first_kept.get_or_insert(k);
            prev_kept = Some(k);
        }
        cur = onward;
    }
    if removed == 0 {
        return Ok(0);
    }
    // `cur` is now the first unvisited node, if any; its back link must skip
    // whatever was removed before it.
    if let Some(rest) = cur {
        let node = read_node(tx, node_prefix, rest).await?;
        ll.nodes.insert(rest, node);
        ll.set_back(rest, prev_kept, fwd);
    }
    let start_end = first_kept.or(cur);
    let far_end = if cur.is_none() { Some(prev_kept) } else { None };
    if fwd {
        ll.head = start_end;
        if let Some(t) = far_end {
            ll.tail = t;
        }
    } else {
        ll.tail = start_end;
        if let Some(h) = far_end {
            ll.head = h;
        }
    }
    Ok(removed as i64)
}

/// Trims the list to indices `[start, stop]` (clamped, and not the whole
/// list), loading only the nodes being removed plus the two new ends. `ll`
/// need only carry the sentinel.
async fn trim_ends(
    tx: &dyn Tx,
    node_prefix: &[u8],
    ll: &mut Linked,
    start: usize,
    stop: usize,
) -> Result<(), DbError> {
    let size = ll.size as usize;
    let (old_head, old_tail) = (ll.head, ll.tail);
    if start > 0 {
        let mut cur = old_head;
        for _ in 0..start {
            let k = cur.ok_or(DbError::Kv(KvError::KeyNotFound))?;
            cur = read_node(tx, node_prefix, k).await?.next;
            ll.removed.push(k);
        }
        let k = cur.ok_or(DbError::Kv(KvError::KeyNotFound))?;
        let node = read_node(tx, node_prefix, k).await?;
        ll.nodes.insert(k, node);
        ll.set_prev(k, None);
        ll.head = Some(k);
    }
    if stop + 1 < size {
        let mut cur = old_tail;
        for _ in 0..size - 1 - stop {
            let k = cur.ok_or(DbError::Kv(KvError::KeyNotFound))?;
            cur = read_node(tx, node_prefix, k).await?.prev;
            ll.removed.push(k);
        }
        let k = cur.ok_or(DbError::Kv(KvError::KeyNotFound))?;
        if !ll.nodes.contains_key(&k) {
            let node = read_node(tx, node_prefix, k).await?;
            ll.nodes.entry(k).or_insert(node);
        }
        ll.set_next(k, None);
        ll.tail = Some(k);
    }
    ll.size = (stop - start + 1) as u32;
    Ok(())
}

/// Persists the sentinel and the nodes that were created or changed since
/// load, and deletes removed nodes. Untouched nodes are not rewritten.
async fn persist_list(
    tx: &dyn Tx,
    public_key: &[u8],
    node_prefix: &ElementPrefix,
    ll: &Linked,
) -> Result<(), DbError> {
    write_sentinel(tx, public_key, ll.head, ll.tail, ll.size, node_prefix).await?;
    ll.delete_removed_nodes(tx, node_prefix)?;
    for key in &ll.dirty {
        let Some(node) = ll.nodes.get(key) else {
            continue; // created and removed within the same operation
        };
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
        keys: Some(session.key_sv(key)),
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
        keys: Some(session.key_sv(key)),
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
        keys: Some(session.key_sv(key)),
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
        keys: Some(session.key_sv(key)),
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
        keys: Some(session.key_sv(key)),
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
        keys: Some(session.key_sv(key)),
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
        keys: Some(session.key_sv(key)),
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
        keys: Some(session.key_sv(key)),
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
        keys: Some(session.key_sv(key)),
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
        keys: Some(session.key_sv(key)),
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
        keys: Some(session.key_sv(key)),
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
        keys: Some(session.key_sv(key)),
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
        keys: Some(session.key_sv(key)),
    }
}

/// `RPOPLPUSH source destination` — atomically pops the tail of `source` and
/// pushes it to the head of `destination`, returning the element. If `source`
/// does not exist, nil is returned.
pub fn rpoplpush(session: &Session, source: &[u8], destination: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(RPopLPushOp {
            src_public: session.public_key(source),
            src_node_prefix: session.private_key(source),
            dst_public: session.public_key(destination),
            dst_node_prefix: session.private_key(destination),
        }),
        wire_op: Box::new(NullableBulkWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
        keys: Some(smallvec![
            String::from_utf8_lossy(&session.public_key(source)).into_owned(),
            String::from_utf8_lossy(&session.public_key(destination)).into_owned()
        ]),
    }
}

/// `LPOS key element [RANK rank] [COUNT count]` — returns the index of the
/// first occurrence of `element`, or nil if not found. RANK and COUNT are
/// accepted but not yet implemented (they default to single-first match).
pub fn lpos(session: &Session, key: &[u8], element: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(LPosOp {
            public_key: session.public_key(key),
            node_prefix: session.private_key(key),
            element: element.to_vec(),
        }),
        wire_op: Box::new(NullableIntWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
        keys: Some(session.key_sv(key)),
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
            let node_prefix = resolve_prefix_for_write(tx, &public_key, &node_prefix).await?;
            let mut ll = match load_end(tx, &public_key, &node_prefix, at_tail, 1).await {
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
            persist_list(tx, &public_key, &node_prefix, &ll).await?;
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
            let node_prefix = resolve_prefix(tx, &public_key, &node_prefix).await?;
            let mut ll = match load_end(tx, &public_key, &node_prefix, from_tail, 2).await {
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
                ll.delete_removed_nodes(tx, &node_prefix)?;
                tx.delete(&public_key)?;
            } else {
                persist_list(tx, &public_key, &node_prefix, &ll).await?;
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
            let node_prefix = resolve_prefix(tx, &public_key, &node_prefix).await?;
            let ll = match load_end(tx, &public_key, &node_prefix, false, 0).await {
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
            let out = values_in_range(tx, &node_prefix, &ll, start as usize, stop as usize).await?;
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
            let node_prefix = resolve_prefix(tx, &public_key, &node_prefix).await?;
            let ll = match load_end(tx, &public_key, &node_prefix, false, 0).await {
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
            let value = node_at(tx, &node_prefix, &ll, index as usize).await?.value;
            let result: DbResult = Box::new(Some(value));
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
            let node_prefix = resolve_prefix(tx, &public_key, &node_prefix).await?;
            let mut ll = match load_end(tx, &public_key, &node_prefix, false, 0).await {
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
            let mut node = node_at(tx, &node_prefix, &ll, index as usize).await?;
            let k = node.key;
            node.value = value;
            ll.nodes.insert(k, node);
            ll.dirty.insert(k);
            persist_list(tx, &public_key, &node_prefix, &ll).await?;
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
            let node_prefix = resolve_prefix(tx, &public_key, &node_prefix).await?;
            let mut ll = match load_end(tx, &public_key, &node_prefix, false, 0).await {
                Ok(ll) => ll,
                Err(DbError::Kv(KvError::KeyNotFound)) => {
                    let result: DbResult = Box::new(0i64);
                    return Ok(result);
                }
                Err(e) => return Err(e),
            };
            let limit = (count != 0).then(|| count.unsigned_abs() as usize);
            let removed =
                remove_matching_walk(tx, &node_prefix, &mut ll, count >= 0, limit, &value).await?;
            if removed == 0 {
                // Nothing changed: skip the write entirely.
            } else if ll.size == 0 {
                ll.delete_removed_nodes(tx, &node_prefix)?;
                tx.delete(&public_key)?;
            } else {
                persist_list(tx, &public_key, &node_prefix, &ll).await?;
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
            let node_prefix = resolve_prefix(tx, &public_key, &node_prefix).await?;
            let mut ll = match load_end(tx, &public_key, &node_prefix, false, 0).await {
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
                // Everything goes, so every node has to be visited anyway.
                let ll = load_list(tx, &public_key, &node_prefix).await?;
                ll.delete_all_nodes(tx, &node_prefix)?;
                tx.delete(&public_key)?;
                let result: DbResult = Box::new(());
                return Ok(result);
            }
            if start == 0 && stop == size - 1 {
                let result: DbResult = Box::new(());
                return Ok(result); // nothing to trim
            }
            trim_ends(tx, &node_prefix, &mut ll, start as usize, stop as usize).await?;
            persist_list(tx, &public_key, &node_prefix, &ll).await?;
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
            let node_prefix = resolve_prefix(tx, &public_key, &node_prefix).await?;
            let mut ll = match load_end(tx, &public_key, &node_prefix, false, 0).await {
                Ok(ll) => ll,
                Err(DbError::Kv(KvError::KeyNotFound)) => {
                    let result: DbResult = Box::new(-1i64);
                    return Ok(result);
                }
                Err(e) => return Err(e),
            };
            let Some((_, pivot_node)) = find_first(tx, &node_prefix, &ll, &pivot).await? else {
                let result: DbResult = Box::new(-1i64);
                return Ok(result);
            };
            // The neighbour on the insertion side gets a new link too.
            let pivot_key = pivot_node.key;
            let neighbour = if before {
                pivot_node.prev
            } else {
                pivot_node.next
            };
            ll.nodes.insert(pivot_key, pivot_node);
            if let Some(nk) = neighbour {
                let n = read_node(tx, &node_prefix, nk).await?;
                ll.nodes.insert(nk, n);
            }
            ll.insert_at(pivot_key, before, value);
            persist_list(tx, &public_key, &node_prefix, &ll).await?;
            let result: DbResult = Box::new(ll.size as i64);
            Ok(result)
        })
    }
}

struct RPopLPushOp {
    src_public: Vec<u8>,
    src_node_prefix: Vec<u8>,
    dst_public: Vec<u8>,
    dst_node_prefix: Vec<u8>,
}

impl DbOp for RPopLPushOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let src_public = self.src_public.clone();
        let src_prefix = self.src_node_prefix.clone();
        let dst_public = self.dst_public.clone();
        let dst_prefix = self.dst_node_prefix.clone();
        Box::pin(async move {
            let src_prefix = resolve_prefix(tx, &src_public, &src_prefix).await?;
            // Pop tail of source.
            let mut src_ll = match load_end(tx, &src_public, &src_prefix, true, 2).await {
                Ok(ll) => ll,
                Err(DbError::Kv(KvError::KeyNotFound)) => {
                    let result: DbResult = Box::new(None::<Vec<u8>>);
                    return Ok(result);
                }
                Err(e) => return Err(e),
            };
            let value = match src_ll.remove_last() {
                Some(v) => v,
                None => {
                    let result: DbResult = Box::new(None::<Vec<u8>>);
                    return Ok(result);
                }
            };
            // Save source list.
            if src_ll.size == 0 {
                src_ll.delete_removed_nodes(tx, &src_prefix)?;
                tx.delete(&src_public)?;
            } else {
                persist_list(tx, &src_public, &src_prefix, &src_ll).await?;
            }
            // Push to head of destination. Resolved only now: it may be the
            // source list, whose sentinel was just rewritten or deleted.
            let dst_prefix = resolve_prefix_for_write(tx, &dst_public, &dst_prefix).await?;
            let mut dst_ll = match load_end(tx, &dst_public, &dst_prefix, false, 1).await {
                Ok(ll) => ll,
                Err(DbError::Kv(KvError::KeyNotFound)) => Linked::new_empty(),
                Err(e) => return Err(e),
            };
            dst_ll.add_first(value.clone());
            persist_list(tx, &dst_public, &dst_prefix, &dst_ll).await?;
            let result: DbResult = Box::new(Some(value));
            Ok(result)
        })
    }
}

struct LPosOp {
    public_key: Vec<u8>,
    node_prefix: Vec<u8>,
    element: Vec<u8>,
}

impl DbOp for LPosOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let public_key = self.public_key.clone();
        let node_prefix = self.node_prefix.clone();
        let element = self.element.clone();
        Box::pin(async move {
            let node_prefix = resolve_prefix(tx, &public_key, &node_prefix).await?;
            // Check key type first (WRONGTYPE on non-list).
            if let Ok(item) = tx.get(&public_key).await {
                if base_type(item.metadata()) != TYPE_LIST {
                    return Err(DbError::WrongType);
                }
            }
            let ll = match load_end(tx, &public_key, &node_prefix, false, 0).await {
                Ok(ll) => ll,
                Err(DbError::Kv(KvError::KeyNotFound)) => {
                    let result: DbResult = Box::new(None::<i64>);
                    return Ok(result);
                }
                Err(e) => return Err(e),
            };
            let pos = find_first(tx, &node_prefix, &ll, &element)
                .await?
                .map(|(pos, _)| pos as i64);
            let result: DbResult = Box::new(pos);
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

/// Replies null for `None` (LPOS nil), an integer otherwise.
struct NullableIntWire;

impl WireOp for NullableIntWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => {
                if let Ok(boxed) = res.downcast::<Option<i64>>() {
                    match *boxed {
                        Some(n) => RespValue::Integer(n),
                        None => RespValue::BulkString(None),
                    }
                } else {
                    internal_error()
                }
            }
            Err(e) => err_resp(&e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::test_session;
    use kv::kv::TxIsolation;

    /// Runs one op through its own transaction and commits if it mutates,
    /// then renders the reply (the unit-test equivalent of Go's
    /// `kvs.Update`/`kvs.Read` calls).
    async fn exec(session: &Session, op: QueuedOp) -> RespValue {
        let store = session.store();
        let tx = store
            .begin(op.is_mutating, TxIsolation::Snapshot)
            .await
            .expect("tx");
        let outcome = op.db_op.run(&*tx).await;
        if op.is_mutating {
            tx.commit().await.expect("commit");
        }
        op.wire_op.reply(outcome)
    }

    /// Loads the list stored under `key` for direct inspection.
    async fn load(session: &Session, key: &[u8]) -> Linked {
        let store = session.store();
        let tx = store
            .begin(false, TxIsolation::Snapshot)
            .await
            .expect("read tx");
        let public_key = session.public_key(key);
        let node_prefix = resolve_prefix(&*tx, &public_key, &session.private_key(key))
            .await
            .expect("prefix");
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

    /// Mixed head/tail ops (which load only the list ends and write only
    /// dirty nodes) must leave the stored chain identical to a model deque.
    #[tokio::test]
    async fn end_ops_match_model_and_keep_chain_consistent() {
        use std::collections::VecDeque;
        let s = test_session();
        let mut model: VecDeque<Vec<u8>> = VecDeque::new();
        let mut seed = 12345u64;
        for i in 0..400u32 {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let v = format!("v{}", i % 7).into_bytes();
            match (seed >> 33) % 6 {
                0 => {
                    exec(&s, lpush(&s, b"m", &[Bytes::from(v.clone())])).await;
                    model.push_front(v);
                }
                1 => {
                    exec(&s, rpush(&s, b"m", &[Bytes::from(v.clone())])).await;
                    model.push_back(v);
                }
                2 => {
                    let r = exec(&s, lpop(&s, b"m")).await;
                    let want = model.pop_front();
                    assert_eq!(expect_bulk(&[r]).map(|b| b.to_vec()), want);
                }
                3 => {
                    let r = exec(&s, rpop(&s, b"m")).await;
                    let want = model.pop_back();
                    assert_eq!(expect_bulk(&[r]).map(|b| b.to_vec()), want);
                }
                4 => {
                    exec(&s, rpoplpush(&s, b"m", b"m")).await;
                    if let Some(x) = model.pop_back() {
                        model.push_front(x);
                    }
                }
                _ => {
                    exec(&s, lrem(&s, b"m", 1, &v)).await;
                    if let Some(pos) = model.iter().position(|x| *x == v) {
                        model.remove(pos);
                    }
                }
            }
            if model.is_empty() {
                let r = exec(&s, llen(&s, b"m")).await;
                assert_eq!(expect_int(&[r]), 0);
            } else {
                let ll = load(&s, b"m").await;
                assert_eq!(values(&ll), Vec::from(model.clone()), "step {i}");
                assert_eq!(ll.size as usize, model.len());
                // The tail must be reachable by following prev links too.
                let mut back = Vec::new();
                let mut cur = ll.tail;
                while let Some(k) = cur {
                    let n = ll.nodes.get(&k).expect("node");
                    back.push(n.value.clone());
                    cur = n.prev;
                }
                back.reverse();
                assert_eq!(back, Vec::from(model.clone()), "step {i} (reverse)");
            }
        }
    }

    /// Asserts the stored chain equals `model` forwards and backwards.
    async fn assert_chain(s: &Session, model: &[Vec<u8>], ctx: &str) {
        if model.is_empty() {
            let r = exec(s, llen(s, b"w")).await;
            assert_eq!(expect_int(&[r]), 0, "{ctx}");
            return;
        }
        let ll = load(s, b"w").await;
        assert_eq!(values(&ll), model, "{ctx}");
        assert_eq!(ll.size as usize, model.len(), "{ctx}");
        let mut back = Vec::new();
        let mut cur = ll.tail;
        while let Some(k) = cur {
            let n = ll.nodes.get(&k).expect("node");
            back.push(n.value.clone());
            cur = n.prev;
        }
        back.reverse();
        assert_eq!(back, model, "{ctx} (reverse)");
    }

    /// Index/range/insert/rem/trim ops, which walk lazily from the nearer
    /// end, must agree with a plain `Vec` model and leave a consistent chain.
    #[tokio::test]
    async fn bounded_scan_ops_match_model() {
        let s = test_session();
        let mut model: Vec<Vec<u8>> = Vec::new();
        let mut seed = 987654321u64;
        let mut rnd = move |n: u64| {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (seed >> 33) % n
        };
        for i in 0..600u32 {
            let v = format!("v{}", rnd(6)).into_bytes();
            let len = model.len() as i64;
            let ctx = format!("step {i}");
            match rnd(9) {
                0 | 1 => {
                    exec(&s, rpush(&s, b"w", &[Bytes::from(v.clone())])).await;
                    model.push(v);
                }
                2 => {
                    exec(&s, lpush(&s, b"w", &[Bytes::from(v.clone())])).await;
                    model.insert(0, v);
                }
                3 => {
                    let idx = rnd(12) as i64 - 6;
                    let r = exec(&s, lindex(&s, b"w", idx)).await;
                    let j = if idx < 0 { idx + len } else { idx };
                    let want = (0..len).contains(&j).then(|| model[j as usize].clone());
                    assert_eq!(expect_bulk(&[r]).map(|b| b.to_vec()), want, "{ctx}");
                }
                4 => {
                    let (a, b) = (rnd(14) as i64 - 7, rnd(14) as i64 - 7);
                    let r = exec(&s, lrange(&s, b"w", a, b)).await;
                    let norm = |x: i64| if x < 0 { x + len } else { x };
                    let (a, b) = (norm(a).max(0), norm(b).min(len - 1));
                    let want: Vec<Vec<u8>> = if a > b || a >= len {
                        vec![]
                    } else {
                        model[a as usize..=b as usize].to_vec()
                    };
                    let got: Vec<Vec<u8>> =
                        expect_bulk_array(&[r]).iter().map(|b| b.to_vec()).collect();
                    assert_eq!(got, want, "{ctx}");
                }
                5 => {
                    let idx = rnd(12) as i64 - 6;
                    let r = exec(&s, lset(&s, b"w", idx, &v)).await;
                    let j = if idx < 0 { idx + len } else { idx };
                    if (0..len).contains(&j) {
                        model[j as usize] = v;
                        assert!(matches!(r, RespValue::SimpleString(_)), "{ctx}");
                    } else {
                        assert!(matches!(r, RespValue::Error(_)), "{ctx}");
                    }
                }
                6 => {
                    let count = rnd(7) as i64 - 3;
                    let r = exec(&s, lrem(&s, b"w", count, &v)).await;
                    let mut want = 0i64;
                    let limit = if count == 0 {
                        usize::MAX
                    } else {
                        count.unsigned_abs() as usize
                    };
                    let idxs: Vec<usize> = if count >= 0 {
                        (0..model.len()).collect()
                    } else {
                        (0..model.len()).rev().collect()
                    };
                    let mut drop = Vec::new();
                    for ix in idxs {
                        if (want as usize) < limit && model[ix] == v {
                            drop.push(ix);
                            want += 1;
                        }
                    }
                    drop.sort_unstable_by(|a, b| b.cmp(a));
                    for ix in drop {
                        model.remove(ix);
                    }
                    assert_eq!(expect_int(&[r]), want, "{ctx}");
                }
                7 => {
                    let before = rnd(2) == 0;
                    let pivot = format!("v{}", rnd(6)).into_bytes();
                    let r = exec(&s, linsert(&s, b"w", before, &pivot, &v)).await;
                    match model.iter().position(|x| *x == pivot) {
                        Some(p) => {
                            model.insert(if before { p } else { p + 1 }, v);
                            assert_eq!(expect_int(&[r]), model.len() as i64, "{ctx}");
                        }
                        None => assert_eq!(expect_int(&[r]), -1, "{ctx}"),
                    }
                }
                _ => {
                    if rnd(3) == 0 {
                        let (a, b) = (rnd(10) as i64 - 4, rnd(10) as i64 - 4);
                        exec(&s, ltrim(&s, b"w", a, b)).await;
                        let norm = |x: i64| if x < 0 { x + len } else { x };
                        let (a, b) = (norm(a).max(0), norm(b).min(len - 1));
                        model = if a > b || a >= len {
                            vec![]
                        } else {
                            model[a as usize..=b as usize].to_vec()
                        };
                    } else {
                        let r = exec(&s, lpos(&s, b"w", &v)).await;
                        let want = model.iter().position(|x| *x == v).map(|p| p as i64);
                        match (&r, want) {
                            (RespValue::Integer(n), Some(p)) => assert_eq!(*n, p, "{ctx}"),
                            (RespValue::BulkString(None), None) => {}
                            other => panic!("{ctx}: lpos mismatch {other:?}"),
                        }
                    }
                }
            }
            assert_chain(&s, &model, &ctx).await;
        }
    }

    /// Ops that only need the ends or a prefix of the list must not read the
    /// nodes in the middle: with a middle node's entry removed from the
    /// store they still succeed, while one that has to cross it fails.
    #[tokio::test]
    async fn bounded_ops_do_not_read_untouched_nodes() {
        let s = test_session();
        let items: Vec<Bytes> = (0..10).map(|i| Bytes::from(format!("e{i}"))).collect();
        exec(&s, rpush(&s, b"w", &items)).await;
        let ll = load(&s, b"w").await;
        let victim = ll.ordered_keys()[5]; // index 5 of 0..10: 5 steps from head, 4 from tail
        {
            let store = s.store();
            let tx = store.begin(true, TxIsolation::Snapshot).await.expect("tx");
            let public_key = s.public_key(b"w");
            let prefix = resolve_prefix(&*tx, &public_key, &s.private_key(b"w"))
                .await
                .expect("prefix");
            tx.delete(&node_key(&prefix, &victim)).expect("delete");
            tx.commit().await.expect("commit");
        }
        let r = exec(&s, lindex(&s, b"w", 0)).await;
        assert_eq!(expect_bulk(&[r]).as_deref(), Some(&b"e0"[..]));
        let r = exec(&s, lindex(&s, b"w", -1)).await;
        assert_eq!(expect_bulk(&[r]).as_deref(), Some(&b"e9"[..]));
        let r = exec(&s, lrange(&s, b"w", 0, 3)).await;
        assert_eq!(expect_bulk_array(&[r]).len(), 4);
        let r = exec(&s, lrange(&s, b"w", -3, -1)).await;
        assert_eq!(expect_bulk_array(&[r]).len(), 3);
        let r = exec(&s, lpos(&s, b"w", b"e2")).await;
        assert_eq!(r, RespValue::Integer(2));
        let r = exec(&s, lrem(&s, b"w", 1, b"e1")).await;
        assert_eq!(expect_int(&[r]), 1);
        let r = exec(&s, lset(&s, b"w", -1, b"last")).await;
        assert!(matches!(r, RespValue::SimpleString(_)));
        let r = exec(&s, linsert(&s, b"w", false, b"e0", b"x")).await;
        assert_eq!(expect_int(&[r]), 10);
        // Trimming to the outer nodes only needs to read the removed ends.
        exec(&s, ltrim(&s, b"w", 0, 3)).await;
        // Crossing the missing node is an error, not a silent empty reply.
        let s2 = test_session();
        exec(&s2, rpush(&s2, b"w", &items)).await;
        let ll2 = load(&s2, b"w").await;
        let victim2 = ll2.ordered_keys()[4];
        {
            let store = s2.store();
            let tx = store.begin(true, TxIsolation::Snapshot).await.expect("tx");
            let public_key = s2.public_key(b"w");
            let prefix = resolve_prefix(&*tx, &public_key, &s2.private_key(b"w"))
                .await
                .expect("prefix");
            tx.delete(&node_key(&prefix, &victim2)).expect("delete");
            tx.commit().await.expect("commit");
        }
        let r = exec(&s2, lindex(&s2, b"w", 4)).await;
        assert!(matches!(r, RespValue::Error(_)), "got {r:?}");
    }

    // --- namespaced layout (RFC 0001) ---

    fn bs(parts: &[&str]) -> Vec<Bytes> {
        parts
            .iter()
            .map(|p| Bytes::copy_from_slice(p.as_bytes()))
            .collect()
    }

    /// Runs the op on a legacy twin and on the namespaced one; replies must match.
    async fn both(
        legacy: &Session,
        ns: &Session,
        build: impl Fn(&Session) -> QueuedOp,
    ) -> RespValue {
        let a = crate::testutil::exec(legacy, build(legacy)).await;
        let b = crate::testutil::exec(ns, build(ns)).await;
        assert_eq!(
            a, b,
            "a namespaced list must answer exactly like a legacy one"
        );
        a
    }

    #[tokio::test]
    async fn namespaced_lists_behave_like_legacy_ones() {
        let (legacy, ns) = (test_session(), test_session());
        for s in [&legacy, &ns] {
            crate::testutil::exec(s, rpush(s, b"l", &bs(&["a", "b", "c", "d", "e"]))).await;
            crate::testutil::exec(s, rpush(s, b"m", &bs(&["x"]))).await;
        }
        // New lists are born namespaced; the twin is put back into the layout earlier releases wrote.
        crate::testutil::legacy_key(&legacy, b"l").await;
        crate::testutil::legacy_key(&legacy, b"m").await;
        let (ns_l, ns_m) = (
            crate::testutil::namespace_of(&ns, b"l").await,
            crate::testutil::namespace_of(&ns, b"m").await,
        );

        // Reads.
        both(&legacy, &ns, |s| llen(s, b"l")).await;
        both(&legacy, &ns, |s| lrange(s, b"l", 0, -1)).await;
        both(&legacy, &ns, |s| lrange(s, b"l", 1, 2)).await;
        both(&legacy, &ns, |s| lindex(s, b"l", -2)).await;
        both(&legacy, &ns, |s| lindex(s, b"l", 99)).await;
        both(&legacy, &ns, |s| lpos(s, b"l", b"d")).await;

        // Writes, each followed by the whole list.
        both(&legacy, &ns, |s| rpush(s, b"l", &bs(&["f", "g"]))).await;
        both(&legacy, &ns, |s| lpush(s, b"l", &bs(&["z"]))).await;
        both(&legacy, &ns, |s| lpushx(s, b"l", b"y")).await;
        both(&legacy, &ns, |s| rpushx(s, b"l", b"h")).await;
        both(&legacy, &ns, |s| lset(s, b"l", 2, b"B")).await;
        both(&legacy, &ns, |s| linsert(s, b"l", true, b"c", b"between")).await;
        both(&legacy, &ns, |s| lrem(s, b"l", 1, b"d")).await;
        both(&legacy, &ns, |s| lpop(s, b"l")).await;
        both(&legacy, &ns, |s| rpop(s, b"l")).await;
        both(&legacy, &ns, |s| lrange(s, b"l", 0, -1)).await;
        both(&legacy, &ns, |s| rpoplpush(s, b"l", b"m")).await; // namespaced -> namespaced
        both(&legacy, &ns, |s| rpoplpush(s, b"m", b"fresh")).await; // namespaced -> missing (legacy, new)
        both(&legacy, &ns, |s| rpoplpush(s, b"l", b"l")).await; // rotate in place
        both(&legacy, &ns, |s| ltrim(s, b"l", 1, 3)).await;
        for k in [&b"l"[..], b"m", b"fresh"] {
            both(&legacy, &ns, |s| lrange(s, k, 0, -1)).await;
        }

        // The namespaced lists never wrote anything under their name-derived prefix
        // and their sentinels still record the namespace; removed nodes are deleted.
        for k in [&b"l"[..], b"m"] {
            assert_eq!(
                crate::testutil::entries_under(&ns, &ns.private_key(k)).await,
                0,
                "{}",
                String::from_utf8_lossy(k)
            );
            let meta = crate::testutil::stored_type_byte(&ns, &ns.public_key(k))
                .await
                .unwrap();
            assert_eq!(meta, TYPE_LIST | crate::common::namespace::NAMESPACE_FLAG);
        }
        let live_l = match crate::testutil::exec(&ns, llen(&ns, b"l")).await {
            RespValue::Integer(n) => n as usize,
            o => panic!("{o:?}"),
        };
        let live_m = match crate::testutil::exec(&ns, llen(&ns, b"m")).await {
            RespValue::Integer(n) => n as usize,
            o => panic!("{o:?}"),
        };
        assert_eq!(crate::testutil::entries_under(&ns, &ns_l).await, live_l);
        assert_eq!(crate::testutil::entries_under(&ns, &ns_m).await, live_m);

        // Emptying a namespaced list removes its sentinel and every node.
        for _ in 0..live_l {
            both(&legacy, &ns, |s| lpop(s, b"l")).await;
        }
        assert_eq!(
            crate::testutil::stored_type_byte(&ns, &ns.public_key(b"l")).await,
            None
        );
        assert_eq!(crate::testutil::entries_under(&ns, &ns_l).await, 0);
    }

    /// Every command that can create a list gives it a namespace of its own.
    #[tokio::test]
    async fn every_command_that_creates_a_list_gives_it_a_namespace() {
        let s = test_session();
        crate::testutil::exec(&s, rpush(&s, b"src", &bs(&["1", "2"]))).await;
        crate::testutil::exec(&s, lpush(&s, b"by_lpush", &bs(&["a"]))).await;
        crate::testutil::exec(&s, rpush(&s, b"by_rpush", &bs(&["a", "b"]))).await;
        crate::testutil::exec(&s, rpoplpush(&s, b"src", b"by_rpoplpush")).await;
        // The *X variants only push onto an existing list, so they create nothing.
        crate::testutil::exec(&s, lpushx(&s, b"never_created", b"a")).await;
        assert_eq!(
            crate::testutil::stored_type_byte(&s, &s.public_key(b"never_created")).await,
            None
        );

        let mut seen = std::collections::HashSet::new();
        for key in ["src", "by_lpush", "by_rpush", "by_rpoplpush"] {
            let ns = crate::testutil::namespace_of(&s, key.as_bytes()).await;
            assert!(crate::testutil::entries_under(&s, &ns).await > 0, "{key}");
            assert_eq!(
                crate::testutil::entries_under(&s, &s.private_key(key.as_bytes())).await,
                0,
                "{key}: nothing under the name"
            );
            assert!(seen.insert(ns), "{key}: namespaces must be unique");
        }
    }

    #[tokio::test]
    async fn emptying_and_recreating_a_list_uses_a_new_namespace() {
        let s = test_session();
        crate::testutil::exec(&s, rpush(&s, b"l", &bs(&["a"]))).await;
        let first = crate::testutil::namespace_of(&s, b"l").await;
        crate::testutil::exec(&s, lpop(&s, b"l")).await;
        crate::testutil::exec(&s, rpush(&s, b"l", &bs(&["b"]))).await;
        assert_ne!(crate::testutil::namespace_of(&s, b"l").await, first);
        assert_eq!(
            crate::testutil::entries_under(&s, &first).await,
            0,
            "the emptied list left nothing behind"
        );
    }

    #[tokio::test]
    async fn a_namespaced_list_keeps_its_ttl() {
        let s = test_session();
        crate::testutil::exec(&s, rpush(&s, b"l", &bs(&["a"]))).await;
        crate::testutil::exec(&s, crate::keys::expire(&s, b"l", 100)).await;
        crate::testutil::exec(&s, rpush(&s, b"l", &bs(&["b"]))).await;
        let ttl = crate::testutil::exec(&s, crate::keys::ttl(&s, b"l")).await;
        assert!(
            matches!(ttl, RespValue::Integer(n) if (90..=100).contains(&n)),
            "ttl was {ttl:?}"
        );
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
        exec(&session, crate::strings::set(&session, key, b"hi", None)).await;

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
    async fn legacy_sentinel_layout_matches_go() {
        // A list in the layout earlier releases wrote: the sentinel has the
        // expected bytes (size 1, head key == tail key) and the node sits under
        // the private key derived from the name.
        let session = test_session();
        let key = b"mylist";
        exec(&session, rpush(&session, key, &[Bytes::from_static(b"v")])).await;
        crate::testutil::legacy_key(&session, key).await;

        let store = session.store();
        let tx = store
            .begin(false, TxIsolation::Snapshot)
            .await
            .expect("read tx");
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

    #[tokio::test]
    async fn new_lists_record_their_namespace_in_the_sentinel() {
        // A new list's sentinel has the same 36 payload bytes as before, then
        // the namespace trailer, and the node lives under that namespace.
        let session = test_session();
        let key = b"mylist";
        exec(&session, rpush(&session, key, &[Bytes::from_static(b"v")])).await;

        let store = session.store();
        let tx = store
            .begin(false, TxIsolation::Snapshot)
            .await
            .expect("read tx");
        let item = tx.get(&session.public_key(key)).await.expect("sentinel");
        assert_eq!(item.metadata(), TYPE_LIST | namespace::NAMESPACE_FLAG);
        let payload = namespace::payload(&item).expect("payload");
        assert_eq!(payload.len(), SENTINEL_LENGTH);
        assert_eq!(u32::from_be_bytes(payload[0..4].try_into().unwrap()), 1);
        let head_key = &payload[4..20];
        assert_eq!(head_key, &payload[20..36]);

        let ns = namespace::prefix_of(&item, &session.private_key(key)).expect("trailer");
        assert!(ns.is_explicit() && ns.starts_with(b"+0:"));
        let node_item = tx
            .get(&node_key(&ns, head_key))
            .await
            .expect("node under the namespace");
        assert_eq!(node_item.metadata(), TYPE_LIST);
        assert_eq!(&node_item.value()[..1], b"v");
        assert!(
            tx.get(&node_key(&session.private_key(key), head_key))
                .await
                .is_err(),
            "no node under the name-derived prefix"
        );
        drop(tx);
    }
}
