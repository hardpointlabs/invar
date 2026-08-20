//! RedisBloom-style bloom filter commands.
//!
//! Port of the Go `redis/bloom` package. A bloom filter key stores a small
//! encoding of its filter metadata under the public key and the actual bit
//! pages under private keys derived as `-<db>:<name>\x00bf:<filterID>:p:<page>`.
//!
//! The layout is a scaling, page-based filter:
//!
//! * The **meta** entry (public key, metadata `ValueType::Bloom`) holds the
//!   expansion rate, a NONSCALING flag, and one [`SubFilterMeta`] record per
//!   sub-filter (capacity, inserted count, error rate, hash count, bit count
//!   and the two FNV seeds). See [`encode_meta`]/[`decode_meta`] for the exact
//!   binary shape, which mirrors the Go on-disk format byte-for-byte.
//! * Each sub-filter's bits live in fixed 4096-byte **pages**, so setting a
//!   bit never rewrites the whole filter — only the pages that changed.
//! * When the newest sub-filter saturates (`inserted >= capacity`) and the
//!   filter is scaling, a fresh sub-filter with doubled capacity and halved
//!   error rate is appended; `BF.EXISTS` probes them newest-to-oldest.
//!
//! Every command returns a [`QueuedOp`] with a [`DbOp`] half that performs the
//! KV reads/writes inside the session-managed transaction and a [`WireOp`]
//! half that renders the result to RESP.

use std::collections::HashMap;

use bytes::Bytes;
use kv::kv::{BoxFuture, Entry, Error as KvError, Tx};

use crate::common::op::{err_resp, DbError, DbOp, DbResult, QueuedOp, WireOp};
use crate::common::session::Session;
use crate::common::ValueType;
use crate::resp::RespValue;

/// Byte size of a single bit page.
const PAGE_SIZE: usize = 4096;
/// Number of bits a single page can hold.
const PAGE_BITS: u64 = (PAGE_SIZE * 8) as u64;
/// Capacity and error rate used for auto-created filters (`BF.ADD` on a key
/// that does not exist).
const DEF_CAP: u64 = 100;
const DEF_ERR: f64 = 0.01;
/// Default expansion rate when `BF.RESERVE`/`BF.INSERT` omit it.
const DEF_EXP: u8 = 2;
/// Bytes of the meta header (flags + expansion + filter count).
const META_HEADER: usize = 4;
/// Bytes of a single [`SubFilterMeta`] record on disk.
const FILTER_META: usize = 60;
/// Upper bound on the number of hash functions per sub-filter.
const MAX_HASHES: u32 = 64;

/// Metadata type byte stamped on every bloom entry (public meta and private
/// pages), matching `ValueType::Bloom` in the Go `RedisValueType` enum.
const TYPE_BLOOM: u8 = ValueType::Bloom as u8;

/// Per-sub-filter parameters, persisted in the meta entry.
#[derive(Debug, Clone, PartialEq)]
struct SubFilterMeta {
    id: u64,
    capacity: u64,
    inserted: u64,
    error_rate: f64,
    num_hashes: u32,
    num_bits: u64,
    seed1: u64,
    seed2: u64,
}

/// The decoded meta entry: expansion policy plus one record per sub-filter.
#[derive(Debug, Clone, PartialEq)]
struct BloomMeta {
    expansion: u8,
    non_scaling: bool,
    filters: Vec<SubFilterMeta>,
}

/// Anything the wire side of a crashed `DbOp` is allowed to claim if the
/// result shape is unexpected (a "can't happen" guard).
fn internal_error() -> RespValue {
    RespValue::Error(Bytes::from_static(b"ERR internal error"))
}

// --- Pure helpers ---

/// FNV-1a 64-bit hash, matching Go's `hash/fnv` `New64a`.
fn fnv1a64(data: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET_BASIS;
    for &b in data {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// Hashes `data` seeded by `seed`, feeding the seed bytes (little-endian)
/// before the data — the same construction as Go's `bloomHash`.
fn bloom_hash(data: &[u8], seed: u64) -> u64 {
    let mut buf = Vec::with_capacity(8 + data.len());
    buf.extend_from_slice(&seed.to_le_bytes());
    buf.extend_from_slice(data);
    fnv1a64(&buf)
}

/// Computes the number of bits and hash functions for a given capacity and
/// target error rate using the classic formulas
/// `m = ceil(-n·ln(p) / ln(2)²)` and `k = round(m/n·ln 2)`.
fn compute_bloom_params(mut capacity: u64, mut error_rate: f64) -> (u64, u32) {
    if error_rate <= 0.0 {
        error_rate = DEF_ERR;
    }
    if capacity < 1 {
        capacity = 1;
    }
    let ln2 = std::f64::consts::LN_2;
    let num_bits = ((-(capacity as f64) * error_rate.ln()) / (ln2 * ln2))
        .ceil()
        .max(1.0) as u64;
    let num_hashes = ((num_bits as f64) / (capacity as f64) * ln2)
        .round()
        .max(1.0)
        .min(MAX_HASHES as f64) as u32;
    (num_bits, num_hashes)
}

/// Derives the two per-sub-filter seeds from its ID, as Go's `subFilterSeeds`.
fn sub_filter_seeds(filter_id: u64) -> (u64, u64) {
    let mut buf = [0u8; 16];
    buf[0..8].copy_from_slice(&filter_id.to_le_bytes());
    let seed1 = fnv1a64(&buf);
    buf[8..16].copy_from_slice(&1u64.to_le_bytes());
    let seed2 = fnv1a64(&buf);
    (seed1, seed2)
}

/// The error rate of sub-filter `index`: each generation halves the target
/// so the overall family meets the requested rate.
fn sub_filter_error_rate(target_rate: f64, index: u64) -> f64 {
    target_rate * 0.5_f64.powi((index + 1) as i32)
}

/// The capacity of sub-filter `index`: `base * expansion^index`.
fn sub_filter_capacity(base_cap: u64, expansion: i64, index: u64) -> u64 {
    base_cap * (expansion as f64).powi(index as i32) as u64
}

/// Builds a brand-new sub-filter's parameters for a given generation ID.
fn new_sub_filter(
    base_capacity: u64,
    error_rate: f64,
    expansion: i64,
    filter_id: u64,
) -> SubFilterMeta {
    let idx = filter_id;
    let cap = sub_filter_capacity(base_capacity, expansion, idx);
    let err_rate = sub_filter_error_rate(error_rate, idx);
    let (num_bits, num_hashes) = compute_bloom_params(cap, err_rate);
    let (s1, s2) = sub_filter_seeds(filter_id);
    SubFilterMeta {
        id: filter_id,
        capacity: cap,
        inserted: 0,
        error_rate: err_rate,
        num_hashes,
        num_bits,
        seed1: s1,
        seed2: s2,
    }
}

/// Builds the single sub-filter of a fresh filter. NONSCALING filters use the
/// requested error rate directly; scaling ones delegate to
/// [`new_sub_filter`] so the initial generation also halves the target.
fn new_initial_sub_filter(
    capacity: u64,
    error_rate: f64,
    expansion: i64,
    non_scaling: bool,
) -> SubFilterMeta {
    if non_scaling {
        let (num_bits, num_hashes) = compute_bloom_params(capacity, error_rate);
        let (s1, s2) = sub_filter_seeds(0);
        SubFilterMeta {
            id: 0,
            capacity,
            inserted: 0,
            error_rate,
            num_hashes,
            num_bits,
            seed1: s1,
            seed2: s2,
        }
    } else {
        new_sub_filter(capacity, error_rate, expansion, 0)
    }
}

/// Returns whether bit `idx` of `page` is set.
fn test_bit(page: &[u8], idx: u64) -> bool {
    page[(idx / 8) as usize] & (1u8 << (idx % 8)) != 0
}

/// Sets bit `idx` of `page`.
fn set_bit(page: &mut [u8], idx: u64) {
    page[(idx / 8) as usize] |= 1u8 << (idx % 8);
}

/// Computes the `num_hashes` bit positions for `item` using the
/// double-hashing trick `h1 + i·h2`, mirrored from Go.
fn hash_positions(item: &[u8], num_hashes: u32, num_bits: u64, seed1: u64, seed2: u64) -> Vec<u64> {
    let h1 = bloom_hash(item, seed1);
    let h2 = bloom_hash(item, seed2);
    (0..num_hashes)
        .map(|i| h1.wrapping_add((u64::from(i)).wrapping_mul(h2)) % num_bits)
        .collect()
}

/// Encodes a filter's meta into the on-disk shape shared with Go:
/// a 4-byte big-endian header (flags, expansion, filter count) followed by
/// one 60-byte little-endian record per sub-filter.
fn encode_meta(m: &BloomMeta) -> Vec<u8> {
    let n = m.filters.len().min(u16::MAX as usize);
    let mut buf = vec![0u8; META_HEADER + n * FILTER_META];
    if m.non_scaling {
        buf[0] |= 1;
    }
    buf[1] = m.expansion;
    buf[2..4].copy_from_slice(&(n as u16).to_be_bytes());
    for (i, f) in m.filters.iter().take(n).enumerate() {
        let off = META_HEADER + i * FILTER_META;
        buf[off..off + 8].copy_from_slice(&f.id.to_le_bytes());
        buf[off + 8..off + 16].copy_from_slice(&f.capacity.to_le_bytes());
        buf[off + 16..off + 24].copy_from_slice(&f.inserted.to_le_bytes());
        buf[off + 24..off + 32].copy_from_slice(&f.error_rate.to_le_bytes());
        buf[off + 32..off + 36].copy_from_slice(&f.num_hashes.to_le_bytes());
        buf[off + 36..off + 44].copy_from_slice(&f.num_bits.to_le_bytes());
        buf[off + 44..off + 52].copy_from_slice(&f.seed1.to_le_bytes());
        buf[off + 52..off + 60].copy_from_slice(&f.seed2.to_le_bytes());
    }
    buf
}

/// Decodes a meta blob, mirroring Go's `decodeBloomMeta`. Data too short for
/// the header or the declared filters yields `KeyNotFound` — the same sentinel
/// Go uses so a non-bloom or absent key is indistinguishable to callers.
fn decode_meta(data: &[u8]) -> Result<BloomMeta, KvError> {
    if data.len() < META_HEADER {
        return Err(KvError::KeyNotFound);
    }
    let non_scaling = data[0] & 1 != 0;
    let expansion = data[1];
    let n = u16::from_be_bytes([data[2], data[3]]) as usize;
    if data.len() < META_HEADER + n * FILTER_META {
        return Err(KvError::KeyNotFound);
    }
    let mut filters = Vec::with_capacity(n);
    for i in 0..n {
        let off = META_HEADER + i * FILTER_META;
        filters.push(SubFilterMeta {
            id: u64::from_le_bytes(data[off..off + 8].try_into().expect("slice in range")),
            capacity: u64::from_le_bytes(
                data[off + 8..off + 16].try_into().expect("slice in range"),
            ),
            inserted: u64::from_le_bytes(
                data[off + 16..off + 24].try_into().expect("slice in range"),
            ),
            error_rate: f64::from_le_bytes(
                data[off + 24..off + 32].try_into().expect("slice in range"),
            ),
            num_hashes: u32::from_le_bytes(
                data[off + 32..off + 36].try_into().expect("slice in range"),
            ),
            num_bits: u64::from_le_bytes(
                data[off + 36..off + 44].try_into().expect("slice in range"),
            ),
            seed1: u64::from_le_bytes(data[off + 44..off + 52].try_into().expect("slice in range")),
            seed2: u64::from_le_bytes(data[off + 52..off + 60].try_into().expect("slice in range")),
        });
    }
    Ok(BloomMeta {
        expansion,
        non_scaling,
        filters,
    })
}

// --- Key access helpers ---

/// Builds the `\x00bf:<filterID>:p:<page>` suffix appended to the key name,
/// mirroring Go's `bloomPageSuffix`. Production page keys are assembled by
/// [`page_key`] from the captured `-<db>:<name>` prefix; this Go-parity form
/// is kept for tests that pin the on-disk layout.
#[cfg(test)]
fn page_suffix(name: &[u8], filter_id: u64, page_num: u64) -> Vec<u8> {
    let mut s = Vec::with_capacity(name.len() + 32);
    s.extend_from_slice(name);
    s.extend_from_slice(b"\x00bf:");
    s.extend_from_slice(filter_id.to_string().as_bytes());
    s.extend_from_slice(b":p:");
    s.extend_from_slice(page_num.to_string().as_bytes());
    s
}

/// Builds the full private storage key of a page. `page_prefix` is the
/// session-derived `-<db>:<name>` prefix captured when the command was built.
fn page_key(page_prefix: &[u8], filter_id: u64, page_num: u64) -> Vec<u8> {
    page_prefix
        .iter()
        .chain(b"\x00bf:")
        .chain(filter_id.to_string().as_bytes())
        .chain(b":p:")
        .chain(page_num.to_string().as_bytes())
        .copied()
        .collect()
}

/// Reads and decodes the meta entry stored under the public key.
async fn read_meta(tx: &dyn Tx, meta_key: &[u8]) -> Result<BloomMeta, KvError> {
    let item = tx.get(meta_key).await?;
    decode_meta(item.value())
}

/// Reads the meta, mapping a missing key to `Ok(None)`.
async fn read_meta_or_nil(tx: &dyn Tx, meta_key: &[u8]) -> Result<Option<BloomMeta>, KvError> {
    match read_meta(tx, meta_key).await {
        Ok(m) => Ok(Some(m)),
        Err(KvError::KeyNotFound) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Persists the meta entry under the public key.
fn write_meta(tx: &dyn Tx, meta_key: &[u8], m: &BloomMeta) -> Result<(), KvError> {
    tx.set(Entry::new(meta_key.to_vec(), encode_meta(m)).metadata(TYPE_BLOOM))
}

/// Reads one fixed-size page, returning a fresh all-zero page when none has
/// been written yet.
async fn read_page(tx: &dyn Tx, page_key: &[u8]) -> Result<Vec<u8>, DbError> {
    match tx.get(page_key).await {
        Ok(item) => Ok(item.value().to_vec()),
        Err(KvError::KeyNotFound) => Ok(vec![0u8; PAGE_SIZE]),
        Err(e) => Err(e.into()),
    }
}

/// Persists one dirty page under its private key.
fn write_page(tx: &dyn Tx, page_key: &[u8], data: &[u8]) -> Result<(), DbError> {
    tx.set(Entry::new(page_key.to_vec(), data.to_vec()).metadata(TYPE_BLOOM))
        .map_err(DbError::from)
}

// --- Core add/exists logic (shared by BF.ADD, BF.MADD and BF.INSERT) ---

/// Adds a single item to the filter at `meta_key`, auto-creating the filter
/// with default parameters if it does not exist. Returns `1` if the item was
/// newly inserted and `0` if setting it was a no-op (all bits already set).
async fn add_one(
    tx: &dyn Tx,
    meta_key: &[u8],
    page_prefix: &[u8],
    item: &[u8],
) -> Result<i64, DbError> {
    let mut meta = match read_meta_or_nil(tx, meta_key).await? {
        Some(m) => m,
        None => {
            let filter = new_initial_sub_filter(DEF_CAP, DEF_ERR, i64::from(DEF_EXP), false);
            let m = BloomMeta {
                expansion: DEF_EXP,
                non_scaling: false,
                filters: vec![filter],
            };
            write_meta(tx, meta_key, &m)?;
            m
        }
    };

    {
        let needs_expand = {
            let last = meta.filters.last().expect("at least one filter");
            last.inserted >= last.capacity && !meta.non_scaling
        };
        if needs_expand {
            let last = meta.filters.last().expect("at least one filter");
            let new_id = last.id + 1;
            let new_f = new_sub_filter(
                last.capacity,
                last.error_rate * 2.0,
                i64::from(meta.expansion),
                new_id,
            );
            meta.filters.push(new_f);
        }
    }

    let filter = meta.filters.last().expect("at least one filter");
    let positions = hash_positions(
        item,
        filter.num_hashes,
        filter.num_bits,
        filter.seed1,
        filter.seed2,
    );
    let filter_id = filter.id;

    let mut pages: HashMap<u64, Vec<u8>> = HashMap::new();
    for &pos in &positions {
        let page_num = pos / PAGE_BITS;
        if !pages.contains_key(&page_num) {
            let key = page_key(page_prefix, filter_id, page_num);
            let data = read_page(tx, &key).await?;
            pages.entry(page_num).or_insert(data);
        }
    }

    let all_set = positions
        .iter()
        .all(|&pos| test_bit(&pages[&(pos / PAGE_BITS)], pos % PAGE_BITS));
    if all_set {
        return Ok(0);
    }

    for &pos in &positions {
        let page_num = pos / PAGE_BITS;
        let bit_off = pos % PAGE_BITS;
        let data = pages.get_mut(&page_num).expect("page loaded above");
        if !test_bit(data, bit_off) {
            set_bit(data, bit_off);
        }
    }

    for (page_num, data) in &pages {
        let key = page_key(page_prefix, filter_id, *page_num);
        write_page(tx, &key, data)?;
    }

    if let Some(last) = meta.filters.last_mut() {
        last.inserted += 1;
    }
    write_meta(tx, meta_key, &meta)?;

    Ok(1)
}

/// Checks whether `item` is possibly present in the filter at `meta_key`.
/// Probes sub-filters newest-to-oldest; a bit miss in the newest does not rule
/// out an older generation.
async fn exists_one(
    tx: &dyn Tx,
    meta_key: &[u8],
    page_prefix: &[u8],
    item: &[u8],
) -> Result<bool, DbError> {
    let meta = match read_meta_or_nil(tx, meta_key).await? {
        Some(m) => m,
        None => return Ok(false),
    };

    for f in meta.filters.iter().rev() {
        let positions = hash_positions(item, f.num_hashes, f.num_bits, f.seed1, f.seed2);
        let mut found = true;
        for &pos in &positions {
            let page_num = pos / PAGE_BITS;
            let key = page_key(page_prefix, f.id, page_num);
            let data = read_page(tx, &key).await?;
            if !test_bit(&data, pos % PAGE_BITS) {
                found = false;
                break;
            }
        }
        if found {
            return Ok(true);
        }
    }

    Ok(false)
}

// --- Info representation ---

/// The computed `BF.INFO` counters, indexed by key in the wire reply.
struct BloomInfo {
    capacity: u64,
    size: u64,
    num_filters: i64,
    num_inserted: u64,
    expansion: i64,
}

// --- Command functions ---

/// `BF.RESERVE key errorRate capacity [EXPANSION n] [NONSCALING]` — creates a
/// new filter, erroring if the key already holds anything.
pub fn reserve(
    session: &Session,
    key: &[u8],
    error_rate: f64,
    capacity: u64,
    expansion: i64,
    non_scaling: bool,
) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(ReserveOp {
            meta_key: session.public_key(key),
            error_rate,
            capacity,
            expansion,
            non_scaling,
        }),
        wire_op: Box::new(OkWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `BF.ADD key item` — adds an item, auto-creating the filter in default.
pub fn add(session: &Session, key: &[u8], item: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(AddOp {
            meta_key: session.public_key(key),
            page_prefix: session.private_key(key),
            item: item.to_vec(),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `BF.EXISTS key item` — reports whether an item is (possibly) present.
pub fn exists(session: &Session, key: &[u8], item: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(ExistsOp {
            meta_key: session.public_key(key),
            page_prefix: session.private_key(key),
            item: item.to_vec(),
        }),
        wire_op: Box::new(BoolIntWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `BF.MADD key item...` — adds many items, returning a 1/0 per item.
pub fn madd(session: &Session, key: &[u8], items: &[Bytes]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(MAddOp {
            meta_key: session.public_key(key),
            page_prefix: session.private_key(key),
            items: items.iter().map(|b| b.to_vec()).collect(),
        }),
        wire_op: Box::new(IntArrayWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `BF.MEXISTS key item...` — reports presence for many items.
pub fn mexists(session: &Session, key: &[u8], items: &[Bytes]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(MExistsOp {
            meta_key: session.public_key(key),
            page_prefix: session.private_key(key),
            items: items.iter().map(|b| b.to_vec()).collect(),
        }),
        wire_op: Box::new(IntArrayWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// Options parsed from `BF.INSERT`'s option list, mirroring Go's `InsertInfo`.
pub struct InsertInfo {
    pub capacity: u64,
    pub error: f64,
    pub expansion: i64,
    pub no_create: bool,
    pub non_scaling: bool,
    pub items: Vec<Bytes>,
}

/// `BF.INSERT key [CAPACITY n] [ERROR rate] [EXPANSION n] [NOCREATE]
/// [NONSCALING] [ITEMS item...]` — creates the filter if needed (unless
/// `NOCREATE`) and adds the given items.
pub fn insert(session: &Session, key: &[u8], info: InsertInfo) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(InsertOp {
            meta_key: session.public_key(key),
            page_prefix: session.private_key(key),
            info,
        }),
        wire_op: Box::new(IntArrayWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

/// `BF.INFO key` — reports filter statistics.
pub fn info(session: &Session, key: &[u8]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(InfoOp {
            meta_key: session.public_key(key),
        }),
        wire_op: Box::new(InfoWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

// --- DbOp halves ---

struct ReserveOp {
    meta_key: Vec<u8>,
    error_rate: f64,
    capacity: u64,
    expansion: i64,
    non_scaling: bool,
}

impl DbOp for ReserveOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let meta_key = self.meta_key.clone();
        let error_rate = self.error_rate;
        let capacity = self.capacity;
        let expansion = self.expansion;
        let non_scaling = self.non_scaling;
        Box::pin(async move {
            match tx.get(&meta_key).await {
                Ok(_) => Err(DbError::KeyExists),
                Err(KvError::KeyNotFound) => {
                    let exp = if expansion < 1 {
                        i64::from(DEF_EXP)
                    } else {
                        expansion
                    };
                    let filter = new_initial_sub_filter(capacity, error_rate, exp, non_scaling);
                    let meta = BloomMeta {
                        expansion: exp as u8,
                        non_scaling,
                        filters: vec![filter],
                    };
                    write_meta(tx, &meta_key, &meta)?;
                    let result: DbResult = Box::new(());
                    Ok(result)
                }
                Err(e) => Err(e.into()),
            }
        })
    }
}

struct AddOp {
    meta_key: Vec<u8>,
    page_prefix: Vec<u8>,
    item: Vec<u8>,
}

impl DbOp for AddOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let meta_key = self.meta_key.clone();
        let page_prefix = self.page_prefix.clone();
        let item = self.item.clone();
        Box::pin(async move {
            let added = add_one(tx, &meta_key, &page_prefix, &item).await?;
            let result: DbResult = Box::new(added);
            Ok(result)
        })
    }
}

struct ExistsOp {
    meta_key: Vec<u8>,
    page_prefix: Vec<u8>,
    item: Vec<u8>,
}

impl DbOp for ExistsOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let meta_key = self.meta_key.clone();
        let page_prefix = self.page_prefix.clone();
        let item = self.item.clone();
        Box::pin(async move {
            let present = exists_one(tx, &meta_key, &page_prefix, &item).await?;
            let result: DbResult = Box::new(present);
            Ok(result)
        })
    }
}

struct MAddOp {
    meta_key: Vec<u8>,
    page_prefix: Vec<u8>,
    items: Vec<Vec<u8>>,
}

impl DbOp for MAddOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let meta_key = self.meta_key.clone();
        let page_prefix = self.page_prefix.clone();
        let items = self.items.clone();
        Box::pin(async move {
            let mut results = Vec::with_capacity(items.len());
            for item in &items {
                results.push(add_one(tx, &meta_key, &page_prefix, item).await?);
            }
            let result: DbResult = Box::new(results);
            Ok(result)
        })
    }
}

struct MExistsOp {
    meta_key: Vec<u8>,
    page_prefix: Vec<u8>,
    items: Vec<Vec<u8>>,
}

impl DbOp for MExistsOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let meta_key = self.meta_key.clone();
        let page_prefix = self.page_prefix.clone();
        let items = self.items.clone();
        Box::pin(async move {
            let mut results = Vec::with_capacity(items.len());
            for item in &items {
                let present = exists_one(tx, &meta_key, &page_prefix, item).await?;
                results.push(if present { 1i64 } else { 0i64 });
            }
            let result: DbResult = Box::new(results);
            Ok(result)
        })
    }
}

struct InsertOp {
    meta_key: Vec<u8>,
    page_prefix: Vec<u8>,
    info: InsertInfo,
}

impl DbOp for InsertOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let meta_key = self.meta_key.clone();
        let page_prefix = self.page_prefix.clone();
        let info = InsertInfo {
            capacity: self.info.capacity,
            error: self.info.error,
            expansion: self.info.expansion,
            no_create: self.info.no_create,
            non_scaling: self.info.non_scaling,
            items: self.info.items.clone(),
        };
        Box::pin(async move {
            match read_meta_or_nil(tx, &meta_key).await? {
                Some(_) => {}
                None => {
                    if info.no_create {
                        return Err(DbError::Kv(KvError::KeyNotFound));
                    }
                    let cap = if info.capacity == 0 {
                        DEF_CAP
                    } else {
                        info.capacity
                    };
                    let err_rate = if info.error <= 0.0 {
                        DEF_ERR
                    } else {
                        info.error
                    };
                    let exp = if info.expansion < 1 {
                        i64::from(DEF_EXP)
                    } else {
                        info.expansion
                    };
                    let filter = new_initial_sub_filter(cap, err_rate, exp, info.non_scaling);
                    let m = BloomMeta {
                        expansion: exp as u8,
                        non_scaling: info.non_scaling,
                        filters: vec![filter],
                    };
                    write_meta(tx, &meta_key, &m)?;
                }
            }
            let mut results = Vec::with_capacity(info.items.len());
            for item in &info.items {
                results.push(add_one(tx, &meta_key, &page_prefix, item).await?);
            }
            let result: DbResult = Box::new(results);
            Ok(result)
        })
    }
}

struct InfoOp {
    meta_key: Vec<u8>,
}

impl DbOp for InfoOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let meta_key = self.meta_key.clone();
        Box::pin(async move {
            let meta = match read_meta_or_nil(tx, &meta_key).await? {
                Some(m) => m,
                None => return Err(DbError::Kv(KvError::KeyNotFound)),
            };
            let mut total_inserted = 0u64;
            let mut total_capacity = 0u64;
            let mut total_bits = 0u64;
            for f in &meta.filters {
                total_inserted += f.inserted;
                total_capacity += f.capacity;
                total_bits += f.num_bits;
            }
            let info = BloomInfo {
                capacity: total_capacity,
                size: total_bits,
                num_filters: meta.filters.len() as i64,
                num_inserted: total_inserted,
                expansion: i64::from(meta.expansion),
            };
            let result: DbResult = Box::new(info);
            Ok(result)
        })
    }
}

// --- WireOp halves ---

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

/// Replies an integer derived from a boolean result (`1`/`0`).
struct BoolIntWire;

impl WireOp for BoolIntWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<bool>() {
                Ok(value) => RespValue::Integer(i64::from(*value)),
                Err(_) => internal_error(),
            },
            Err(e) => err_resp(&e),
        }
    }
}

/// Replies an array of integers.
struct IntArrayWire;

impl WireOp for IntArrayWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<Vec<i64>>() {
                Ok(values) => RespValue::Array(Some(
                    values.iter().map(|v| RespValue::Integer(*v)).collect(),
                )),
                Err(_) => internal_error(),
            },
            Err(e) => err_resp(&e),
        }
    }
}

/// Replies the `BF.INFO` key/value pairs in the canonical order RedisBloom
/// documents: `Capacity`, `Size`, `Number of filters`, `Number of items
/// inserted`, `Expansion rate`.
struct InfoWire;

impl WireOp for InfoWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<BloomInfo>() {
                Ok(boxed) => {
                    let info = *boxed;
                    RespValue::Array(Some(vec![
                        RespValue::BulkString(Some(Bytes::from_static(b"Capacity"))),
                        RespValue::Integer(info.capacity as i64),
                        RespValue::BulkString(Some(Bytes::from_static(b"Size"))),
                        RespValue::Integer(info.size as i64),
                        RespValue::BulkString(Some(Bytes::from_static(b"Number of filters"))),
                        RespValue::Integer(info.num_filters),
                        RespValue::BulkString(Some(Bytes::from_static(
                            b"Number of items inserted",
                        ))),
                        RespValue::Integer(info.num_inserted as i64),
                        RespValue::BulkString(Some(Bytes::from_static(b"Expansion rate"))),
                        RespValue::Integer(info.expansion),
                    ]))
                }
                Err(_) => internal_error(),
            },
            Err(e) => err_resp(&e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::test_session;

    /// Runs a batch of ops through a single writable transaction and renders
    /// each reply (the unit-test equivalent of `kvs.Update` in the Go tests).
    async fn run_ops(session: &Session, ops: Vec<QueuedOp>) -> Vec<RespValue> {
        let store = session.store();
        let tx = store.begin(true).await.expect("write tx");
        let mut replies = Vec::with_capacity(ops.len());
        for op in ops {
            let outcome = op.db_op.run(&*tx).await;
            replies.push(op.wire_op.reply(outcome));
        }
        tx.commit().await.expect("commit");
        replies
    }

    /// Runs a read against a single transaction (Go's `kvs.Read`).
    async fn read<F, R>(session: &Session, f: F) -> R
    where
        F: for<'r> FnOnce(&'r dyn Tx) -> BoxFuture<'r, R>,
        R: Send + 'static,
    {
        let store = session.store();
        let tx = store.begin(false).await.expect("read tx");
        let value = f(&*tx).await;
        drop(tx);
        value
    }

    fn expect_int(replies: &[RespValue]) -> i64 {
        match &replies[0] {
            RespValue::Integer(n) => *n,
            other => panic!("expected integer, got {other:?}"),
        }
    }

    fn expect_int_array(replies: &[RespValue]) -> Vec<i64> {
        match &replies[0] {
            RespValue::Array(Some(items)) => items
                .iter()
                .map(|r| match r {
                    RespValue::Integer(n) => *n,
                    other => panic!("expected integer element, got {other:?}"),
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

    #[tokio::test]
    async fn reserve_creates_filter() {
        let session = test_session();
        run_ops(
            &session,
            vec![reserve(&session, b"mybloom", 0.01, 1000, 2, false)],
        )
        .await;

        let meta = read(&session, |tx| {
            let key = session.public_key(b"mybloom");
            Box::pin(async move { read_meta(tx, &key).await.expect("meta") })
        })
        .await;
        assert_eq!(meta.filters.len(), 1);
        assert_eq!(meta.filters[0].capacity, 1000);
        assert_eq!(meta.filters[0].inserted, 0);
        assert_eq!(meta.expansion, 2);
        assert!(!meta.non_scaling);
        assert_eq!(meta.filters[0].error_rate, 0.005);
    }

    #[tokio::test]
    async fn reserve_key_exists() {
        let session = test_session();
        run_ops(
            &session,
            vec![reserve(&session, b"dupbloom", 0.01, 100, 2, false)],
        )
        .await;
        let replies = run_ops(
            &session,
            vec![reserve(&session, b"dupbloom", 0.01, 100, 2, false)],
        )
        .await;
        assert_eq!(
            expect_error(&replies),
            Bytes::from_static(b"ERR key already exists")
        );
    }

    #[tokio::test]
    async fn add_then_exists() {
        let session = test_session();
        run_ops(
            &session,
            vec![reserve(&session, b"bf", 0.01, 1000, 2, false)],
        )
        .await;
        let replies = run_ops(&session, vec![add(&session, b"bf", b"hello")]).await;
        assert_eq!(expect_int(&replies), 1);
        let replies = run_ops(&session, vec![exists(&session, b"bf", b"hello")]).await;
        assert_eq!(expect_int(&replies), 1);
    }

    #[tokio::test]
    async fn add_duplicate_returns_zero() {
        let session = test_session();
        run_ops(
            &session,
            vec![reserve(&session, b"bf", 0.01, 1000, 2, false)],
        )
        .await;
        let first = run_ops(&session, vec![add(&session, b"bf", b"hello")]).await;
        assert_eq!(expect_int(&first), 1);
        let second = run_ops(&session, vec![add(&session, b"bf", b"hello")]).await;
        assert_eq!(expect_int(&second), 0);
    }

    #[tokio::test]
    async fn exists_nonexistent_and_missing_key() {
        let session = test_session();
        run_ops(
            &session,
            vec![reserve(&session, b"bf", 0.01, 1000, 2, false)],
        )
        .await;
        let replies = run_ops(&session, vec![exists(&session, b"bf", b"nope")]).await;
        assert_eq!(expect_int(&replies), 0);

        let replies = run_ops(&session, vec![exists(&session, b"nokey", b"x")]).await;
        assert_eq!(expect_int(&replies), 0);
    }

    #[tokio::test]
    async fn add_creates_default() {
        let session = test_session();
        let replies = run_ops(&session, vec![add(&session, b"auto", b"item")]).await;
        assert_eq!(expect_int(&replies), 1);

        let meta = read(&session, |tx| {
            let key = session.public_key(b"auto");
            Box::pin(async move { read_meta(tx, &key).await.expect("meta") })
        })
        .await;
        assert_eq!(meta.filters.len(), 1);
        assert_eq!(meta.filters[0].capacity, 100);
        assert_eq!(meta.expansion, 2);
    }

    #[tokio::test]
    async fn madd_adds_multiple() {
        let session = test_session();
        run_ops(
            &session,
            vec![reserve(&session, b"bf", 0.01, 1000, 2, false)],
        )
        .await;
        let items = vec![
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
            Bytes::from_static(b"c"),
        ];
        let replies = run_ops(&session, vec![madd(&session, b"bf", &items)]).await;
        assert_eq!(expect_int_array(&replies), vec![1, 1, 1]);
    }

    #[tokio::test]
    async fn mexists_checks_multiple() {
        let session = test_session();
        run_ops(
            &session,
            vec![reserve(&session, b"bf", 0.01, 1000, 2, false)],
        )
        .await;
        let adds = vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")];
        run_ops(&session, vec![madd(&session, b"bf", &adds)]).await;
        let probes = vec![
            Bytes::from_static(b"a"),
            Bytes::from_static(b"x"),
            Bytes::from_static(b"b"),
        ];
        let replies = run_ops(&session, vec![mexists(&session, b"bf", &probes)]).await;
        assert_eq!(expect_int_array(&replies), vec![1, 0, 1]);
    }

    #[tokio::test]
    async fn info_reports_counters() {
        let session = test_session();
        run_ops(
            &session,
            vec![reserve(&session, b"bf", 0.01, 1000, 2, false)],
        )
        .await;
        let adds = vec![
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
            Bytes::from_static(b"c"),
        ];
        run_ops(&session, vec![madd(&session, b"bf", &adds)]).await;

        let replies = run_ops(&session, vec![info(&session, b"bf")]).await;
        let mut fields = std::collections::HashMap::new();
        match &replies[0] {
            RespValue::Array(Some(items)) => {
                for pair in items.chunks(2) {
                    let key = match &pair[0] {
                        RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
                        other => panic!("expected bulk key, got {other:?}"),
                    };
                    let value = match &pair[1] {
                        RespValue::Integer(n) => *n,
                        other => panic!("expected integer value, got {other:?}"),
                    };
                    fields.insert(key, value);
                }
            }
            other => panic!("expected array, got {other:?}"),
        }
        assert_eq!(fields["Number of items inserted"], 3);
        assert_eq!(fields["Number of filters"], 1);
        assert_eq!(fields["Capacity"], 1000);
        assert_eq!(fields["Expansion rate"], 2);
        assert!(fields["Size"] > 0);
    }

    #[tokio::test]
    async fn info_nonexistent_is_error() {
        let session = test_session();
        let replies = run_ops(&session, vec![info(&session, b"nokey")]).await;
        assert_eq!(
            expect_error(&replies),
            Bytes::from_static(b"ERR Key not found")
        );
    }

    #[tokio::test]
    async fn insert_creates_and_adds() {
        let session = test_session();
        let items = vec![Bytes::from_static(b"x"), Bytes::from_static(b"y")];
        let op = insert(
            &session,
            b"bf",
            InsertInfo {
                capacity: 500,
                error: 0.01,
                expansion: 2,
                no_create: false,
                non_scaling: false,
                items,
            },
        );
        let replies = run_ops(&session, vec![op]).await;
        assert_eq!(expect_int_array(&replies), vec![1, 1]);

        let meta = read(&session, |tx| {
            let key = session.public_key(b"bf");
            Box::pin(async move { read_meta(tx, &key).await.expect("meta") })
        })
        .await;
        assert_eq!(meta.filters[0].capacity, 500);
    }

    #[tokio::test]
    async fn insert_nocreate_is_error() {
        let session = test_session();
        let items = vec![Bytes::from_static(b"x")];
        let op = insert(
            &session,
            b"bf",
            InsertInfo {
                capacity: 0,
                error: 0.0,
                expansion: 0,
                no_create: true,
                non_scaling: false,
                items,
            },
        );
        let replies = run_ops(&session, vec![op]).await;
        assert_eq!(
            expect_error(&replies),
            Bytes::from_static(b"ERR Key not found")
        );
    }

    #[tokio::test]
    async fn scaling_keeps_old_items() {
        let session = test_session();
        run_ops(&session, vec![reserve(&session, b"bf", 0.01, 5, 2, false)]).await;

        // Add 20 items — enough to overflow the tiny initial filter.
        let store = session.store();
        let tx = store.begin(true).await.expect("write tx");
        let meta_key = session.public_key(b"bf");
        let page_prefix = session.private_key(b"bf");
        for i in 0u8..20 {
            add_one(&*tx, &meta_key, &page_prefix, &[i])
                .await
                .expect("add");
        }
        tx.commit().await.expect("commit");

        let meta = read_bloom_meta(&session, b"bf").await;
        assert!(
            meta.filters.len() >= 2,
            "expected growth, got {}",
            meta.filters.len()
        );

        for i in [0u8, 5, 10, 15] {
            let present = read(&session, |tx| {
                let meta_key = session.public_key(b"bf");
                let page_prefix = session.private_key(b"bf");
                let item = [i];
                Box::pin(async move {
                    exists_one(tx, &meta_key, &page_prefix, &item)
                        .await
                        .expect("exists")
                })
            })
            .await;
            assert!(present, "item {i} disappeared after scaling");
        }
    }

    #[tokio::test]
    async fn non_scaling_never_grows() {
        let session = test_session();
        run_ops(&session, vec![reserve(&session, b"bf", 0.01, 5, 2, true)]).await;

        let store = session.store();
        let tx = store.begin(true).await.expect("write tx");
        let meta_key = session.public_key(b"bf");
        let page_prefix = session.private_key(b"bf");
        for i in 0u8..100 {
            add_one(&*tx, &meta_key, &page_prefix, &[i])
                .await
                .expect("add");
        }
        tx.commit().await.expect("commit");

        let meta = read_bloom_meta(&session, b"bf").await;
        assert_eq!(meta.filters.len(), 1);
    }

    #[tokio::test]
    async fn add_overwrites_non_bloom_key() {
        // A plain string stored under the same key is undecodable as bloom
        // meta, so `add_one` treats it as absent and overwrites it with a
        // fresh filter — mirroring the Go behavior exactly.
        let session = test_session();
        run_ops(&session, vec![crate::strings::set(&session, b"k", b"hi")]).await;
        let replies = run_ops(&session, vec![add(&session, b"k", b"item")]).await;
        assert_eq!(expect_int(&replies), 1);
    }

    #[test]
    fn compute_params_ranges() {
        for (cap, rate) in [(100u64, 0.01f64), (1000, 0.01), (1_000_000, 0.01)] {
            let (bits, hashes) = compute_bloom_params(cap, rate);
            assert!(bits >= 1);
            assert!((1..=MAX_HASHES).contains(&hashes));
        }
    }

    #[test]
    fn sub_filter_error_rate_halves() {
        assert!((sub_filter_error_rate(0.01, 0) - 0.005).abs() < 1e-12);
        assert!((sub_filter_error_rate(0.01, 1) - 0.0025).abs() < 1e-12);
        assert!((sub_filter_error_rate(0.01, 2) - 0.00125).abs() < 1e-12);
    }

    #[test]
    fn sub_filter_capacity_grows_by_expansion() {
        assert_eq!(sub_filter_capacity(1000, 2, 0), 1000);
        assert_eq!(sub_filter_capacity(1000, 2, 1), 2000);
        assert_eq!(sub_filter_capacity(1000, 2, 2), 4000);
    }

    #[test]
    fn hash_is_deterministic() {
        assert_eq!(bloom_hash(b"hello", 42), bloom_hash(b"hello", 42));
        assert_ne!(bloom_hash(b"hello", 42), bloom_hash(b"world", 42));
        assert_ne!(bloom_hash(b"hello", 42), bloom_hash(b"hello", 43));
    }

    #[test]
    fn hash_positions_in_bounds() {
        let positions = hash_positions(b"hello", 10, 1000, 42, 99);
        assert_eq!(positions.len(), 10);
        for pos in positions {
            assert!(pos < 1000);
        }
    }

    #[test]
    fn meta_round_trip() {
        let m = BloomMeta {
            expansion: 3,
            non_scaling: true,
            filters: vec![
                SubFilterMeta {
                    id: 0,
                    capacity: 100,
                    inserted: 50,
                    error_rate: 0.01,
                    num_hashes: 7,
                    num_bits: 1000,
                    seed1: 123,
                    seed2: 456,
                },
                SubFilterMeta {
                    id: 1,
                    capacity: 200,
                    inserted: 10,
                    error_rate: 0.005,
                    num_hashes: 8,
                    num_bits: 2000,
                    seed1: 789,
                    seed2: 101112,
                },
            ],
        };
        let data = encode_meta(&m);
        assert_eq!(decode_meta(&data).unwrap(), m);

        // Decode never panics on garbage and yields KeyNotFound for short data.
        assert_eq!(decode_meta(&[]), Err(KvError::KeyNotFound));
        assert_eq!(
            decode_meta(&[0, 0, 0, 0]),
            Ok(BloomMeta {
                expansion: 0,
                non_scaling: false,
                filters: Vec::new(),
            })
        );
        for seed in [&vec![1u8, 2, 3, 4][..], &vec![0u8; 100][..]] {
            let _ = decode_meta(seed);
        }
    }

    #[test]
    fn page_keys_match_go_layout() {
        let session = test_session();
        let suffix = page_suffix(b"mybloom", 0, 0);
        assert_eq!(
            session.private_key(&suffix),
            b"-0:mybloom\x00bf:0:p:0".to_vec()
        );

        let suffix2 = page_suffix(b"mybloom", 3, 42);
        assert_eq!(
            session.private_key(&suffix2),
            b"-0:mybloom\x00bf:3:p:42".to_vec()
        );

        // The DbOp-built key and the Go-style private_key(suffix) agree.
        let page_prefix = session.private_key(b"mybloom");
        assert_eq!(page_key(&page_prefix, 3, 42), session.private_key(&suffix2));
    }

    #[test]
    fn sub_filter_seeds_are_nonzero() {
        for id in [0u64, 1, 1 << 63, u64::MAX] {
            let (s1, s2) = sub_filter_seeds(id);
            assert!(s1 != 0 && s2 != 0, "zero seed for id {id}");
        }
    }

    /// Deterministic pseudo-random bytes (xorshift64), no rand dependency.
    fn pseudo_random_items(n: usize) -> Vec<Vec<u8>> {
        let mut seed = 0x1234_5678_9abc_def0u64;
        let mut item = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let len = 1 + (seed % 24) as usize;
            let len = len.min(24);
            let mut v = Vec::with_capacity(len);
            for _ in 0..len {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                v.push((seed & 0xff) as u8);
            }
            v
        };
        (0..n).map(|_| item()).collect()
    }

    #[tokio::test]
    async fn added_items_never_false_negative() {
        let session = test_session();
        run_ops(
            &session,
            vec![reserve(&session, b"bf", 0.01, 1000, 2, false)],
        )
        .await;
        let items = pseudo_random_items(40);

        let store = session.store();
        let tx = store.begin(true).await.expect("write tx");
        let meta_key = session.public_key(b"bf");
        let page_prefix = session.private_key(b"bf");
        // First add per item is new (1); a repeat in the same session must be a
        // no-op (0) — mirroring Go's duplicate property.
        for (i, item) in items.iter().enumerate() {
            let r = add_one(&*tx, &meta_key, &page_prefix, item)
                .await
                .expect("add");
            assert_eq!(r, 1, "first add of item {i} must be new");
        }
        for item in &items {
            let r = add_one(&*tx, &meta_key, &page_prefix, item)
                .await
                .expect("add");
            assert_eq!(r, 0, "repeat add must be a no-op");
        }
        tx.commit().await.expect("commit");

        let read_tx = store.begin(false).await.expect("read tx");
        for item in &items {
            let present = exists_one(&*read_tx, &meta_key, &page_prefix, item)
                .await
                .expect("exists");
            assert!(present, "false negative for {item:?}");
        }
        drop(read_tx);
    }

    #[tokio::test]
    async fn false_positive_rate_within_bounds() {
        let session = test_session();
        run_ops(
            &session,
            vec![reserve(&session, b"bf", 0.001, 5000, 2, false)],
        )
        .await;

        let n = 1000usize;
        let store = session.store();
        let meta_key = session.public_key(b"bf");
        let page_prefix = session.private_key(b"bf");
        let item = |i: usize| vec![(i >> 8) as u8, i as u8, (i & 0xff) as u8];
        {
            let tx = store.begin(true).await.expect("write tx");
            for i in 0..n {
                add_one(&*tx, &meta_key, &page_prefix, &item(i))
                    .await
                    .expect("add");
            }
            tx.commit().await.expect("commit");
        }

        let tx = store.begin(false).await.expect("read tx");
        let mut false_positives = 0usize;
        for i in n..n * 2 {
            if exists_one(&*tx, &meta_key, &page_prefix, &item(i))
                .await
                .expect("exists")
            {
                false_positives += 1;
            }
        }
        let mut missed = 0usize;
        for i in 0..n {
            if !exists_one(&*tx, &meta_key, &page_prefix, &item(i))
                .await
                .expect("exists")
            {
                missed += 1;
            }
        }
        drop(tx);

        assert_eq!(missed, 0, "false negatives");
        let fp_rate = false_positives as f64 / n as f64;
        assert!(fp_rate < 0.05, "false positive rate too high: {fp_rate}");
    }

    async fn read_bloom_meta(session: &Session, key: &[u8]) -> BloomMeta {
        read(session, |tx| {
            let meta_key = session.public_key(key);
            Box::pin(async move { read_meta(tx, &meta_key).await.expect("meta") })
        })
        .await
    }
}
