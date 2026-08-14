//! Redis HyperLogLog commands: `PFADD`, `PFCOUNT`, and `PFMERGE`.
//!
//! Port of the Go `redis/hll` package. Each key stores a dense HyperLogLog
//! sketch — a 16-byte header (`HYLL`, encoding byte, cached-count/cache
//! validity fields) followed by 16384 packed 6-bit registers — under the
//! public key stamped with `ValueType::String` metadata, matching Go. On disk
//! the sketch is therefore indistinguishable from a plain string, so `TYPE`
//! reports `string` and the cached count lives inside the value buffer in a
//! way that is invisible to other commands.
//!
//! The HLL parameters mirror Redis (p=14, 6-bit registers, MurmurHash64A
//! seeded with `0xadc83b19`, the same tau/sigma estimation constants), and
//! the unrolled register-histogram loop below matches Redis for the exact
//! `HLL_REGISTERS == 16384 && HLL_BITS == 6` case so computed counts agree
//! with stock Redis.

use bytes::Bytes;
use kv::kv::{BoxFuture, Entry, Error as KvError, Tx};

use crate::common::op::{err_resp, DbError, DbOp, DbResult, QueuedOp, WireOp};
use crate::common::session::Session;
use crate::common::ValueType;
use crate::resp::RespValue;

/// Number of register-index bits (p), giving 2^14 registers.
const HLL_P: u32 = 14;
/// Number of hash bits used for the trailing-zero count (64 - p).
const HLL_Q: u32 = 64 - HLL_P;
/// The 2^14 dense registers.
const HLL_REGISTERS: usize = 1 << HLL_P;
/// Mask isolating the register index from a 64-bit hash.
const HLL_P_MASK: u64 = (HLL_REGISTERS - 1) as u64;
/// Bits per register in the dense packing.
const HLL_BITS: u32 = 6;
/// Maximum storable register value (2^6 - 1).
const HLL_REGISTER_MAX: u32 = (1 << HLL_BITS) - 1;
/// Bytes of the `HYLL` header preceding the registers.
const HLL_HDR_SIZE: usize = 16;
/// Total on-disk size of a dense sketch: header plus packed registers.
const HLL_DENSE_SIZE: usize =
    HLL_HDR_SIZE + (HLL_REGISTERS * HLL_BITS as usize).div_ceil(8);
/// The only encoding this port writes (dense).
const HLL_DENSE: u8 = 0;
/// The bias-correction constant for the estimator (the nearest f64 to
/// Redis's `0.721347520444481703680`).
const HLL_ALPHA_INF: f64 = 0.7213475204444817;

/// Metadata byte stamped on every stored sketch, matching the Go
/// `common.RedisString` value type.
const TYPE_STRING: u8 = ValueType::String as u8;

/// A brand-new empty dense sketch. The freshly zeroed buffer has a valid
/// cached count of 0 (cache-validity bit clear, little-endian count 0).
fn create_hll() -> Vec<u8> {
    let mut data = vec![0u8; HLL_DENSE_SIZE];
    data[0..4].copy_from_slice(b"HYLL");
    data[4] = HLL_DENSE;
    data
}

/// Whether `data` looks like a dense sketch this port understands: exactly
/// `HLL_DENSE_SIZE` bytes, a `HYLL` magic, and the dense encoding byte.
fn is_valid_hll(data: &[u8]) -> bool {
    data.len() == HLL_DENSE_SIZE && &data[0..4] == b"HYLL" && data[4] == HLL_DENSE
}

/// Marks the cached count in the header as stale.
fn hll_invalidate_cache(data: &mut [u8]) {
    data[15] |= 1 << 7;
}

/// Returns the cached count and whether it is valid. The high bit of byte 15
/// indicates staleness; a valid cache holds a little-endian u64 in bytes
/// 8..16.
fn hll_get_cached_count(data: &[u8]) -> (u64, bool) {
    if data[15] & (1 << 7) != 0 {
        return (0, false);
    }
    (
        u64::from_le_bytes(data[8..16].try_into().expect("fixed-size slice")),
        true,
    )
}

/// Stores a recomputed count in the header, clearing the count's most
/// significant bit (reserved as the cache-validity flag).
fn hll_set_cached_count(data: &mut [u8], count: u64) {
    let count = count & !(1 << 63);
    data[8..16].copy_from_slice(&count.to_le_bytes());
}

/// Reads register `regnum` from the packed 6-bits-per-register block,
/// mirroring Go's little-bit-order dense packing.
fn get_register(p: &[u8], regnum: usize) -> u8 {
    let b = regnum * HLL_BITS as usize / 8;
    let fb = (regnum * HLL_BITS as usize) & 7;
    let b0 = i32::from(p[b]) >> fb;
    if b + 1 >= p.len() {
        return (b0 & HLL_REGISTER_MAX as i32) as u8;
    }
    let fb8 = 8 - fb;
    ((b0 | (i32::from(p[b + 1]) << fb8)) & HLL_REGISTER_MAX as i32) as u8
}

/// Writes `val` into register `regnum` of the packed block.
fn set_register(p: &mut [u8], regnum: usize, val: u8) {
    let b = regnum * HLL_BITS as usize / 8;
    let fb = (regnum * HLL_BITS as usize) & 7;
    let v = i32::from(val);
    let mask1 = (HLL_REGISTER_MAX as i32) << fb;
    p[b] = ((i32::from(p[b]) & !mask1) | (v << fb)) as u8;
    if b + 1 >= p.len() {
        return;
    }
    let fb8 = 8 - fb;
    let mask2 = HLL_REGISTER_MAX as i32 >> fb8;
    p[b + 1] = ((i32::from(p[b + 1]) & !mask2) | (v >> fb8)) as u8;
}

/// MurmurHash64A, byte-for-byte identical to Go's and Redis's.
fn murmur_hash64a(data: &[u8], seed: u64) -> u64 {
    const M: u64 = 0xc6a4_a793_5bd1_e995;
    const R: u32 = 47;

    let mut h = seed ^ (data.len() as u64).wrapping_mul(M);
    let mut rest = data;
    while rest.len() >= 8 {
        let mut k = u64::from_le_bytes(rest[..8].try_into().expect("8-byte slice"));
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h ^= k;
        h = h.wrapping_mul(M);
        rest = &rest[8..];
    }

    for (i, &byte) in rest.iter().enumerate() {
        h ^= (byte as u64) << (8 * i);
    }
    if !rest.is_empty() {
        h = h.wrapping_mul(M);
    }

    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;
    h
}

/// Hashes `ele` into its register index and leader-of-zeros run length.
fn hll_pat_len(ele: &[u8]) -> (usize, u32) {
    let mut hash = murmur_hash64a(ele, 0xadc8_3b19);
    let index = (hash & HLL_P_MASK) as usize;
    hash >>= HLL_P;
    hash |= 1u64 << HLL_Q;
    let count = hash.trailing_zeros() + 1;
    (index, count)
}

/// Raises register `index` to `count` if it is larger, reporting the change.
fn hll_dense_set(registers: &mut [u8], index: usize, count: u8) -> bool {
    let oldcount = get_register(registers, index);
    if count > oldcount {
        set_register(registers, index, count);
        return true;
    }
    false
}

/// Adds `ele` to the register block, returning whether any register changed.
fn hll_dense_add(registers: &mut [u8], ele: &[u8]) -> bool {
    let (index, count) = hll_pat_len(ele);
    hll_dense_set(registers, index, count as u8)
}

/// The `σ(x)` series from Redis's HLL estimator.
fn hll_sigma(mut x: f64) -> f64 {
    if x == 1.0 {
        return f64::INFINITY;
    }
    let mut y = 1.0;
    let mut z = x;
    loop {
        x *= x;
        let z_prime = z;
        z += x * y;
        y += y;
        if z_prime == z {
            break;
        }
    }
    z
}

/// The `τ(x)` series from Redis's HLL estimator.
fn hll_tau(mut x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        return 0.0;
    }
    let mut y = 1.0;
    let mut z = 1.0 - x;
    loop {
        x = x.sqrt();
        let z_prime = z;
        y *= 0.5;
        z -= (1.0 - x).powi(2) * y;
        if z_prime == z {
            break;
        }
    }
    z / 3.0
}

/// Builds the register-value histogram, unrolled to match Redis for the
/// exact `HLL_REGISTERS == 16384 && HLL_BITS == 6` configuration.
fn hll_dense_reg_histo(registers: &[u8], reghisto: &mut [i64]) {
    let (mut h0, mut h1, mut h2, mut h3) = ([0i64; 64], [0i64; 64], [0i64; 64], [0i64; 64]);

    for j in 0..1024 {
        let r = &registers[j * 12..j * 12 + 12];

        let r0 = (i32::from(r[0])) & 63;
        let r1 = ((i32::from(r[0]) >> 6) | (i32::from(r[1]) << 2)) & 63;
        let r2 = ((i32::from(r[1]) >> 4) | (i32::from(r[2]) << 4)) & 63;
        let r3 = (i32::from(r[2]) >> 2) & 63;
        let r4 = (i32::from(r[3])) & 63;
        let r5 = ((i32::from(r[3]) >> 6) | (i32::from(r[4]) << 2)) & 63;
        let r6 = ((i32::from(r[4]) >> 4) | (i32::from(r[5]) << 4)) & 63;
        let r7 = (i32::from(r[5]) >> 2) & 63;
        let r8 = (i32::from(r[6])) & 63;
        let r9 = ((i32::from(r[6]) >> 6) | (i32::from(r[7]) << 2)) & 63;
        let r10 = ((i32::from(r[7]) >> 4) | (i32::from(r[8]) << 4)) & 63;
        let r11 = (i32::from(r[8]) >> 2) & 63;
        let r12 = (i32::from(r[9])) & 63;
        let r13 = ((i32::from(r[9]) >> 6) | (i32::from(r[10]) << 2)) & 63;
        let r14 = ((i32::from(r[10]) >> 4) | (i32::from(r[11]) << 4)) & 63;
        let r15 = (i32::from(r[11]) >> 2) & 63;

        h0[r0 as usize] += 1;
        h1[r1 as usize] += 1;
        h2[r2 as usize] += 1;
        h3[r3 as usize] += 1;
        h0[r4 as usize] += 1;
        h1[r5 as usize] += 1;
        h2[r6 as usize] += 1;
        h3[r7 as usize] += 1;
        h0[r8 as usize] += 1;
        h1[r9 as usize] += 1;
        h2[r10 as usize] += 1;
        h3[r11 as usize] += 1;
        h0[r12 as usize] += 1;
        h1[r13 as usize] += 1;
        h2[r14 as usize] += 1;
        h3[r15 as usize] += 1;
    }

    for j in 0..64 {
        reghisto[j] = h0[j] + h1[j] + h2[j] + h3[j];
    }
}

/// Estimates the cardinality of the sketch, caching the result in the
/// header. The cache is refreshed whenever it was stale.
fn hll_count(data: &mut [u8]) -> u64 {
    if let (cached, true) = hll_get_cached_count(data) {
        return cached;
    }

    let registers = &data[HLL_HDR_SIZE..];
    let m = HLL_REGISTERS as f64;
    let mut reghisto = [0i64; 64];

    hll_dense_reg_histo(registers, &mut reghisto);

    let mut z = m * hll_tau((m - reghisto[HLL_Q as usize + 1] as f64) / m);
    let mut j = HLL_Q as i64;
    while j >= 1 {
        z += reghisto[j as usize] as f64;
        z *= 0.5;
        j -= 1;
    }
    z += m * hll_sigma(reghisto[0] as f64 / m);

    let count = (HLL_ALPHA_INF * m * m / z).round() as u64;
    hll_set_cached_count(data, count);
    count
}

/// Unions the registers of `sources` into the raw register array `raw`.
fn hll_merge_to_raw(raw: &mut [u8], sources: &[&[u8]]) {
    for src in sources {
        let src_registers = &src[HLL_HDR_SIZE..];
        for (i, slot) in raw.iter_mut().enumerate() {
            let val = get_register(src_registers, i);
            if val > *slot {
                *slot = val;
            }
        }
    }
}

/// Expands a raw register array into a dense sketch, invalidating its cache.
fn hll_raw_to_dense(dest_dense: &mut [u8], raw: &[u8]) {
    let dest_registers = &mut dest_dense[HLL_HDR_SIZE..];
    for (i, &val) in raw.iter().enumerate() {
        set_register(dest_registers, i, val);
    }
    hll_invalidate_cache(dest_dense);
}

/// `PFADD key element [element ...]` — adds elements to the sketch, creating
/// the key on first use. Returns 1 if at least one register changed, 0
/// otherwise. A key whose value is not a valid sketch is left untouched and
/// yields 0, mirroring Go.
pub fn pfadd(session: &Session, key: &[u8], elements: &[Bytes]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(PfAddOp {
            key: session.public_key(key),
            elements: elements.iter().map(|e| e.to_vec()).collect(),
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
    }
}

/// `PFCOUNT key [key ...]` — estimates the cardinality of one or more
/// sketches, unioning multiple keys. Missing or invalid keys contribute
/// nothing.
pub fn pfcount(session: &Session, keys: &[Bytes]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(PfCountOp {
            keys: keys.iter().map(|k| session.public_key(k)).collect(),
        }),
        wire_op: Box::new(CountWire),
        is_mutating: false,
    }
}

/// `PFMERGE destkey sourcekey [sourcekey ...]` — unions the sources and
/// stores the result in `destkey`, overwriting whatever it held. Replies
/// `+OK`.
pub fn pfmerge(session: &Session, dest: &[u8], sources: &[Bytes]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(PfMergeOp {
            dest: session.public_key(dest),
            sources: sources.iter().map(|k| session.public_key(k)).collect(),
        }),
        wire_op: Box::new(OkWire),
        is_mutating: true,
    }
}

// --- DbOp halves ---

struct PfAddOp {
    key: Vec<u8>,
    elements: Vec<Vec<u8>>,
}

impl DbOp for PfAddOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let elements = self.elements.clone();
        Box::pin(async move {
            let mut hll_data = match tx.get(&key).await {
                Ok(item) => item.value().to_vec(),
                Err(KvError::KeyNotFound) => create_hll(),
                Err(e) => return Err(e.into()),
            };

            if !is_valid_hll(&hll_data) {
                let result: DbResult = Box::new(0i64);
                return Ok(result);
            }

            let registers = &mut hll_data[HLL_HDR_SIZE..];
            let mut updated = 0i64;
            for ele in &elements {
                if hll_dense_add(registers, ele) {
                    updated = 1;
                }
            }

            if updated == 1 {
                hll_invalidate_cache(&mut hll_data);
                tx.set(Entry::new(key, hll_data).metadata(TYPE_STRING))?;
            }

            let result: DbResult = Box::new(updated);
            Ok(result)
        })
    }
}

struct PfCountOp {
    keys: Vec<Vec<u8>>,
}

impl DbOp for PfCountOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let keys = self.keys.clone();
        Box::pin(async move {
            if keys.is_empty() {
                let result: DbResult = Box::new(0u64);
                return Ok(result);
            }

            if keys.len() == 1 {
                let val = match tx.get(&keys[0]).await {
                    Ok(item) => item.value().to_vec(),
                    Err(KvError::KeyNotFound) => {
                        let result: DbResult = Box::new(0u64);
                        return Ok(result);
                    }
                    Err(e) => return Err(e.into()),
                };
                if !is_valid_hll(&val) {
                    let result: DbResult = Box::new(0u64);
                    return Ok(result);
                }
                let mut val = val;
                let result: DbResult = Box::new(hll_count(&mut val));
                return Ok(result);
            }

            let mut valid = Vec::with_capacity(keys.len());
            for key in &keys {
                match tx.get(key).await {
                    Ok(item) => {
                        let val = item.value().to_vec();
                        if is_valid_hll(&val) {
                            valid.push(val);
                        }
                    }
                    Err(KvError::KeyNotFound) => {}
                    Err(e) => return Err(e.into()),
                }
            }

            let mut raw = vec![0u8; HLL_REGISTERS];
            let refs: Vec<&[u8]> = valid.iter().map(|v| v.as_slice()).collect();
            hll_merge_to_raw(&mut raw, &refs);

            let mut dense = create_hll();
            hll_raw_to_dense(&mut dense, &raw);
            let result: DbResult = Box::new(hll_count(&mut dense));
            Ok(result)
        })
    }
}

struct PfMergeOp {
    dest: Vec<u8>,
    sources: Vec<Vec<u8>>,
}

impl DbOp for PfMergeOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let dest = self.dest.clone();
        let sources = self.sources.clone();
        Box::pin(async move {
            let mut valid = Vec::with_capacity(sources.len());
            for key in &sources {
                match tx.get(key).await {
                    Ok(item) => {
                        let val = item.value().to_vec();
                        if is_valid_hll(&val) {
                            valid.push(val);
                        }
                    }
                    Err(KvError::KeyNotFound) => {}
                    Err(e) => return Err(e.into()),
                }
            }

            let mut raw = vec![0u8; HLL_REGISTERS];
            let refs: Vec<&[u8]> = valid.iter().map(|v| v.as_slice()).collect();
            hll_merge_to_raw(&mut raw, &refs);

            let mut dense = create_hll();
            hll_raw_to_dense(&mut dense, &raw);
            tx.set(Entry::new(dest, dense).metadata(TYPE_STRING))?;

            let result: DbResult = Box::new(());
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

/// Replies an integer result (`PFADD`'s 0/1).
struct IntWire;

impl WireOp for IntWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<i64>() {
                Ok(value) => RespValue::Integer(*value),
                Err(_) => RespValue::Error(Bytes::from_static(b"ERR internal error: bad int result")),
            },
            Err(e) => err_resp(&e),
        }
    }
}

/// Replies a `PFCOUNT` result: a u64 rendered as a signed integer.
struct CountWire;

impl WireOp for CountWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<u64>() {
                Ok(count) => RespValue::Integer(*count as i64),
                Err(_) => RespValue::Error(Bytes::from_static(b"ERR internal error: bad count result")),
            },
            Err(e) => err_resp(&e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::test_session;

    fn expect_int(reply: &RespValue) -> i64 {
        match reply {
            RespValue::Integer(n) => *n,
            other => panic!("expected integer, got {other:?}"),
        }
    }

    fn expect_ok(reply: &RespValue) {
        assert_eq!(*reply, RespValue::SimpleString(Bytes::from_static(b"OK")));
    }

    /// Runs one op through its own transaction, committing if it mutates, and
    /// renders the reply.
    async fn exec(session: &Session, op: QueuedOp) -> RespValue {
        let store = session.store();
        let tx = store.begin(op.is_mutating).await.expect("tx");
        let outcome = op.db_op.run(&*tx).await;
        if op.is_mutating {
            tx.commit().await.expect("commit");
        }
        op.wire_op.reply(outcome)
    }

    /// Reads the raw stored value at key (any metadata), or None if missing.
    async fn stored(session: &Session, key: &[u8]) -> Option<Vec<u8>> {
        let store = session.store();
        let tx = store.begin(false).await.expect("read tx");
        match tx.get(&session.public_key(key)).await {
            Ok(item) => Some(item.value().to_vec()),
            Err(KvError::KeyNotFound) => None,
            Err(e) => panic!("get failed: {e:?}"),
        }
    }

    #[test]
    fn create_hll_is_valid_and_right_size() {
        let h = create_hll();
        assert_eq!(h.len(), HLL_DENSE_SIZE);
        assert!(is_valid_hll(&h));
        assert_eq!(HLL_DENSE_SIZE, 12304);
    }

    #[test]
    fn is_valid_hll_never_panics_on_garbage() {
        let seeds: Vec<Vec<u8>> = vec![
            create_hll(),
            Vec::new(),
            vec![0x48, 0x59, 0x4c, 0x4c],
            vec![0u8; 0],
            {
                let mut a = vec![0u8; HLL_DENSE_SIZE];
                a[4] = 1;
                a
            },
            {
                let mut a = vec![0u8; HLL_DENSE_SIZE];
                a[0] = 0;
                a
            },
            vec![0u8; HLL_DENSE_SIZE + 1],
            vec![0u8; HLL_DENSE_SIZE - 1],
            vec![0x48, 0x59, 0x4c, 0x4c, 0x00],
        ];
        for seed in seeds {
            let _ = is_valid_hll(&seed);
        }
        assert!(!is_valid_hll(&[]));
        assert!(!is_valid_hll(&vec![0u8; HLL_DENSE_SIZE - 1]));
        // A zeroed buffer of the right size has the right magic/encoding.
        let mut zeroed = vec![0u8; HLL_DENSE_SIZE];
        zeroed[0..4].copy_from_slice(b"HYLL");
        assert!(is_valid_hll(&zeroed));
    }

    #[test]
    fn hll_count_empty_is_zero() {
        let mut h = create_hll();
        assert_eq!(hll_count(&mut h), 0);
    }

    #[test]
    fn hll_add_single_distinct() {
        let mut h = create_hll();
        assert!(hll_dense_add(&mut h[HLL_HDR_SIZE..], b"hello"));
        assert!(!hll_dense_add(&mut h[HLL_HDR_SIZE..], b"hello"));
        hll_invalidate_cache(&mut h);
        assert_eq!(hll_count(&mut h), 1);
    }

    #[test]
    fn hll_add_multiple() {
        let mut h = create_hll();
        let items: [&[u8]; 5] = [b"one", b"two", b"three", b"four", b"five"];
        for item in items {
            hll_dense_add(&mut h[HLL_HDR_SIZE..], item);
        }
        hll_invalidate_cache(&mut h);
        let c = hll_count(&mut h);
        assert!((3..=7).contains(&c), "expected count ~5, got {c}");
    }

    #[test]
    fn hll_add_different_unique() {
        let mut h = create_hll();
        assert!(hll_dense_add(&mut h[HLL_HDR_SIZE..], b"a"));
        assert!(hll_dense_add(&mut h[HLL_HDR_SIZE..], b"b"));
        assert!(!hll_dense_add(&mut h[HLL_HDR_SIZE..], b"a"));
    }

    #[test]
    fn hll_cache_field_roundtrip() {
        let mut h = create_hll();
        assert!(hll_get_cached_count(&h).1);
        assert_eq!(hll_get_cached_count(&h).0, 0);

        hll_invalidate_cache(&mut h);
        assert!(!hll_get_cached_count(&h).1);

        hll_set_cached_count(&mut h, 1234);
        assert_eq!(hll_get_cached_count(&h), (1234, true));
    }

    #[test]
    fn hll_count_recomputes_and_caches() {
        let mut h = create_hll();
        hll_dense_add(&mut h[HLL_HDR_SIZE..], b"hello");
        hll_invalidate_cache(&mut h);
        assert!(!hll_get_cached_count(&h).1);

        let c = hll_count(&mut h);
        assert_ne!(c, 0);
        let (cached, ok) = hll_get_cached_count(&h);
        assert!(ok);
        assert_eq!(cached, c);
        assert_eq!(hll_count(&mut h), c);
    }

    #[test]
    fn hll_merge_greater_or_equal() {
        let mut h1 = create_hll();
        let mut h2 = create_hll();
        hll_dense_add(&mut h1[HLL_HDR_SIZE..], b"a");
        hll_dense_add(&mut h1[HLL_HDR_SIZE..], b"b");
        hll_dense_add(&mut h2[HLL_HDR_SIZE..], b"c");
        hll_dense_add(&mut h2[HLL_HDR_SIZE..], b"d");
        hll_invalidate_cache(&mut h1);
        hll_invalidate_cache(&mut h2);
        let c1 = hll_count(&mut h1);
        let c2 = hll_count(&mut h2);

        let mut raw = vec![0u8; HLL_REGISTERS];
        let refs = [&h1[..], &h2[..]];
        hll_merge_to_raw(&mut raw, &refs);

        let mut dense = create_hll();
        hll_raw_to_dense(&mut dense, &raw);
        let c_merged = hll_count(&mut dense);

        assert!(c_merged >= c1 && c_merged >= c2);
        assert!(c_merged <= c1 + c2);
    }

    #[test]
    fn hll_merge_overlap_stays_close() {
        let mut h1 = create_hll();
        let mut h2 = create_hll();
        hll_dense_add(&mut h1[HLL_HDR_SIZE..], b"a");
        hll_dense_add(&mut h1[HLL_HDR_SIZE..], b"b");
        hll_dense_add(&mut h2[HLL_HDR_SIZE..], b"b");
        hll_dense_add(&mut h2[HLL_HDR_SIZE..], b"c");
        hll_invalidate_cache(&mut h1);
        hll_invalidate_cache(&mut h2);

        let mut raw = vec![0u8; HLL_REGISTERS];
        let refs = [&h1[..], &h2[..]];
        hll_merge_to_raw(&mut raw, &refs);
        let mut dense = create_hll();
        hll_raw_to_dense(&mut dense, &raw);
        let c_merged = hll_count(&mut dense);

        let mut separate = create_hll();
        hll_dense_add(&mut separate[HLL_HDR_SIZE..], b"a");
        hll_dense_add(&mut separate[HLL_HDR_SIZE..], b"b");
        hll_dense_add(&mut separate[HLL_HDR_SIZE..], b"c");
        hll_invalidate_cache(&mut separate);
        let c_expected = hll_count(&mut separate);

        let diff = i64::try_from(c_merged).unwrap() - i64::try_from(c_expected).unwrap();
        assert!(diff.abs() <= 2, "merged {c_merged} too far from {c_expected}");
    }

    #[test]
    fn murmur_hash_distinguishes_inputs() {
        let h = murmur_hash64a(b"", 0xadc8_3b19);
        assert_ne!(h, 0);
        let h2 = murmur_hash64a(b"hello", 0xadc8_3b19);
        assert_ne!(h2, 0);
        assert_ne!(h, h2);
    }

    #[test]
    fn hll_pat_len_in_bounds() {
        let (index, count) = hll_pat_len(b"hello");
        assert!(index < HLL_REGISTERS);
        assert!((1..=HLL_Q + 1).contains(&count));
    }

    #[test]
    fn registers_survive_roundtrip() {
        let mut h = create_hll();
        let registers = &mut h[HLL_HDR_SIZE..];
        for (i, val) in [63u8, 0, 1, 32, 8, 51, 60].iter().enumerate() {
            let idx = i * 1000 + 5;
            set_register(registers, idx, *val);
        }
        for (i, val) in [63u8, 0, 1, 32, 8, 51, 60].iter().enumerate() {
            let idx = i * 1000 + 5;
            assert_eq!(get_register(registers, idx), *val, "register {idx}");
        }
        // A register that touches the view's final byte.
        set_register(registers, HLL_REGISTERS - 1, 7);
        assert_eq!(get_register(registers, HLL_REGISTERS - 1), 7);
    }

    #[test]
    fn hll_many_elements_within_ten_percent() {
        let mut h = create_hll();
        let n = 1000u64;
        for i in 0..n {
            let ele = [(i >> 8) as u8, i as u8, 0];
            hll_dense_add(&mut h[HLL_HDR_SIZE..], &ele);
        }
        hll_invalidate_cache(&mut h);
        let c = hll_count(&mut h);
        let ratio = c as f64 / n as f64;
        assert!((0.9..=1.1).contains(&ratio), "expected ~{n}, got {c}");
    }

    /// Deterministic pseudo-random bytes (xorshift64), no rand dependency.
    fn pseudo_random_items(n: usize) -> Vec<Vec<u8>> {
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut item = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let len = 1 + (seed % 20) as usize;
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
    async fn pfadd_and_pfcount_roundtrip() {
        let session = test_session();
        let reply = exec(
            &session,
            pfadd(
                &session,
                b"hll_a",
                &[Bytes::from_static(b"hello"), Bytes::from_static(b"world")],
            ),
        )
        .await;
        assert_eq!(expect_int(&reply), 1);

        let reply = exec(&session, pfadd(&session, b"hll_a", &[Bytes::from_static(b"hello")])).await;
        assert_eq!(expect_int(&reply), 0);

        let reply = exec(&session, pfcount(&session, &[Bytes::from_static(b"hll_a")])).await;
        assert!(expect_int(&reply) > 0);

        let reply = exec(&session, pfcount(&session, &[Bytes::from_static(b"hll_empty")])).await;
        assert_eq!(expect_int(&reply), 0);

        let reply = exec(&session, pfcount(&session, &[Bytes::from_static(b"hll_nonexist")])).await;
        assert_eq!(expect_int(&reply), 0);
    }

    #[tokio::test]
    async fn pfadd_duplicate_detection_is_sticky() {
        let session = test_session();
        let items = pseudo_random_items(25);
        for (i, item) in items.iter().enumerate() {
            let first = exec(&session, pfadd(&session, b"hll", &[Bytes::copy_from_slice(item)])).await;
            assert_eq!(expect_int(&first), 1, "first add of item {i} must be new");
            let second = exec(&session, pfadd(&session, b"hll", &[Bytes::copy_from_slice(item)])).await;
            assert_eq!(expect_int(&second), 0, "repeat add must be a no-op");
        }
        let reply = exec(&session, pfcount(&session, &[Bytes::from_static(b"hll")])).await;
        let count = expect_int(&reply);
        assert!(count <= items.len() as i64 + 100);
    }

    #[tokio::test]
    async fn pfcount_multi_key_unions() {
        let session = test_session();
        exec(
            &session,
            pfadd(
                &session,
                b"hll_a",
                &[Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            ),
        )
        .await;
        exec(
            &session,
            pfadd(
                &session,
                b"hll_b",
                &[Bytes::from_static(b"c"), Bytes::from_static(b"d")],
            ),
        )
        .await;

        let reply = exec(
            &session,
            pfcount(
                &session,
                &[
                    Bytes::from_static(b"hll_a"),
                    Bytes::from_static(b"hll_b"),
                    Bytes::from_static(b"missing"),
                ],
            ),
        )
        .await;
        let count = expect_int(&reply);
        let a = expect_int(&exec(
            &session,
            pfcount(&session, &[Bytes::from_static(b"hll_a")]),
        )
        .await);
        assert!(count >= a, "multi-key count {count} below single {a}");
    }

    #[tokio::test]
    async fn pfmerge_stores_union() {
        let session = test_session();
        exec(
            &session,
            pfadd(&session, b"hll_a", &[Bytes::from_static(b"hello")]),
        )
        .await;
        exec(
            &session,
            pfadd(&session, b"hll_b", &[Bytes::from_static(b"world")]),
        )
        .await;

        let reply = exec(
            &session,
            pfmerge(
                &session,
                b"hll_merged",
                &[Bytes::from_static(b"hll_a"), Bytes::from_static(b"hll_b")],
            ),
        )
        .await;
        expect_ok(&reply);

        let reply = exec(&session, pfcount(&session, &[Bytes::from_static(b"hll_merged")])).await;
        assert_eq!(expect_int(&reply), 2);

        // The stored merged value must still be a valid sketch.
        assert!(is_valid_hll(&stored(&session, b"hll_merged").await.unwrap()));
    }

    #[tokio::test]
    async fn pfmerge_with_no_valid_sources_creates_empty() {
        let session = test_session();
        // A plain string under a source key is skipped, and the dest is
        // still created as an empty sketch.
        exec(&session, crate::strings::set(&session, b"notahll", b"plain")).await;
        let reply = exec(
            &session,
            pfmerge(&session, b"hll_merged", &[Bytes::from_static(b"missing")]),
        )
        .await;
        expect_ok(&reply);
        let reply = exec(
            &session,
            pfcount(&session, &[Bytes::from_static(b"hll_merged")]),
        )
        .await;
        assert_eq!(expect_int(&reply), 0);
    }

    #[tokio::test]
    async fn pfadd_on_invalid_key_leaves_it_untouched() {
        let session = test_session();
        exec(&session, crate::strings::set(&session, b"notahll", b"plain")).await;

        let reply = exec(&session, pfadd(&session, b"notahll", &[Bytes::from_static(b"x")])).await;
        assert_eq!(expect_int(&reply), 0);
        assert_eq!(stored(&session, b"notahll").await, Some(b"plain".to_vec()));

        let reply = exec(&session, pfcount(&session, &[Bytes::from_static(b"notahll")])).await;
        assert_eq!(expect_int(&reply), 0);
    }
}