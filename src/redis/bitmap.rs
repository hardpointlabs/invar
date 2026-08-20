//! Redis bitmap commands: `SETBIT`, `GETBIT`, `BITCOUNT`, `BITPOS` and
//! `BITOP`.
//!
//! Port of the Go `redis/bitmap` package. Bitmaps are stored as plain string
//! values (metadata `ValueType::String`) whose bytes are indexed MSB-first:
//! bit 0 is the most significant bit of byte 0. As in Go, the read paths do
//! **not** validate the metadata byte, so any stored value is treated as a
//! byte string.
//!
//! `BITOP` supports the standard `AND`/`OR`/`XOR`/`NOT` plus the
//! non-standard `DIFF`, `DIFF1`, `ANDOR` and `ONE` variants from Go.

use bytes::Bytes;
use kv::kv::{BoxFuture, Entry, Error as KvError, Tx};

use crate::common::op::{err_resp, DbError, DbOp, DbResult, QueuedOp, WireOp};
use crate::common::session::Session;
use crate::common::ValueType;
use crate::resp::RespValue;

/// The metadata byte stamped on every stored bitmap (a plain string).
const TYPE_STRING: u8 = ValueType::String as u8;

/// A `BITOP` operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitOpType {
    And,
    Or,
    Xor,
    Not,
    Diff,
    Diff1,
    AndOr,
    One,
}

/// Parses a `BITOP` operation name, case-insensitively.
pub fn parse_bit_op(op: &[u8]) -> Option<BitOpType> {
    match op {
        b if b.eq_ignore_ascii_case(b"AND") => Some(BitOpType::And),
        b if b.eq_ignore_ascii_case(b"OR") => Some(BitOpType::Or),
        b if b.eq_ignore_ascii_case(b"XOR") => Some(BitOpType::Xor),
        b if b.eq_ignore_ascii_case(b"NOT") => Some(BitOpType::Not),
        b if b.eq_ignore_ascii_case(b"DIFF") => Some(BitOpType::Diff),
        b if b.eq_ignore_ascii_case(b"DIFF1") => Some(BitOpType::Diff1),
        b if b.eq_ignore_ascii_case(b"ANDOR") => Some(BitOpType::AndOr),
        b if b.eq_ignore_ascii_case(b"ONE") => Some(BitOpType::One),
        _ => None,
    }
}

/// Scans `[start_bit, end_bit]` for the first bit equal to `bit`, returning
/// its absolute position or -1. `ignore_trailing_zero` is accepted for
/// parity with Go but never affects the result here.
fn bit_pos_in_range(data: &[u8], start_bit: i64, end_bit: i64, bit: i64) -> i64 {
    let mut byte_idx = start_bit / 8;
    let end_byte = end_bit / 8;
    while byte_idx <= end_byte && (byte_idx as usize) < data.len() {
        let b = data[byte_idx as usize];
        let bit_start = if byte_idx == start_bit / 8 {
            start_bit % 8
        } else {
            0
        };
        let bit_end = if byte_idx == end_bit / 8 {
            end_bit % 8
        } else {
            7
        };
        let mut bit_pos = bit_start;
        while bit_pos <= bit_end {
            let mask = 1u8 << (7 - bit_pos as u32);
            let is_set = (b & mask) != 0;
            if (bit == 1 && is_set) || (bit == 0 && !is_set) {
                return byte_idx * 8 + bit_pos;
            }
            bit_pos += 1;
        }
        byte_idx += 1;
    }
    -1
}

/// `SETBIT key offset value` — sets or clears the bit at `offset`, extending
/// the string with zero bytes as needed, and returns the previous bit.
pub fn set_bit(session: &Session, key: &[u8], offset: i64, value: i64) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(SetBitOp {
            key: session.public_key(key),
            offset,
            value,
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

struct SetBitOp {
    key: Vec<u8>,
    offset: i64,
    value: i64,
}

impl DbOp for SetBitOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let offset = self.offset;
        let value = self.value;
        Box::pin(async move {
            let byte_index = offset / 8;
            let bit_pos = (7 - offset % 8) as u32;
            let mask = 1u8 << bit_pos;

            let mut data = match tx.get(&key).await {
                Ok(item) => item.value().to_vec(),
                Err(KvError::KeyNotFound) => vec![0u8; (byte_index + 1) as usize],
                Err(e) => return Err(e.into()),
            };
            if (byte_index as usize) >= data.len() {
                let mut new_data = vec![0u8; (byte_index + 1) as usize];
                new_data[..data.len()].copy_from_slice(&data);
                data = new_data;
            }

            let old_bit = ((data[byte_index as usize] & mask) >> bit_pos) as i64;

            if value == 1 {
                data[byte_index as usize] |= mask;
            } else {
                data[byte_index as usize] &= !mask;
            }

            tx.set(Entry::new(key, data).metadata(TYPE_STRING))?;
            let result: DbResult = Box::new(old_bit);
            Ok(result)
        })
    }
}

/// `GETBIT key offset` — returns the bit at `offset`, or 0 if the key is
/// missing or the string is too short.
pub fn get_bit(session: &Session, key: &[u8], offset: i64) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(GetBitOp {
            key: session.public_key(key),
            offset,
        }),
        wire_op: Box::new(IntWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

struct GetBitOp {
    key: Vec<u8>,
    offset: i64,
}

impl DbOp for GetBitOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let offset = self.offset;
        Box::pin(async move {
            let data = match tx.get(&key).await {
                Ok(item) => item.value().to_vec(),
                Err(KvError::KeyNotFound) => {
                    let result: DbResult = Box::new(0i64);
                    return Ok(result);
                }
                Err(e) => return Err(e.into()),
            };
            let byte_index = (offset / 8) as usize;
            if byte_index >= data.len() {
                let result: DbResult = Box::new(0i64);
                return Ok(result);
            }
            let bit_pos = (7 - offset % 8) as u32;
            let result: DbResult =
                Box::new(if data[byte_index] & (1u8 << bit_pos) != 0 { 1i64 } else { 0i64 });
            Ok(result)
        })
    }
}

/// `BITCOUNT key [start [end [BYTE|BIT]]]` — counts the set bits in the whole
/// string or a byte/bit range.
#[allow(clippy::too_many_arguments)]
pub fn bit_count(
    session: &Session,
    key: &[u8],
    start_given: bool,
    _end_given: bool,
    start_val: i64,
    end_val: i64,
    use_bit: bool,
) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(BitCountOp {
            key: session.public_key(key),
            start_given,
            start_val,
            end_val,
            use_bit,
        }),
        wire_op: Box::new(IntWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

struct BitCountOp {
    key: Vec<u8>,
    start_given: bool,
    start_val: i64,
    end_val: i64,
    use_bit: bool,
}

impl DbOp for BitCountOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let start_given = self.start_given;
        let start_val = self.start_val;
        let end_val = self.end_val;
        let use_bit = self.use_bit;
        Box::pin(async move {
            let data = match tx.get(&key).await {
                Ok(item) => item.value().to_vec(),
                Err(KvError::KeyNotFound) => {
                    let result: DbResult = Box::new(0i64);
                    return Ok(result);
                }
                Err(e) => return Err(e.into()),
            };

            if !start_given {
                let result: DbResult =
                    Box::new(data.iter().map(|b| b.count_ones() as i64).sum::<i64>());
                return Ok(result);
            }

            let mut start = start_val;
            let mut end = end_val;
            if use_bit {
                let total_bits = (data.len() * 8) as i64;
                if start < 0 {
                    start += total_bits;
                }
                if end < 0 {
                    end += total_bits;
                }
                if start < 0 {
                    start = 0;
                }
                if end >= total_bits {
                    end = total_bits - 1;
                }
                if start > end || start >= total_bits {
                    let result: DbResult = Box::new(0i64);
                    return Ok(result);
                }
                let mut count = 0i64;
                for bit in start..=end {
                    if data[(bit / 8) as usize] & (1u8 << (7 - (bit % 8) as u32)) != 0 {
                        count += 1;
                    }
                }
                let result: DbResult = Box::new(count);
                return Ok(result);
            }

            if start < 0 {
                start += data.len() as i64;
            }
            if end < 0 {
                end += data.len() as i64;
            }
            if start < 0 {
                start = 0;
            }
            if end >= data.len() as i64 {
                end = data.len() as i64 - 1;
            }
            if start > end || start >= data.len() as i64 {
                let result: DbResult = Box::new(0i64);
                return Ok(result);
            }
            let mut count = 0i64;
            for i in start..=end {
                count += data[i as usize].count_ones() as i64;
            }
            let result: DbResult = Box::new(count);
            Ok(result)
        })
    }
}

/// `BITPOS key bit [start [end [BYTE|BIT]]]` — finds the first bit equal to
/// `bit` in the whole string or a byte/bit range.
#[allow(clippy::too_many_arguments)]
pub fn bit_pos(
    session: &Session,
    key: &[u8],
    bit: i64,
    start_given: bool,
    start_val: i64,
    end_val: i64,
    use_bit: bool,
) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(BitPosOp {
            key: session.public_key(key),
            bit,
            start_given,
            start_val,
            end_val,
            use_bit,
        }),
        wire_op: Box::new(IntWire),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

struct BitPosOp {
    key: Vec<u8>,
    bit: i64,
    start_given: bool,
    start_val: i64,
    end_val: i64,
    use_bit: bool,
}

impl DbOp for BitPosOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let key = self.key.clone();
        let bit = self.bit;
        let start_given = self.start_given;
        let start_val = self.start_val;
        let end_val = self.end_val;
        let use_bit = self.use_bit;
        Box::pin(async move {
            let data = match tx.get(&key).await {
                Ok(item) => item.value().to_vec(),
                Err(KvError::KeyNotFound) => {
                    let result: DbResult = Box::new(if bit == 0 { 0i64 } else { -1i64 });
                    return Ok(result);
                }
                Err(e) => return Err(e.into()),
            };

            if !start_given {
                let pos = bit_pos_in_range(&data, 0, (data.len() * 8) as i64 - 1, bit);
                let result: DbResult = Box::new(if pos >= 0 {
                    pos
                } else if bit == 0 {
                    (data.len() * 8) as i64
                } else {
                    -1
                });
                return Ok(result);
            }

            let mut start = start_val;
            let mut end = end_val;
            if use_bit {
                let total_bits = (data.len() * 8) as i64;
                if start < 0 {
                    start += total_bits;
                }
                if end < 0 {
                    end += total_bits;
                }
                if start < 0 {
                    start = 0;
                }
                if end >= total_bits {
                    end = total_bits - 1;
                }
                if start > end {
                    let result: DbResult = Box::new(-1i64);
                    return Ok(result);
                }
                let pos = bit_pos_in_range(&data, start, end, bit);
                let result: DbResult = Box::new(pos);
                return Ok(result);
            }

            if start < 0 {
                start += data.len() as i64;
            }
            if end < 0 {
                end += data.len() as i64;
            }
            if start < 0 {
                start = 0;
            }
            if end >= data.len() as i64 {
                end = data.len() as i64 - 1;
            }
            if start > end {
                let result: DbResult = Box::new(-1i64);
                return Ok(result);
            }
            let start_bit = start * 8;
            let mut end_bit = end * 8 + 7;
            if end_bit >= (data.len() * 8) as i64 {
                end_bit = (data.len() * 8) as i64 - 1;
            }
            let pos = bit_pos_in_range(&data, start_bit, end_bit, bit);
            let result: DbResult = Box::new(pos);
            Ok(result)
        })
    }
}

/// `BITOP op destkey key [key ...]` — applies a bitwise operation to the
/// source keys and stores the result in `destkey`, returning the length of
/// the longest source string.
pub fn bit_op(session: &Session, dest_key: &[u8], op: BitOpType, src_keys: &[&[u8]]) -> QueuedOp {
    QueuedOp {
        db_op: Box::new(BitOpOp {
            dest_key: session.public_key(dest_key),
            src_keys: src_keys.iter().map(|k| session.public_key(k)).collect(),
            op,
        }),
        wire_op: Box::new(IntWire),
        is_mutating: true,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

struct BitOpOp {
    dest_key: Vec<u8>,
    src_keys: Vec<Vec<u8>>,
    op: BitOpType,
}

impl DbOp for BitOpOp {
    fn run<'a>(&'a self, tx: &'a dyn Tx) -> BoxFuture<'a, Result<DbResult, DbError>> {
        let dest_key = self.dest_key.clone();
        let src_keys = self.src_keys.clone();
        let op = self.op;
        Box::pin(async move {
            let mut sources: Vec<Option<Vec<u8>>> = Vec::with_capacity(src_keys.len());
            for sk in &src_keys {
                match tx.get(sk).await {
                    Ok(item) => sources.push(Some(item.value().to_vec())),
                    Err(KvError::KeyNotFound) => sources.push(None),
                    Err(e) => return Err(e.into()),
                }
            }

            let mut max_len = 0usize;
            for s in sources.iter().flatten() {
                if s.len() > max_len {
                    max_len = s.len();
                }
            }

            let mut result = vec![0u8; max_len];
            match op {
                BitOpType::And => {
                    result.fill(0xFF);
                    for s in &sources {
                        match s {
                            None => {
                                result.fill(0);
                                break;
                            }
                            Some(s) => {
                                for j in 0..max_len {
                                    result[j] = if j < s.len() {
                                        result[j] & s[j]
                                    } else {
                                        0
                                    };
                                }
                            }
                        }
                    }
                }
                BitOpType::Or => {
                    for s in sources.iter().flatten() {
                        for j in 0..s.len() {
                            result[j] |= s[j];
                        }
                    }
                }
                BitOpType::Xor => {
                    for s in sources.iter().flatten() {
                        for j in 0..s.len() {
                            result[j] ^= s[j];
                        }
                    }
                }
                BitOpType::Not => {
                    match &sources[0] {
                        None => result.fill(0xFF),
                        Some(s) => {
                            for j in 0..max_len {
                                result[j] = !s[j];
                            }
                        }
                    }
                }
                BitOpType::Diff => {
                    if let Some(s) = &sources[0] {
                        let n = s.len().min(max_len);
                        result[..n].copy_from_slice(&s[..n]);
                    }
                    for s in sources.iter().skip(1).flatten() {
                        for j in 0..s.len() {
                            result[j] &= !s[j];
                        }
                    }
                }
                BitOpType::Diff1 => {
                    match &sources[0] {
                        Some(s) => {
                            for j in 0..max_len {
                                result[j] = if j < s.len() { !s[j] } else { 0xFF };
                            }
                        }
                        None => result.fill(0xFF),
                    }
                    let mut has_one = false;
                    for s in sources.iter().skip(1).flatten() {
                        has_one = true;
                        for j in 0..max_len {
                            result[j] = if j < s.len() { result[j] & s[j] } else { 0 };
                        }
                    }
                    if !has_one {
                        result.fill(0);
                    }
                }
                BitOpType::AndOr => {
                    if let Some(s) = &sources[0] {
                        let n = s.len().min(max_len);
                        result[..n].copy_from_slice(&s[..n]);
                    }
                    let mut or_accum = vec![0u8; max_len];
                    let mut has_one = false;
                    for s in sources.iter().skip(1).flatten() {
                        has_one = true;
                        for j in 0..s.len() {
                            or_accum[j] |= s[j];
                        }
                    }
                    if !has_one {
                        result.fill(0);
                    } else {
                        for j in 0..max_len {
                            result[j] &= or_accum[j];
                        }
                    }
                }
                BitOpType::One => {
                    for bit_pos in 0..max_len * 8 {
                        let mut count = 0i64;
                        for s in sources.iter().flatten() {
                            let byte_idx = bit_pos / 8;
                            if byte_idx >= s.len() {
                                continue;
                            }
                            if s[byte_idx] & (1u8 << (7 - (bit_pos % 8) as u32)) != 0 {
                                count += 1;
                            }
                        }
                        if count == 1 {
                            result[bit_pos / 8] |= 1u8 << (7 - (bit_pos % 8) as u32);
                        }
                    }
                }
            }

            tx.set(Entry::new(dest_key, result).metadata(TYPE_STRING))?;
            let result: DbResult = Box::new(max_len as i64);
            Ok(result)
        })
    }
}

/// Replies an integer result.
struct IntWire;

impl WireOp for IntWire {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(res) => match res.downcast::<i64>() {
                Ok(value) => RespValue::Integer(*value),
                Err(_) => {
                    RespValue::Error(Bytes::from_static(b"ERR internal error: bad int result"))
                }
            },
            Err(e) => err_resp(&e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::test_session;

    /// Runs a queued op through its own transaction and renders the reply.
    async fn exec(session: &Session, op: QueuedOp) -> RespValue {
        let store = session.store();
        let tx = store.begin(op.is_mutating).await.expect("tx");
        let outcome = op.db_op.run(&*tx).await;
        if op.is_mutating {
            tx.commit().await.expect("commit");
        }
        op.wire_op.reply(outcome)
    }

    fn expect_int(reply: &RespValue) -> i64 {
        match reply {
            RespValue::Integer(n) => *n,
            other => panic!("expected integer, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn set_bit_roundtrip_and_old_values() {
        let session = test_session();
        // SETBIT on a missing key: bit 0 → 1 returns 0, string becomes [0x80].
        assert_eq!(
            expect_int(&exec(&session, set_bit(&session, b"k", 0, 1)).await),
            0
        );
        assert_eq!(
            expect_int(&exec(&session, get_bit(&session, b"k", 0)).await),
            1
        );
        // Old bit was 1 → returns 1 when clearing.
        assert_eq!(
            expect_int(&exec(&session, set_bit(&session, b"k", 0, 0)).await),
            1
        );
        assert_eq!(
            expect_int(&exec(&session, get_bit(&session, b"k", 0)).await),
            0
        );
        // Bit 15 → 1 extends the string to two bytes.
        assert_eq!(
            expect_int(&exec(&session, set_bit(&session, b"k", 15, 1)).await),
            0
        );
        assert_eq!(
            expect_int(&exec(&session, get_bit(&session, b"k", 15)).await),
            1
        );
        // Final bytes: 0x00 (bit 0 cleared), 0x01 (bit 15 = last bit of byte 1).
        let store = session.store();
        let tx = store.begin(false).await.unwrap();
        let item = tx.get(&session.public_key(b"k")).await.unwrap();
        assert_eq!(item.value(), &[0x00, 0x01]);
        drop(tx);
    }

    #[tokio::test]
    async fn get_bit_out_of_range_and_missing() {
        let session = test_session();
        assert_eq!(
            expect_int(&exec(&session, get_bit(&session, b"missing", 0)).await),
            0
        );
        assert_eq!(
            expect_int(&exec(&session, set_bit(&session, b"k", 0, 1)).await),
            0
        );
        assert_eq!(
            expect_int(&exec(&session, get_bit(&session, b"k", 8)).await),
            0
        );
        assert_eq!(
            expect_int(&exec(&session, get_bit(&session, b"k", 0)).await),
            1
        );
        assert_eq!(
            expect_int(&exec(&session, get_bit(&session, b"k", 7)).await),
            0
        );
    }

    #[tokio::test]
    async fn bit_count_whole_and_ranges() {
        let session = test_session();
        assert_eq!(
            expect_int(&exec(&session, bit_count(&session, b"missing", false, false, 0, 0, false)).await),
            0
        );
        // 0b10101010 → 4 bits.
        set_value(&session, b"k", &[0b10101010]).await;
        assert_eq!(
            expect_int(&exec(&session, bit_count(&session, b"k", false, false, 0, 0, false)).await),
            4
        );
        // Byte range [0,0] on 0xFF, 0x00 → 8.
        set_value(&session, b"k2", &[0xFF, 0x00]).await;
        assert_eq!(
            expect_int(&exec(&session, bit_count(&session, b"k2", true, true, 0, 0, false)).await),
            8
        );
        // Byte range [1,1] → 0.
        assert_eq!(
            expect_int(&exec(&session, bit_count(&session, b"k2", true, true, 1, 1, false)).await),
            0
        );
        // Negative start: [ -1, -1 ] = last byte → 0.
        assert_eq!(
            expect_int(&exec(&session, bit_count(&session, b"k2", true, true, -1, -1, false)).await),
            0
        );
        // Bit range [0,3] on 0xFF → 4.
        assert_eq!(
            expect_int(&exec(&session, bit_count(&session, b"k2", true, true, 0, 3, true)).await),
            4
        );
        // Bit range [8,15] on 0x00 → 0.
        assert_eq!(
            expect_int(&exec(&session, bit_count(&session, b"k2", true, true, 8, 15, true)).await),
            0
        );
    }

    #[tokio::test]
    async fn bit_pos_variants() {
        let session = test_session();
        // Missing key: bit 0 → 0, bit 1 → -1.
        assert_eq!(
            expect_int(&exec(&session, bit_pos(&session, b"missing", 0, false, 0, 0, false)).await),
            0
        );
        assert_eq!(
            expect_int(&exec(&session, bit_pos(&session, b"missing", 1, false, 0, 0, false)).await),
            -1
        );
        // 0b01000000 → first 1 at bit 1.
        set_value(&session, b"k", &[0b01000000]).await;
        assert_eq!(
            expect_int(&exec(&session, bit_pos(&session, b"k", 1, false, 0, 0, false)).await),
            1
        );
        // All ones: first 0 is past the end → 8.
        set_value(&session, b"k2", &[0xFF]).await;
        assert_eq!(
            expect_int(&exec(&session, bit_pos(&session, b"k2", 0, false, 0, 0, false)).await),
            8
        );
        // 0b11111011 → first 0 at bit 5.
        set_value(&session, b"k3", &[0b11111011]).await;
        assert_eq!(
            expect_int(&exec(&session, bit_pos(&session, b"k3", 0, false, 0, 0, false)).await),
            5
        );
        // Range search: [0,0] byte → first 1 in first byte.
        assert_eq!(
            expect_int(&exec(&session, bit_pos(&session, b"k3", 1, true, 0, 0, false)).await),
            0
        );
        // 0x00, 0x80 → first 1 at bit 8.
        set_value(&session, b"k4", &[0x00, 0x80]).await;
        assert_eq!(
            expect_int(&exec(&session, bit_pos(&session, b"k4", 1, false, 0, 0, false)).await),
            8
        );
        assert_eq!(
            expect_int(&exec(&session, bit_pos(&session, b"k4", 1, true, 8, 15, true)).await),
            8
        );
        assert_eq!(
            expect_int(&exec(&session, bit_pos(&session, b"k4", 1, true, 0, 7, true)).await),
            -1
        );
    }

    #[tokio::test]
    async fn bit_pos_in_range_unit() {
        let data: &[u8] = &[0xFF, 0x00, 0xFF];
        assert_eq!(bit_pos_in_range(data, 0, 7, 1), 0);
        assert_eq!(bit_pos_in_range(data, 0, 7, 0), -1);
        assert_eq!(bit_pos_in_range(data, 8, 15, 0), 8);
        assert_eq!(bit_pos_in_range(data, 8, 15, 1), -1);
        assert_eq!(bit_pos_in_range(data, 4, 12, 1), 4);
        assert_eq!(bit_pos_in_range(data, 4, 12, 0), 8);
        assert_eq!(bit_pos_in_range(data, 16, 15, 0), -1);
    }

    #[tokio::test]
    async fn bit_op_and_or_xor_not() {
        let session = test_session();
        set_value(&session, b"a", &[0xF0]).await;
        set_value(&session, b"b", &[0x0F]).await;
        // AND
        let op = bit_op(&session, b"dest", BitOpType::And, &[b"a", b"b"]);
        assert_eq!(expect_int(&exec(&session, op).await), 1);
        assert_eq!(read_value(&session, b"dest").await, &[0x00]);
        // OR
        let op = bit_op(&session, b"dest", BitOpType::Or, &[b"a", b"b"]);
        assert_eq!(expect_int(&exec(&session, op).await), 1);
        assert_eq!(read_value(&session, b"dest").await, &[0xFF]);
        // XOR
        let op = bit_op(&session, b"dest", BitOpType::Xor, &[b"a", b"b"]);
        assert_eq!(expect_int(&exec(&session, op).await), 1);
        assert_eq!(read_value(&session, b"dest").await, &[0xFF]);
        // NOT
        let op = bit_op(&session, b"dest", BitOpType::Not, &[b"a"]);
        assert_eq!(expect_int(&exec(&session, op).await), 1);
        assert_eq!(read_value(&session, b"dest").await, &[0x0F]);
        // DIFF: a AND NOT b = 0xF0
        let op = bit_op(&session, b"dest", BitOpType::Diff, &[b"a", b"b"]);
        assert_eq!(expect_int(&exec(&session, op).await), 1);
        assert_eq!(read_value(&session, b"dest").await, &[0xF0]);
    }

    #[tokio::test]
    async fn bit_op_one() {
        let session = test_session();
        set_value(&session, b"a", &[0b10001000]).await;
        set_value(&session, b"b", &[0b00101000]).await;
        let op = bit_op(&session, b"dest", BitOpType::One, &[b"a", b"b"]);
        assert_eq!(expect_int(&exec(&session, op).await), 1);
        assert_eq!(read_value(&session, b"dest").await, &[0b10100000]);
    }

    #[tokio::test]
    async fn bit_op_variable_length_and_missing() {
        let session = test_session();
        set_value(&session, b"a", &[0xFF]).await;
        set_value(&session, b"b", &[0x0F, 0xFF]).await;
        // OR of [0xFF] and [0x0F, 0xFF] → [0xFF, 0xFF], length 2.
        let op = bit_op(&session, b"dest", BitOpType::Or, &[b"a", b"b"]);
        assert_eq!(expect_int(&exec(&session, op).await), 2);
        assert_eq!(read_value(&session, b"dest").await, &[0xFF, 0xFF]);
        // Missing sources → empty result, length 0.
        let op = bit_op(&session, b"dest2", BitOpType::Or, &[b"nope", b"nope2"]);
        assert_eq!(expect_int(&exec(&session, op).await), 0);
        assert_eq!(read_value(&session, b"dest2").await, Vec::<u8>::new());
    }

    async fn set_value(session: &Session, key: &[u8], value: &[u8]) {
        let store = session.store();
        let tx = store.begin(true).await.expect("tx");
        tx.set(Entry::new(session.public_key(key), value.to_vec()).metadata(TYPE_STRING))
            .expect("set");
        tx.commit().await.expect("commit");
    }

    async fn read_value(session: &Session, key: &[u8]) -> Vec<u8> {
        let store = session.store();
        let tx = store.begin(false).await.unwrap();
        let item = tx.get(&session.public_key(key)).await.unwrap();
        let v = item.value().to_vec();
        drop(tx);
        v
    }
}
