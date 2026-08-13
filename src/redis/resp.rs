//! RESP (REdis Serialization Protocol) wire-format decoding.
//!
//! Implements [`Decoder`] so that a Tokio [`FramedRead`] can turn a raw
//! stream of bytes from a client socket into a stream of [`RespValue`]s.
//!
//! Design notes, in contrast to typical tutorial implementations:
//!
//! * Bulk strings are **binary-safe**: they are carried as [`Bytes`], not
//!   `String`, so payloads that are not valid UTF-8 (protobuf blobs, digests,
//!   serialized objects) round-trip byte-for-byte.
//! * **Client-declared lengths are bounded** the moment they are parsed:
//!   [`RespDecoder::max_bulk_len`] and [`RespDecoder::max_array_len`] reject
//!   absurd declared sizes before any allocation or waiting happens (the
//!   proto-max-bulk-len equivalent of Redis's `PROTO_MAX_BULK_LEN` and
//!   `PROTO_MAX_MULTIBULK_LEN`).
//! * The decoder is **stateful** (a stack of in-progress arrays) and never
//!   clones the input buffer to peek: lines are located with an immutable
//!   slice scan and consumed with `split_to`, and bulk-string payloads are
//!   zero-copy views over the receive buffer (`split_to(...).truncate(...)
//!   .freeze()`).
//!
//! A command arrives as an array of bulk strings, e.g. `*3\r\n$3\r\nSET\r\n
//! $3\r\nfoo\r\n$3\r\nbar\r\n` decodes to
//! `RespValue::Array(Some([BulkString(SET), BulkString(foo),
//! BulkString(bar)]))`.

use bytes::{Buf, Bytes, BytesMut};
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};

/// Default maximum length of a bulk string (512 MiB, matching Redis's
/// `PROTO_MAX_BULK_LEN`).
pub const DEFAULT_MAX_BULK_LEN: usize = 512 * 1024 * 1024;
/// Default maximum number of elements in an array (matching Redis's
/// `PROTO_MAX_MULTIBULK_LEN`).
pub const DEFAULT_MAX_ARRAY_LEN: usize = 1024 * 1024;
/// Default maximum nesting depth for arrays.
pub const DEFAULT_MAX_DEPTH: usize = 64;

/// Error produced while decoding RESP data. Used as the [`Decoder::Error`]
/// type; a protocol violation aborts the connection, matching how a real
/// Redis server responds to malformed input.
#[derive(Debug, Error)]
pub enum RespError {
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
}

impl RespError {
    fn protocol(msg: impl Into<String>) -> Self {
        RespError::Protocol(msg.into())
    }
}

/// A decoded RESP value. Bulk strings and simple strings are binary-safe
/// ([`Bytes`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespValue {
    SimpleString(Bytes),
    Error(Bytes),
    Integer(i64),
    /// `None` represents the null bulk string (`$-1\r\n`).
    BulkString(Option<Bytes>),
    /// `None` represents the null array (`*-1\r\n`).
    Array(Option<Vec<RespValue>>),
}

/// A frame currently being assembled: an array waiting for its remaining
/// elements.
struct ArrayFrame {
    remaining: usize,
    elements: Vec<RespValue>,
}

/// A Tokio codec that frames incoming bytes into [`RespValue`]s.
///
/// The decoder is stateful across calls: once it has seen a bulk-string
/// header or an array header it remembers it, so a frame split across many
/// socket reads is assembled without re-scanning the header.
pub struct RespDecoder {
    max_bulk_len: usize,
    max_array_len: usize,
    max_depth: usize,
    /// In-progress arrays; the last entry is the innermost array being
    /// filled with elements.
    stack: Vec<ArrayFrame>,
    /// Length of the bulk-string payload currently awaited, if any.
    pending_bulk_len: Option<usize>,
}

impl RespDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a decoder with explicit limits. Declared bulk-string and
    /// array lengths above these are rejected with a protocol error.
    pub fn with_limits(max_bulk_len: usize, max_array_len: usize, max_depth: usize) -> Self {
        Self {
            max_bulk_len,
            max_array_len,
            max_depth,
            stack: Vec::new(),
            pending_bulk_len: None,
        }
    }

    pub fn max_bulk_len(&self) -> usize {
        self.max_bulk_len
    }

    pub fn max_array_len(&self) -> usize {
        self.max_array_len
    }

    pub fn max_depth(&self) -> usize {
        self.max_depth
    }

    /// Routes a completed value to the innermost open array, or returns it
    /// directly when it is a top-level value. When a completed array closes
    /// its parent, the cascade continues.
    fn deliver(&mut self, value: RespValue) -> Option<RespValue> {
        if self.stack.is_empty() {
            return Some(value);
        }
        let top = self.stack.last_mut().expect("stack is not empty");
        top.elements.push(value);
        top.remaining -= 1;
        while self.stack.last().is_some_and(|f| f.remaining == 0) {
            let frame = self.stack.pop().expect("stack is not empty");
            let array = RespValue::Array(Some(frame.elements));
            if self.stack.is_empty() {
                return Some(array);
            }
            let top = self.stack.last_mut().expect("stack is not empty");
            top.elements.push(array);
            top.remaining -= 1;
        }
        None
    }
}

impl Default for RespDecoder {
    fn default() -> Self {
        Self::with_limits(
            DEFAULT_MAX_BULK_LEN,
            DEFAULT_MAX_ARRAY_LEN,
            DEFAULT_MAX_DEPTH,
        )
    }
}

impl Decoder for RespDecoder {
    type Item = RespValue;
    type Error = RespError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            // Finish a bulk string whose header was already consumed.
            if let Some(len) = self.pending_bulk_len.take() {
                if src.len() < len + 2 {
                    self.pending_bulk_len = Some(len);
                    return Ok(None);
                }
                if !src[len..len + 2].starts_with(b"\r\n") {
                    return Err(RespError::protocol(
                        "bulk string payload not followed by CRLF",
                    ));
                }
                let mut payload = src.split_to(len + 2);
                payload.truncate(len);
                if let Some(v) = self.deliver(RespValue::BulkString(Some(payload.freeze()))) {
                    return Ok(Some(v));
                }
                continue;
            }

            if src.is_empty() {
                return Ok(None);
            }

            match src[0] {
                b'+' => {
                    let Some((clen, consumed)) = read_line(src, self.max_bulk_len)? else {
                        return Ok(None);
                    };
                    let value = RespValue::SimpleString(Bytes::copy_from_slice(&src[1..1 + clen]));
                    src.advance(consumed);
                    if let Some(v) = self.deliver(value) {
                        return Ok(Some(v));
                    }
                }
                b'-' => {
                    let Some((clen, consumed)) = read_line(src, self.max_bulk_len)? else {
                        return Ok(None);
                    };
                    let value = RespValue::Error(Bytes::copy_from_slice(&src[1..1 + clen]));
                    src.advance(consumed);
                    if let Some(v) = self.deliver(value) {
                        return Ok(Some(v));
                    }
                }
                b':' => {
                    let Some((clen, consumed)) = read_line(src, self.max_bulk_len)? else {
                        return Ok(None);
                    };
                    let integer = parse_int(&src[1..1 + clen])?;
                    src.advance(consumed);
                    if let Some(v) = self.deliver(RespValue::Integer(integer)) {
                        return Ok(Some(v));
                    }
                }
                b'$' => {
                    let Some((clen, consumed)) = read_line(src, self.max_bulk_len)? else {
                        return Ok(None);
                    };
                    let declared = parse_int(&src[1..1 + clen])?;
                    src.advance(consumed);
                    if declared < -1 {
                        return Err(RespError::protocol(format!(
                            "invalid bulk string length {declared}"
                        )));
                    }
                    if declared == -1 {
                        if let Some(v) = self.deliver(RespValue::BulkString(None)) {
                            return Ok(Some(v));
                        }
                        continue;
                    }
                    let len = declared as usize;
                    if len > self.max_bulk_len {
                        return Err(RespError::protocol(format!(
                            "bulk string length {len} exceeds configured maximum {}",
                            self.max_bulk_len
                        )));
                    }
                    self.pending_bulk_len = Some(len);
                    continue;
                }
                b'*' => {
                    let Some((clen, consumed)) = read_line(src, self.max_bulk_len)? else {
                        return Ok(None);
                    };
                    let declared = parse_int(&src[1..1 + clen])?;
                    src.advance(consumed);
                    if declared < -1 {
                        return Err(RespError::protocol(format!(
                            "invalid array length {declared}"
                        )));
                    }
                    if declared == -1 {
                        if let Some(v) = self.deliver(RespValue::Array(None)) {
                            return Ok(Some(v));
                        }
                        continue;
                    }
                    let len = declared as usize;
                    if len > self.max_array_len {
                        return Err(RespError::protocol(format!(
                            "array length {len} exceeds configured maximum {}",
                            self.max_array_len
                        )));
                    }
                    if len == 0 {
                        if let Some(v) = self.deliver(RespValue::Array(Some(Vec::new()))) {
                            return Ok(Some(v));
                        }
                        continue;
                    }
                    if self.stack.len() >= self.max_depth {
                        return Err(RespError::protocol("maximum array nesting depth exceeded"));
                    }
                    self.stack.push(ArrayFrame {
                        remaining: len,
                        elements: Vec::new(),
                    });
                    continue;
                }
                b => {
                    return Err(RespError::protocol(format!(
                        "unexpected type byte {b:#04x}"
                    )));
                }
            }
        }
    }
}

/// Encodes a [`RespValue`] to the wire format so a [`Framed`] can use
/// [`RespDecoder`] for both directions. Bulk strings stay binary-safe: the
/// payload length is written in bytes, never via string length semantics.
impl Encoder<RespValue> for RespDecoder {
    type Error = RespError;

    fn encode(&mut self, item: RespValue, dst: &mut BytesMut) -> Result<(), Self::Error> {
        encode_value(&item, dst);
        Ok(())
    }
}

/// Encodes `value` into `dst` as RESP.
fn encode_value(value: &RespValue, dst: &mut BytesMut) {
    match value {
        RespValue::SimpleString(s) => {
            dst.extend_from_slice(b"+");
            dst.extend_from_slice(s);
            dst.extend_from_slice(b"\r\n");
        }
        RespValue::Error(e) => {
            dst.extend_from_slice(b"-");
            dst.extend_from_slice(e);
            dst.extend_from_slice(b"\r\n");
        }
        RespValue::Integer(n) => {
            dst.extend_from_slice(b":");
            dst.extend_from_slice(n.to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        RespValue::BulkString(Some(b)) => {
            dst.extend_from_slice(b"$");
            dst.extend_from_slice(b.len().to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
            dst.extend_from_slice(b);
            dst.extend_from_slice(b"\r\n");
        }
        RespValue::BulkString(None) => dst.extend_from_slice(b"$-1\r\n"),
        RespValue::Array(Some(items)) => {
            dst.extend_from_slice(b"*");
            dst.extend_from_slice(items.len().to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
            for item in items {
                encode_value(item, dst);
            }
        }
        RespValue::Array(None) => dst.extend_from_slice(b"*-1\r\n"),
    }
}

/// Locates the `\r\n` terminator, returning the index of the `\r`.
fn find_crlf(src: &[u8]) -> Option<usize> {
    src.windows(2).position(|w| w == b"\r\n")
}

/// Reads a CRLF-terminated line whose first byte is the type prefix already
/// consumed by the caller's match. Returns `(content_len, total_consumed)`
/// where the content is `src[1..1 + content_len]` and `total_consumed` is
/// the number of bytes to advance past (type byte + content + CRLF).
///
/// Returns `Ok(None)` when the line is incomplete. Declared lengths that
/// exceed `max_len` (or buffers with no CRLF that already exceed it) are
/// rejected so a hostile client cannot grow the receive buffer unboundedly.
fn read_line(src: &[u8], max_len: usize) -> Result<Option<(usize, usize)>, RespError> {
    let crlf = match find_crlf(src) {
        Some(i) => i,
        None => {
            if src.len() >= max_len {
                return Err(RespError::protocol("line exceeds maximum length"));
            }
            return Ok(None);
        }
    };
    let content_len = crlf - 1;
    if content_len > max_len {
        return Err(RespError::protocol("line exceeds maximum length"));
    }
    Ok(Some((content_len, crlf + 2)))
}

/// Parses a base-10 signed integer, the only shape RESP allows for integer
/// and length fields. Overflow and non-digit bytes are protocol errors.
fn parse_int(buf: &[u8]) -> Result<i64, RespError> {
    let mut value: i64 = 0;
    let mut idx = 0;
    let negative = match buf.first() {
        Some(b'-') => {
            idx = 1;
            true
        }
        _ => false,
    };
    if idx >= buf.len() {
        return Err(RespError::protocol("invalid integer: no digits"));
    }
    for &b in &buf[idx..] {
        if !b.is_ascii_digit() {
            return Err(RespError::protocol(format!(
                "invalid integer: unexpected byte {b:#04x}"
            )));
        }
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(i64::from(b - b'0')))
            .ok_or_else(|| RespError::protocol("integer out of range"))?;
    }
    Ok(if negative { -value } else { value })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(input: &[u8]) -> Result<Option<RespValue>, RespError> {
        let mut buf = BytesMut::from(input);
        RespDecoder::new().decode(&mut buf)
    }

    fn bulk(bytes: &[u8]) -> RespValue {
        RespValue::BulkString(Some(Bytes::copy_from_slice(bytes)))
    }

    fn array(elements: Vec<RespValue>) -> RespValue {
        RespValue::Array(Some(elements))
    }

    // --- scalar types ---

    #[test]
    fn simple_string() {
        assert_eq!(
            decode(b"+OK\r\n").unwrap(),
            Some(RespValue::SimpleString(Bytes::from_static(b"OK")))
        );
    }

    #[test]
    fn error_value() {
        assert_eq!(
            decode(b"-ERR wrong number of arguments\r\n").unwrap(),
            Some(RespValue::Error(Bytes::from_static(
                b"ERR wrong number of arguments"
            )))
        );
    }

    #[test]
    fn integer() {
        assert_eq!(
            decode(b":1000\r\n").unwrap(),
            Some(RespValue::Integer(1000))
        );
        assert_eq!(decode(b":-1\r\n").unwrap(), Some(RespValue::Integer(-1)));
        assert_eq!(decode(b":0\r\n").unwrap(), Some(RespValue::Integer(0)));
    }

    #[test]
    fn bulk_string() {
        assert_eq!(decode(b"$5\r\nhello\r\n").unwrap(), Some(bulk(b"hello")));
    }

    #[test]
    fn bulk_string_is_binary_safe() {
        // 8 bytes that are not valid UTF-8, including NUL and 0x80+ bytes.
        let payload = [0x00, 0x01, 0x02, 0xff, 0xfe, 0xfd, 0xfc, 0x7f];
        let mut wire = b"$8\r\n".to_vec();
        wire.extend_from_slice(&payload);
        wire.extend_from_slice(b"\r\n");
        assert_eq!(
            decode(&wire).unwrap(),
            Some(RespValue::BulkString(Some(Bytes::copy_from_slice(
                &payload
            ))))
        );
    }

    #[test]
    fn empty_bulk_string() {
        assert_eq!(
            decode(b"$0\r\n\r\n").unwrap(),
            Some(RespValue::BulkString(Some(Bytes::new())))
        );
    }

    #[test]
    fn null_bulk_string() {
        assert_eq!(
            decode(b"$-1\r\n").unwrap(),
            Some(RespValue::BulkString(None))
        );
    }

    // --- arrays ---

    #[test]
    fn array_of_bulk_strings() {
        let wire = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        assert_eq!(
            decode(wire).unwrap(),
            Some(array(vec![bulk(b"SET"), bulk(b"foo"), bulk(b"bar")]))
        );
    }

    #[test]
    fn empty_array() {
        assert_eq!(decode(b"*0\r\n").unwrap(), Some(array(vec![])));
    }

    #[test]
    fn null_array() {
        assert_eq!(decode(b"*-1\r\n").unwrap(), Some(RespValue::Array(None)));
    }

    #[test]
    fn nested_arrays() {
        let wire = b"*2\r\n*1\r\n$1\r\na\r\n$1\r\nb\r\n";
        assert_eq!(
            decode(wire).unwrap(),
            Some(array(vec![array(vec![bulk(b"a")]), bulk(b"b"),]))
        );
    }

    #[test]
    fn array_of_mixed_types() {
        let wire = b"*3\r\n:1\r\n+OK\r\n-FINE\r\n";
        assert_eq!(
            decode(wire).unwrap(),
            Some(array(vec![
                RespValue::Integer(1),
                RespValue::SimpleString(Bytes::from_static(b"OK")),
                RespValue::Error(Bytes::from_static(b"FINE")),
            ]))
        );
    }

    #[test]
    fn array_elements_may_be_null() {
        let wire = b"*2\r\n$-1\r\n*-1\r\n";
        assert_eq!(
            decode(wire).unwrap(),
            Some(array(vec![
                RespValue::BulkString(None),
                RespValue::Array(None),
            ]))
        );
    }

    // --- incomplete frames ---

    #[test]
    fn empty_input_is_not_a_frame() {
        assert_eq!(decode(b"").unwrap(), None);
    }

    #[test]
    fn incomplete_bulk_string_header() {
        assert_eq!(decode(b"$5\r").unwrap(), None);
        assert_eq!(decode(b"$").unwrap(), None);
    }

    #[test]
    fn incomplete_bulk_string_payload_accumulates_across_calls() {
        let mut decoder = RespDecoder::new();
        let mut buf = BytesMut::from(&b"$5\r\nhel"[..]);
        assert_eq!(decoder.decode(&mut buf).unwrap(), None);
        assert_eq!(buf.len(), 3, "incomplete payload must not be consumed");

        buf.extend_from_slice(b"lo\r\n");
        assert_eq!(decoder.decode(&mut buf).unwrap(), Some(bulk(b"hello")));
        assert!(buf.is_empty());
    }

    #[test]
    fn bulk_string_requires_trailing_crlf() {
        // Payload present but terminated by LF only: never a complete frame.
        let mut buf = BytesMut::from(&b"$3\r\nabc\n"[..]);
        let mut decoder = RespDecoder::new();
        assert_eq!(decoder.decode(&mut buf).unwrap(), None);

        // Payload terminated by garbage: protocol error.
        let mut buf = BytesMut::from(&b"$3\r\nabcXX\r\n"[..]);
        let mut decoder = RespDecoder::new();
        assert!(matches!(
            decoder.decode(&mut buf).unwrap_err(),
            RespError::Protocol(_)
        ));
    }

    #[test]
    fn incomplete_array() {
        let mut decoder = RespDecoder::new();
        let mut buf = BytesMut::from(&b"*2\r\n$1\r\na\r\n"[..]);
        assert_eq!(decoder.decode(&mut buf).unwrap(), None);

        buf.extend_from_slice(b"$1\r\nb\r\n");
        assert_eq!(
            decoder.decode(&mut buf).unwrap(),
            Some(array(vec![bulk(b"a"), bulk(b"b")]))
        );
    }

    #[test]
    fn declared_large_bulk_waits_without_allocating() {
        let mut decoder = RespDecoder::new();
        let mut buf = BytesMut::from(&b"$100\r\n"[..]);
        assert_eq!(decoder.decode(&mut buf).unwrap(), None);
    }

    #[test]
    fn byte_at_a_time_accumulation() {
        let cmd = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let mut decoder = RespDecoder::new();
        let mut buf = BytesMut::new();
        let mut completed = None;
        for (i, &b) in cmd.iter().enumerate() {
            buf.extend_from_slice(&[b]);
            if let Some(v) = decoder.decode(&mut buf).unwrap() {
                assert_eq!(
                    i,
                    cmd.len() - 1,
                    "frame must not complete before the final byte"
                );
                completed = Some(v);
            }
        }
        assert_eq!(
            completed,
            Some(array(vec![bulk(b"SET"), bulk(b"foo"), bulk(b"bar")]))
        );
    }

    // --- pipelining ---

    #[test]
    fn pipelined_frames_decode_individually() {
        let mut decoder = RespDecoder::new();
        let mut buf = BytesMut::from(&b"+OK\r\n:42\r\n"[..]);

        assert_eq!(
            decoder.decode(&mut buf).unwrap(),
            Some(RespValue::SimpleString(Bytes::from_static(b"OK")))
        );
        assert_eq!(
            decoder.decode(&mut buf).unwrap(),
            Some(RespValue::Integer(42))
        );
        assert_eq!(decoder.decode(&mut buf).unwrap(), None);
        assert!(buf.is_empty());
    }

    // --- length bounds (the proto-max-bulk-len guard) ---

    #[test]
    fn oversize_bulk_string_rejected() {
        let mut decoder = RespDecoder::with_limits(4, DEFAULT_MAX_ARRAY_LEN, DEFAULT_MAX_DEPTH);
        let mut buf = BytesMut::from(&b"$5\r\nhello\r\n"[..]);
        assert!(matches!(
            decoder.decode(&mut buf).unwrap_err(),
            RespError::Protocol(_)
        ));
    }

    #[test]
    fn absurd_declared_bulk_length_rejected_before_payload() {
        // No payload is present at all; the declared length alone must fail.
        let mut decoder = RespDecoder::new();
        let mut buf = BytesMut::from(&b"$99999999999\r\n"[..]);
        assert!(matches!(
            decoder.decode(&mut buf).unwrap_err(),
            RespError::Protocol(_)
        ));
    }

    #[test]
    fn oversize_array_rejected() {
        let mut decoder = RespDecoder::with_limits(DEFAULT_MAX_BULK_LEN, 2, DEFAULT_MAX_DEPTH);
        let mut buf = BytesMut::from(&b"*3\r\n$1\r\na\r\n"[..]);
        assert!(matches!(
            decoder.decode(&mut buf).unwrap_err(),
            RespError::Protocol(_)
        ));
    }

    #[test]
    fn line_without_crlf_capped() {
        let mut decoder = RespDecoder::with_limits(8, DEFAULT_MAX_ARRAY_LEN, DEFAULT_MAX_DEPTH);
        // Fewer bytes than the cap, no CRLF: just incomplete.
        let mut buf = BytesMut::from(&b"+abcdef"[..]);
        assert_eq!(decoder.decode(&mut buf).unwrap(), None);

        // At the cap with no CRLF: protocol error, buffer cannot grow unbounded.
        let mut buf = BytesMut::from(&b"+abcdefg"[..]);
        assert!(matches!(
            decoder.decode(&mut buf).unwrap_err(),
            RespError::Protocol(_)
        ));
    }

    #[test]
    fn oversized_line_content_rejected() {
        let mut decoder = RespDecoder::with_limits(4, DEFAULT_MAX_ARRAY_LEN, DEFAULT_MAX_DEPTH);
        let mut buf = BytesMut::from(&b"+hello\r\n"[..]);
        assert!(matches!(
            decoder.decode(&mut buf).unwrap_err(),
            RespError::Protocol(_)
        ));
    }

    #[test]
    fn nesting_depth_is_bounded() {
        let mut decoder = RespDecoder::with_limits(DEFAULT_MAX_BULK_LEN, DEFAULT_MAX_ARRAY_LEN, 2);

        // 2 levels is within the limit.
        let mut buf = BytesMut::from(&b"*1\r\n*1\r\n$1\r\na\r\n"[..]);
        assert_eq!(
            decoder.decode(&mut buf).unwrap(),
            Some(array(vec![array(vec![bulk(b"a")])]))
        );

        // 3 levels exceeds it.
        let mut buf = BytesMut::from(&b"*1\r\n*1\r\n*1\r\n$1\r\na\r\n"[..]);
        assert!(matches!(
            decoder.decode(&mut buf).unwrap_err(),
            RespError::Protocol(_)
        ));
    }

    // --- malformed input ---

    #[test]
    fn negative_length_below_minus_one_rejected() {
        assert!(decode(b"$-2\r\n").is_err());
        assert!(decode(b"*-2\r\n").is_err());
    }

    #[test]
    fn non_numeric_length_rejected() {
        assert!(decode(b"$abc\r\n").is_err());
        assert!(decode(b"$+5\r\n").is_err());
        assert!(decode(b"$ 5\r\n").is_err());
        assert!(decode(b"*x\r\n").is_err());
    }

    #[test]
    fn malformed_integer_rejected() {
        assert!(decode(b":12a3\r\n").is_err());
        assert!(decode(b":\r\n").is_err());
        assert!(decode(b":+5\r\n").is_err());
    }

    #[test]
    fn integer_overflow_rejected() {
        assert!(decode(b":99999999999999999999999999\r\n").is_err());
        assert!(decode(b"$99999999999999999999999999\r\n").is_err());
    }

    #[test]
    fn unknown_type_byte_rejected() {
        assert!(decode(b"~foo\r\n").is_err());
        assert!(decode(b"\r\n").is_err());
        assert!(decode(b"#t\r\n").is_err());
    }

    // --- encoding (Encoder side of the codec) ---

    #[test]
    fn encode_simple_string() {
        let mut dst = BytesMut::new();
        RespDecoder::new()
            .encode(RespValue::SimpleString(Bytes::from_static(b"OK")), &mut dst)
            .unwrap();
        assert_eq!(&dst[..], b"+OK\r\n");
    }

    #[test]
    fn encode_integer() {
        let mut dst = BytesMut::new();
        let mut codec = RespDecoder::new();
        codec.encode(RespValue::Integer(42), &mut dst).unwrap();
        codec.encode(RespValue::Integer(-7), &mut dst).unwrap();
        assert_eq!(&dst[..], b":42\r\n:-7\r\n");
    }

    #[test]
    fn encode_bulk_string_is_binary_safe() {
        let payload = [0x00, 0x01, 0xff, 0xfe];
        let mut dst = BytesMut::new();
        RespDecoder::new().encode(bulk(&payload), &mut dst).unwrap();

        let mut expected = b"$4\r\n".to_vec();
        expected.extend_from_slice(&payload);
        expected.extend_from_slice(b"\r\n");
        assert_eq!(&dst[..], &expected[..]);
    }

    #[test]
    fn encode_null_and_empty() {
        let mut dst = BytesMut::new();
        let mut codec = RespDecoder::new();
        codec.encode(RespValue::BulkString(None), &mut dst).unwrap();
        codec.encode(RespValue::Array(None), &mut dst).unwrap();
        codec
            .encode(RespValue::BulkString(Some(Bytes::new())), &mut dst)
            .unwrap();
        assert_eq!(&dst[..], b"$-1\r\n*-1\r\n$0\r\n\r\n");
    }

    #[test]
    fn encode_array_round_trips_through_decode() {
        let value = array(vec![bulk(b"SET"), bulk(b"foo"), bulk(b"bar")]);
        let mut dst = BytesMut::new();
        RespDecoder::new().encode(value.clone(), &mut dst).unwrap();
        assert_eq!(RespDecoder::new().decode(&mut dst).unwrap(), Some(value));
    }

    // --- Decoder over an actual byte stream (FramedRead) ---

    #[tokio::test]
    async fn framed_read_decodes_pipelined_commands() {
        use futures::StreamExt;
        use tokio::io::{duplex, AsyncWriteExt};
        use tokio_util::codec::FramedRead;

        let (mut tx, rx) = duplex(64);
        let mut framed = FramedRead::new(rx, RespDecoder::new());

        tokio::spawn(async move {
            tx.write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*1\r\n$4\r\nPING\r\n")
                .await
                .unwrap();
        });

        let first = framed.next().await.expect("first frame").unwrap();
        assert_eq!(first, array(vec![bulk(b"SET"), bulk(b"foo"), bulk(b"bar")]));

        let second = framed.next().await.expect("second frame").unwrap();
        assert_eq!(second, array(vec![bulk(b"PING")]));

        assert!(
            framed.next().await.is_none(),
            "stream must end after the bytes"
        );
    }
}
