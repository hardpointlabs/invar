//! Response encoding for the Redis-over-HTTP listener (RFC 0003).

use base64::{engine::general_purpose::STANDARD, Engine};
use bytes::{Bytes, BytesMut};
use serde_json::{json, Map, Value};
use tokio_util::codec::Encoder;

use crate::resp::{downgrade_to_resp2, Protocol, RespDecoder, RespValue};

fn text(bytes: &Bytes, base64: bool) -> Value {
    if base64 {
        Value::String(STANDARD.encode(bytes))
    } else {
        Value::String(String::from_utf8_lossy(bytes).into_owned())
    }
}

fn error_text(bytes: &Bytes) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// Maps a RESP2-shaped reply onto the JSON `result` value. With `base64`, every
/// string except the `OK` status reply is base64 encoded; error strings never
/// are. The caller must have run [`downgrade_to_resp2`] already.
pub fn result_value(value: &RespValue, base64: bool) -> Value {
    match value {
        RespValue::SimpleString(s) if !base64 || s.as_ref() == b"OK" => text(s, false),
        RespValue::SimpleString(s) | RespValue::BulkString(Some(s)) => text(s, base64),
        RespValue::BulkString(None) | RespValue::Array(None) => Value::Null,
        RespValue::Integer(n) => json!(n),
        RespValue::Error(e) => Value::String(error_text(e)),
        RespValue::Array(Some(items)) => {
            Value::Array(items.iter().map(|i| result_value(i, base64)).collect())
        }
        // Unreachable after `downgrade_to_resp2`; fail loudly in tests.
        other => {
            debug_assert!(false, "RESP3 value reached the JSON mapper: {other:?}");
            Value::Null
        }
    }
}

/// One `{"result": ..}` / `{"error": ..}` object for a (possibly RESP3) reply.
/// Returns whether the reply was an error alongside it.
pub fn item(mut reply: RespValue, base64: bool) -> (Value, bool) {
    downgrade_to_resp2(&mut reply);
    match &reply {
        RespValue::Error(e) => (error_object(&error_text(e)), true),
        other => {
            let mut obj = Map::new();
            obj.insert("result".into(), result_value(other, base64));
            (Value::Object(obj), false)
        }
    }
}

pub fn error_object(message: &str) -> Value {
    json!({ "error": message })
}

/// The raw RESP2 bytes of a reply, for `Upstash-Response-Format: resp2`.
pub fn resp2_bytes(reply: RespValue) -> Bytes {
    let mut codec = RespDecoder::new();
    codec.set_protocol(Protocol::Resp2);
    let mut dst = BytesMut::new();
    // Encoding into a BytesMut cannot fail.
    let _ = codec.encode(reply, &mut dst);
    dst.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> Bytes {
        Bytes::copy_from_slice(v.as_bytes())
    }

    #[test]
    fn plain_mapping() {
        let reply = RespValue::Array(Some(vec![
            RespValue::SimpleString(s("OK")),
            RespValue::BulkString(Some(s("héllo"))),
            RespValue::BulkString(None),
            RespValue::Integer(-3),
            RespValue::Array(Some(vec![RespValue::Integer(1)])),
        ]));
        assert_eq!(
            result_value(&reply, false),
            json!(["OK", "héllo", null, -3, [1]])
        );
    }

    #[test]
    fn invalid_utf8_is_replaced() {
        let reply = RespValue::BulkString(Some(Bytes::from_static(&[0xff, b'a'])));
        assert_eq!(result_value(&reply, false), json!("\u{fffd}a"));
    }

    #[test]
    fn base64_rule() {
        // OK stays plain; PONG and bulk strings (even "OK") are encoded; nested arrays recurse.
        assert_eq!(
            item(RespValue::SimpleString(s("OK")), true).0,
            json!({"result": "OK"})
        );
        assert_eq!(
            item(RespValue::SimpleString(s("PONG")), true).0,
            json!({"result": "UE9ORw=="})
        );
        assert_eq!(
            item(RespValue::BulkString(Some(s("OK"))), true).0,
            json!({"result": "T0s="})
        );
        let nested = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(s("a"))),
            RespValue::Integer(7),
            RespValue::BulkString(None),
        ]));
        assert_eq!(item(nested, true).0, json!({"result": ["YQ==", 7, null]}));
    }

    #[test]
    fn errors_are_never_encoded() {
        let (v, is_err) = item(RespValue::Error(s("ERR boom")), true);
        assert!(is_err);
        assert_eq!(v, json!({"error": "ERR boom"}));
    }

    #[test]
    fn resp3_replies_are_downgraded_first() {
        let map = RespValue::Map(vec![(
            RespValue::BulkString(Some(s("f"))),
            RespValue::Double(1.5),
        )]);
        assert_eq!(item(map, false).0, json!({"result": ["f", "1.5"]}));
        assert_eq!(item(RespValue::Null, false).0, json!({"result": null}));
        assert_eq!(
            item(RespValue::Boolean(true), false).0,
            json!({"result": 1})
        );
        let set = RespValue::Set(vec![RespValue::BulkString(Some(s("m")))]);
        assert_eq!(item(set, false).0, json!({"result": ["m"]}));
        let nested = RespValue::NestedMap(vec![(
            RespValue::BulkString(Some(s("k"))),
            RespValue::Integer(1),
        )]);
        assert_eq!(item(nested, false).0, json!({"result": [["k", 1]]}));
    }

    #[test]
    fn resp2_bytes_downgrades() {
        assert_eq!(resp2_bytes(RespValue::Null).as_ref(), b"$-1\r\n");
        assert_eq!(resp2_bytes(RespValue::Integer(2)).as_ref(), b":2\r\n");
    }
}
