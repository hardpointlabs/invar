//! Request decoding for the Redis-over-HTTP listener (RFC 0003): turns JSON
//! bodies and path-form URLs into Redis argument vectors. Everything here is
//! pure so it can be unit tested without a server.

use bytes::Bytes;
use percent_encoding::percent_decode_str;
use serde_json::value::RawValue;

/// Converts one JSON element into a bulk-string argument.
///
/// Strings become their UTF-8 bytes and numbers keep the exact decimal text
/// from the request. Booleans, `null`, arrays and objects are rejected: what
/// Upstash does with them is not settled (RFC 0003, Appendix A), and writing a
/// guess into a durable store is not acceptable.
fn element_to_arg(raw: &RawValue) -> Result<Bytes, String> {
    let text = raw.get();
    match text.as_bytes().first() {
        Some(b'"') => serde_json::from_str::<String>(text)
            .map(Bytes::from)
            .map_err(|e| format!("ERR invalid string argument: {e}")),
        Some(b'-' | b'0'..=b'9') => Ok(Bytes::copy_from_slice(text.as_bytes())),
        _ => Err("ERR command arguments must be strings or numbers".to_string()),
    }
}

fn row_to_args(row: Vec<&RawValue>) -> Result<Vec<Bytes>, String> {
    if row.is_empty() {
        return Err("ERR empty command".to_string());
    }
    row.into_iter().map(element_to_arg).collect()
}

/// Parses the body of `POST /`: one JSON array `[cmd, arg...]`.
pub fn parse_command(body: &[u8]) -> Result<Vec<Bytes>, String> {
    let row: Vec<&RawValue> = serde_json::from_slice(body)
        .map_err(|_| "ERR body must be a JSON array of strings and numbers".to_string())?;
    row_to_args(row)
}

/// Parses the body of `/pipeline` and `/multi-exec`: a JSON array of command
/// arrays.
pub fn parse_batch(body: &[u8]) -> Result<Vec<Vec<Bytes>>, String> {
    let rows: Vec<Vec<&RawValue>> = serde_json::from_slice(body)
        .map_err(|_| "ERR body must be a JSON array of command arrays".to_string())?;
    if rows.is_empty() {
        return Err("ERR empty command list".to_string());
    }
    rows.into_iter().map(row_to_args).collect()
}

/// Splits a raw request path into percent-decoded segments, skipping empty
/// ones (`//pipeline` is tolerated, as the SDKs may produce it).
pub fn path_segments(path: &str) -> Vec<Bytes> {
    path.split('/')
        .filter(|s| !s.is_empty())
        .map(|s| Bytes::from(percent_decode_str(s).collect::<Vec<u8>>()))
        .collect()
}

/// Builds the argument vector for a path-form request: segments, then the
/// body (if any) as an argument, then the query parameters as `name value`
/// pairs. `_token` is dropped, never forwarded.
///
/// Only `name=value` query parameters are accepted. Valueless and repeated
/// parameters have unsettled semantics (RFC 0003, Appendix A) and are
/// rejected.
pub fn path_form_args(
    segments: Vec<Bytes>,
    query: Option<&str>,
    body: &[u8],
) -> Result<Vec<Bytes>, String> {
    let mut args = segments;
    // Body first: `POST /set/foo?EX=100` with body `v` is `SET foo v EX 100`.
    if !body.is_empty() {
        args.push(Bytes::copy_from_slice(body));
    }
    if let Some(query) = query {
        let mut seen: Vec<Vec<u8>> = Vec::new();
        for pair in query.split('&').filter(|p| !p.is_empty()) {
            let (name, value) = pair
                .split_once('=')
                .ok_or_else(|| "ERR query parameters must have the form name=value".to_string())?;
            let name = percent_decode_str(name).collect::<Vec<u8>>();
            if name == b"_token" {
                continue;
            }
            let folded = name.to_ascii_lowercase();
            if seen.contains(&folded) {
                return Err("ERR repeated query parameters are not supported".to_string());
            }
            seen.push(folded);
            args.push(Bytes::from(name));
            args.push(Bytes::from(percent_decode_str(value).collect::<Vec<u8>>()));
        }
    }
    if args.is_empty() {
        return Err("ERR empty command".to_string());
    }
    Ok(args)
}

/// Commands that make no sense without a long-lived connection, or that block.
/// Upstash rejects the same set over REST (RFC 0003, *Command availability*).
const UNSUPPORTED: &[&[u8]] = &[
    b"select",
    b"auth",
    b"hello",
    b"client",
    b"multi",
    b"exec",
    b"discard",
    b"watch",
    b"unwatch",
    b"subscribe",
    b"psubscribe",
    b"ssubscribe",
    b"unsubscribe",
    b"punsubscribe",
    b"sunsubscribe",
    b"monitor",
    b"quit",
    b"reset",
    b"blpop",
    b"brpop",
    b"brpoplpush",
    b"blmove",
    b"blmpop",
    b"bzpopmin",
    b"bzpopmax",
    b"bzmpop",
    b"xreadgroup",
];

/// Returns the error to answer with if `args` is not allowed over HTTP.
pub fn check_supported(args: &[Bytes]) -> Result<(), String> {
    let name = args[0].to_ascii_lowercase();
    let blocking_xread =
        name == b"xread" && args[1..].iter().any(|a| a.eq_ignore_ascii_case(b"block"));
    if UNSUPPORTED.contains(&name.as_slice()) || blocking_xread {
        return Err(format!(
            "ERR unsupported command '{}'",
            String::from_utf8_lossy(&args[0])
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strs(v: &[Bytes]) -> Vec<String> {
        v.iter()
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .collect()
    }

    #[test]
    fn json_command_preserves_number_text() {
        let args = parse_command(br#"["set","k",100,1.50,9223372036854775807]"#).unwrap();
        assert_eq!(
            strs(&args),
            ["set", "k", "100", "1.50", "9223372036854775807"]
        );
    }

    #[test]
    fn json_command_rejects_unsettled_types() {
        for body in [
            &br#"["set","k",null]"#[..],
            br#"["set","k",true]"#,
            br#"["set","k",["a"]]"#,
            br#"["set","k",{"a":1}]"#,
            br#"{"a":1}"#,
            b"not json",
            b"[]",
        ] {
            assert!(
                parse_command(body).is_err(),
                "{}",
                String::from_utf8_lossy(body)
            );
        }
    }

    #[test]
    fn json_string_unescapes() {
        let args = parse_command(br#"["set","k","a\"\u00e9\n"]"#).unwrap();
        assert_eq!(args[2].as_ref(), "a\"é\n".as_bytes());
    }

    #[test]
    fn batch_parses_rows_and_rejects_bad_shapes() {
        let rows = parse_batch(br#"[["SET","a",1],["GET","a"]]"#).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(strs(&rows[0]), ["SET", "a", "1"]);
        assert!(parse_batch(b"[]").is_err());
        assert!(parse_batch(br#"[["GET","a"],[]]"#).is_err());
        assert!(parse_batch(br#"["GET","a"]"#).is_err());
    }

    #[test]
    fn path_segments_decode_and_skip_empties() {
        let segs = path_segments("//set/a%2Fb/%00%FF");
        assert_eq!(segs[0].as_ref(), b"set");
        assert_eq!(segs[1].as_ref(), b"a/b");
        assert_eq!(segs[2].as_ref(), &[0, 0xff]);
        assert_eq!(segs.len(), 3);
    }

    #[test]
    fn path_form_appends_body_then_query() {
        let args =
            path_form_args(path_segments("/set/foo"), Some("EX=100&_token=x"), b"val").unwrap();
        assert_eq!(strs(&args), ["set", "foo", "val", "EX", "100"]);
    }

    #[test]
    fn path_form_rejects_valueless_and_repeated_params() {
        assert!(path_form_args(path_segments("/set/a"), Some("NX"), b"").is_err());
        assert!(path_form_args(path_segments("/set/a"), Some("EX=1&ex=2"), b"").is_err());
        assert!(path_form_args(vec![], None, b"").is_err());
    }

    #[test]
    fn unsupported_commands() {
        let a = |v: &[&str]| {
            v.iter()
                .map(|s| Bytes::from(s.to_string()))
                .collect::<Vec<_>>()
        };
        assert!(check_supported(&a(&["GET", "k"])).is_ok());
        assert!(check_supported(&a(&["xread", "streams", "s", "0"])).is_ok());
        for cmd in [
            a(&["Select", "1"]),
            a(&["blpop", "l", "0"]),
            a(&["xread", "BLOCK", "0", "streams", "s", "$"]),
            a(&["multi"]),
        ] {
            let err = check_supported(&cmd).unwrap_err();
            assert!(err.starts_with("ERR unsupported command"), "{err}");
        }
    }
}
