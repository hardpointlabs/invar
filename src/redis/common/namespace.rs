//! Physical namespaces of compound values (RFC 0001).
//!
//! The elements of a hash, set, sorted set, list, stream or bloom filter live
//! under internal keys of the form `<prefix><suffix>`. Historically the prefix
//! was derived from the key's *name* (`-<db>:<name>`), which made the name the
//! physical address of the data. A **namespace** decouples the two: the
//! sentinel (the entry under the public key) may record the prefix its
//! elements live under, so moving the sentinel moves the value.
//!
//! A sentinel that records a namespace has the high bit (`NAMESPACE_FLAG`)
//! set in its type byte and ends with a trailer:
//!
//! ```text
//! <type specific payload> || prefix (n bytes) || n (u32, big endian)
//! ```
//!
//! A sentinel without the flag is a legacy one and its prefix is derived from
//! the name as before. Setting the bit in the type byte makes a binary that
//! predates namespaces fail closed (`WRONGTYPE`) on such a key instead of
//! silently addressing the wrong prefix.
//!
//! Commands never parse the trailer themselves: they ask `resolve_prefix`
//! (or `prefix_of` when they already hold the sentinel) for the prefix to
//! use, decode the sentinel payload with `payload`, compare types with
//! `base_type`, and write the sentinel back with `write_sentinel`, which
//! keeps the trailer, the TTL and refuses to drop a namespace by accident.

use std::ops::Deref;

use kv::kv::{Entry, Error as KvError, Item, Tx};

use crate::common::op::DbError;

/// High bit of a sentinel's type byte: the value ends with a namespace trailer.
pub(crate) const NAMESPACE_FLAG: u8 = 0x80;

/// Size of the length field that ends a namespace trailer.
const TRAILER_LEN_SIZE: usize = 4;

/// The type byte without the namespace flag; compare this, not the raw byte,
/// against a `ValueType`.
pub(crate) fn base_type(metadata: u8) -> u8 {
    metadata & !NAMESPACE_FLAG
}

/// Whether a sentinel with this type byte records a namespace.
pub(crate) fn is_namespaced(metadata: u8) -> bool {
    metadata & NAMESPACE_FLAG != 0
}

/// The prefix under which one compound value's elements are stored, and where
/// it came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ElementPrefix {
    bytes: Vec<u8>,
    /// `true` when the sentinel records this prefix, `false` when it is the
    /// legacy prefix derived from the key's name.
    explicit: bool,
}

impl ElementPrefix {
    /// The prefix derived from the key's name, for a sentinel that records none.
    pub(crate) fn legacy(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            explicit: false,
        }
    }

    /// A prefix recorded in (or about to be recorded in) a sentinel.
    pub(crate) fn explicit(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            explicit: true,
        }
    }

    pub(crate) fn is_explicit(&self) -> bool {
        self.explicit
    }
}

impl Deref for ElementPrefix {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.bytes
    }
}

fn corrupt() -> DbError {
    DbError::Redis("internal error: corrupt namespace trailer".into())
}

/// Splits the value of a namespaced sentinel into `(payload, prefix)`.
fn split_trailer(value: &[u8]) -> Result<(&[u8], &[u8]), DbError> {
    let len_at = value
        .len()
        .checked_sub(TRAILER_LEN_SIZE)
        .ok_or_else(corrupt)?;
    let len = u32::from_be_bytes(value[len_at..].try_into().expect("4 bytes")) as usize;
    let prefix_at = len_at.checked_sub(len).ok_or_else(corrupt)?;
    Ok((&value[..prefix_at], &value[prefix_at..len_at]))
}

/// The type specific payload of a sentinel, without any namespace trailer.
pub(crate) fn payload(item: &Item) -> Result<&[u8], DbError> {
    if is_namespaced(item.metadata()) {
        Ok(split_trailer(item.value())?.0)
    } else {
        Ok(item.value())
    }
}

/// The prefix to use for the compound value whose sentinel is `item`: the one
/// it records, or `legacy` (derived from the key's name) when it records none.
pub(crate) fn prefix_of(item: &Item, legacy: &[u8]) -> Result<ElementPrefix, DbError> {
    if is_namespaced(item.metadata()) {
        Ok(ElementPrefix::explicit(
            split_trailer(item.value())?.1.to_vec(),
        ))
    } else {
        Ok(ElementPrefix::legacy(legacy.to_vec()))
    }
}

/// The same prefix with its db number replaced: prefixes have the form
/// `<kind><db>:<rest>` (`-0:name` for name-derived ones, `+0:<id>` for
/// namespaces). `None` if `prefix` is not of that form.
pub(crate) fn with_db(prefix: &[u8], db: i32) -> Option<Vec<u8>> {
    let kind = *prefix.first()?;
    let colon = prefix.iter().position(|&b| b == b':')?;
    let digits = &prefix[1..colon];
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let mut out = vec![kind];
    out.extend_from_slice(db.to_string().as_bytes());
    out.extend_from_slice(&prefix[colon..]);
    Some(out)
}

/// Number of random bytes that identify a namespace.
const NAMESPACE_ID_LEN: usize = 16;

/// A fresh namespace for a value that is being created: `+<db>:` followed by
/// 128 random bits, so it cannot collide with the prefix of any other value,
/// with a name-derived prefix (those start with `-`) or with a public key
/// (those start with a digit). `legacy` is the name-derived prefix
/// (`-<db>:<name>`) of the key being created; it only supplies the db number.
pub(crate) fn new_namespace(legacy: &[u8]) -> Result<ElementPrefix, DbError> {
    // `-<db>:...` -> `+<db>:`
    let colon = legacy
        .iter()
        .position(|&b| b == b':')
        .ok_or_else(corrupt_prefix)?;
    let db = legacy
        .get(1..colon)
        .filter(|d| !d.is_empty() && d.iter().all(u8::is_ascii_digit));
    let db = db.ok_or_else(corrupt_prefix)?;
    let mut prefix = Vec::with_capacity(colon + 1 + NAMESPACE_ID_LEN);
    prefix.push(b'+');
    prefix.extend_from_slice(db);
    prefix.push(b':');
    let mut id = [0u8; NAMESPACE_ID_LEN];
    rand::rand_core::Rng::fill_bytes(&mut rand::rng(), &mut id);
    prefix.extend_from_slice(&id);
    Ok(ElementPrefix::explicit(prefix))
}

fn corrupt_prefix() -> DbError {
    DbError::Redis("internal error: malformed element prefix".into())
}

/// Like `resolve_prefix` for a command that may create the value: a key with
/// no live sentinel gets a brand new namespace, which the command must then
/// record by writing the sentinel with it. An expired sentinel counts as
/// absent, so the elements it left behind can never be mistaken for the new
/// value's.
pub(crate) async fn resolve_prefix_for_write(
    tx: &dyn Tx,
    public_key: &[u8],
    legacy: &[u8],
) -> Result<ElementPrefix, DbError> {
    match tx.get(public_key).await {
        Ok(item) => prefix_of(&item, legacy),
        Err(KvError::KeyNotFound) => new_namespace(legacy),
        Err(e) => Err(e.into()),
    }
}

/// Reads the sentinel at `public_key` and returns the prefix of its elements.
/// A missing (or expired) key has no namespace yet, so `legacy` is returned.
pub(crate) async fn resolve_prefix(
    tx: &dyn Tx,
    public_key: &[u8],
    legacy: &[u8],
) -> Result<ElementPrefix, DbError> {
    match tx.get(public_key).await {
        Ok(item) => prefix_of(&item, legacy),
        Err(KvError::KeyNotFound) => Ok(ElementPrefix::legacy(legacy.to_vec())),
        Err(e) => Err(e.into()),
    }
}

/// Encodes a sentinel: `payload`, plus a trailer and the flag when `prefix`
/// is explicit. Returns the value and the type byte to store it with.
pub(crate) fn encode_sentinel(
    mut payload: Vec<u8>,
    type_byte: u8,
    prefix: &ElementPrefix,
) -> (Vec<u8>, u8) {
    if !prefix.is_explicit() {
        return (payload, type_byte);
    }
    payload.extend_from_slice(prefix);
    payload.extend_from_slice(&(prefix.len() as u32).to_be_bytes());
    (payload, type_byte | NAMESPACE_FLAG)
}

/// Writes the sentinel of a compound value, keeping the key's TTL.
///
/// `prefix` is the prefix the value's elements are stored under. The sentinel
/// records it iff it is explicit. Rewriting a namespaced sentinel with a
/// legacy prefix would strand the value's elements, so that is refused.
pub(crate) async fn write_sentinel(
    tx: &dyn Tx,
    public_key: &[u8],
    payload: Vec<u8>,
    type_byte: u8,
    prefix: &ElementPrefix,
) -> Result<(), DbError> {
    let existing = match tx.get(public_key).await {
        Ok(item) => Some(item),
        Err(KvError::KeyNotFound) => None,
        Err(e) => return Err(e.into()),
    };
    if let Some(item) = &existing {
        if is_namespaced(item.metadata()) && !prefix.is_explicit() {
            return Err(DbError::Redis(
                "internal error: namespaced sentinel rewritten without its namespace".into(),
            ));
        }
    }
    let (value, metadata) = encode_sentinel(payload, type_byte, prefix);
    let mut entry = Entry::new(public_key.to_vec(), value).metadata(metadata);
    if let Some(item) = &existing {
        entry = entry.keep_ttl_of(item);
    }
    tx.set(entry).map_err(DbError::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trailer_round_trips_and_the_flag_is_only_set_for_explicit_prefixes() {
        let prefix = ElementPrefix::explicit(b"+0:abcdefgh".to_vec());
        let (value, meta) = encode_sentinel(vec![0, 0, 0, 7], 4, &prefix);
        assert_eq!(meta, 4 | NAMESPACE_FLAG);
        assert_eq!(base_type(meta), 4);
        assert!(is_namespaced(meta));
        let (payload, recorded) = split_trailer(&value).unwrap();
        assert_eq!(
            (payload, recorded),
            (&[0u8, 0, 0, 7][..], &b"+0:abcdefgh"[..])
        );

        let legacy = ElementPrefix::legacy(b"-0:name".to_vec());
        assert_eq!(encode_sentinel(vec![1, 2], 4, &legacy), (vec![1, 2], 4));
        assert!(!is_namespaced(4));
    }

    #[test]
    fn with_db_swaps_the_db_number_of_both_prefix_kinds() {
        assert_eq!(with_db(b"+0:abc", 12), Some(b"+12:abc".to_vec()));
        assert_eq!(with_db(b"-3:my:key", 0), Some(b"-0:my:key".to_vec()));
        assert_eq!(with_db(b"abc", 1), None);
        assert_eq!(with_db(b"-x:abc", 1), None);
        assert_eq!(with_db(b"-:abc", 1), None);
        assert_eq!(with_db(b"", 1), None);
    }

    #[test]
    fn new_namespaces_are_unique_db_scoped_and_distinct_from_name_derived_prefixes() {
        let a = new_namespace(b"-3:my:key").unwrap();
        let b = new_namespace(b"-3:my:key").unwrap();
        assert!(a.is_explicit());
        assert_ne!(&*a, &*b, "128 random bits must not repeat");
        assert!(a.starts_with(b"+3:"), "kind '+' and the db of the key");
        assert_eq!(a.len(), b"+3:".len() + NAMESPACE_ID_LEN);
        // The name does not leak into the namespace.
        assert!(!a.windows(6).any(|w| w == b"my:key"));
        assert!(new_namespace(b"nonsense").is_err());
        assert!(new_namespace(b"-x:name").is_err());
    }

    #[test]
    fn a_corrupt_trailer_is_an_error_not_a_panic() {
        assert!(split_trailer(b"").is_err());
        assert!(split_trailer(&[0, 0]).is_err());
        // Claims a 9 byte prefix in a value that has only 6 bytes before the length.
        assert!(split_trailer(&[1, 2, 3, 4, 5, 6, 0, 0, 0, 9]).is_err());
    }

    #[test]
    fn an_empty_payload_and_an_empty_prefix_are_representable() {
        let (value, _) = encode_sentinel(vec![], 4, &ElementPrefix::explicit(vec![]));
        assert_eq!(split_trailer(&value).unwrap(), (&[][..], &[][..]));
    }
}
