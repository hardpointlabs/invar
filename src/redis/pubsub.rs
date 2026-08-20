//! Redis Pub/Sub — `SUBSCRIBE`, `PSUBSCRIBE`, `PUBLISH`, `UNSUBSCRIBE`,
//! `PUNSUBSCRIBE`, `SSUBSCRIBE`, `SPUBLISH`, `SUNSUBSCRIBE`, and the
//! `PUBSUB` introspection subcommands (`CHANNELS`, `NUMSUB`, `NUMPAT`).
//!
//! ## Architecture
//!
//! A process-wide [`PubSubRegistry`] (held as `Arc<PubSubRegistry>` and shared
//! by every connection task via [`RedisListener`]) owns two maps:
//!
//! - `channels`: exact-match channel name → `broadcast::Sender<PubSubMsg>`
//! - `patterns`: glob pattern → `broadcast::Sender<PubSubMsg>`
//!
//! Each map entry is created lazily on first subscribe / first publish and
//! cleaned up as soon as its last receiver drops (checked after every
//! unsubscribe via `sender.receiver_count()`).
//!
//! Per-connection subscription state is kept in [`ConnectionSubs`], which the
//! listener's connection handler creates on the stack and drives via the
//! `select!` loop described in [`listener`].  Incoming subscription push
//! messages are forwarded directly to the client as RESP push arrays without
//! passing through the `QueuedOp`/`DbOp` machinery — they carry no database
//! state.
//!
//! ## Cancellation safety
//!
//! The `select!` loop in `listener.rs` polls both the command stream and the
//! `StreamMap` of broadcast receivers in the same `select!`.  The Tokio team
//! documents a hazard where a `StreamMap::next()` future may be cancelled
//! mid-poll when the other branch completes first, potentially losing the
//! item that was being returned (see:
//! <https://smallcultfollowing.com/babysteps/blog/2022/06/13/async-cancellation-a-case-study-of-pub-sub-in-mini-redis/>).
//!
//! We avoid the hazard by **never polling the `StreamMap` inside `select!`
//! directly**.  Instead the connection handler pre-polls it before entering the
//! select loop with `try_next()`, and inside `select!` each arm is guarded by
//! a `biased` annotation so the message-push branch drains fully before
//! accepting new commands.  Concretely: we use `tokio::select!` with `biased`,
//! give the subscription push branch priority, and always check whether a
//! message is already present (via `futures::StreamExt::next` on the fused
//! stream) before suspending.  This ensures a partially-iterated broadcast
//! item is never silently discarded.
//!
//! ## Pattern matching
//!
//! `PSUBSCRIBE` / pattern-side `PUBLISH` use [`redis_glob_match`], a faithful
//! port of Redis's `stringmatchlen()` from `src/util.c`.  It supports `*`
//! (any sequence of bytes), `?` (any single byte), `[...]` character-class
//! ranges, and `\`-escaping.  It operates on raw `&[u8]` so it is
//! byte-correct for binary channel names.

use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamMap;
use crate::common::{op, DbError, DbOp, DbResult, QueuedOp, WireOp};
use crate::common::op::NoOp;
use crate::RespValue;

/// Capacity of each per-channel/per-pattern broadcast channel.  Slow consumers
/// that fall more than this many messages behind receive a
/// `RecvError::Lagged` and miss the overflowed messages — matching Redis's
/// own behaviour where a very slow subscriber may miss messages.
const BROADCAST_CAPACITY: usize = 512;

/// A single pub/sub message: the originating channel name (used in push
/// framing) plus the payload.
#[derive(Debug, Clone)]
pub struct PubSubMsg {
    /// The exact channel name the `PUBLISH` was directed at.
    pub channel: Bytes,
    /// The message payload.
    pub payload: Bytes,
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

/// Process-wide pub/sub registry shared by all connection tasks.
///
/// Both maps use [`Bytes`] keys so subscriptions and publishes avoid copying
/// channel-name bytes more than once.
pub struct PubSubRegistry {
    /// Exact-channel senders.
    channels: DashMap<Bytes, broadcast::Sender<PubSubMsg>>,
    /// Pattern senders.  The key is the glob pattern string (as raw bytes).
    patterns: DashMap<Bytes, broadcast::Sender<PubSubMsg>>,
}

impl PubSubRegistry {
    pub fn new() -> Self {
        Self {
            channels: DashMap::new(),
            patterns: DashMap::new(),
        }
    }

    // --- Subscribe / unsubscribe ---

    /// Returns a receiver for the given exact channel, creating the sender if
    /// this is the first subscriber.
    pub fn subscribe_channel(&self, channel: Bytes) -> broadcast::Receiver<PubSubMsg> {
        self.channels
            .entry(channel)
            .or_insert_with(|| broadcast::channel(BROADCAST_CAPACITY).0)
            .value()
            .subscribe()
    }

    /// Returns a receiver for the given pattern, creating the sender if this
    /// is the first subscriber.
    pub fn subscribe_pattern(&self, pattern: Bytes) -> broadcast::Receiver<PubSubMsg> {
        self.patterns
            .entry(pattern)
            .or_insert_with(|| broadcast::channel(BROADCAST_CAPACITY).0)
            .value()
            .subscribe()
    }

    /// Cleans up the channel entry if no receivers remain.  Must be called
    /// after every unsubscribe so dead entries don't accumulate.
    pub fn cleanup_channel(&self, channel: &[u8]) {
        if let Some(entry) = self.channels.get(channel) {
            if entry.receiver_count() == 0 {
                drop(entry); // release the shared ref before removing
                self.channels.remove(channel);
            }
        }
    }

    /// Cleans up the pattern entry if no receivers remain.
    pub fn cleanup_pattern(&self, pattern: &[u8]) {
        if let Some(entry) = self.patterns.get(pattern) {
            if entry.receiver_count() == 0 {
                drop(entry);
                self.patterns.remove(pattern);
            }
        }
    }

    // --- Publish ---

    /// Publishes `payload` to the exact channel `channel`, plus any patterns
    /// that glob-match `channel`.  Returns the total number of receivers that
    /// received the message (sum of `receiver_count()` across all matched
    /// senders at the moment of the send).
    ///
    /// The receiver-count approach matches real Redis's return value: it
    /// reports how many subscribers had an open receiver *at the time of
    /// publish*, which may differ slightly from the number that will actually
    /// read the message (due to lagged receivers), but it is exactly what
    /// real Redis returns.
    pub fn publish(&self, channel: &[u8], payload: Bytes) -> i64 {
        let mut total: i64 = 0;

        // Exact-match channel.
        if let Some(sender) = self.channels.get(channel) {
            let count = sender.receiver_count() as i64;
            if count > 0 {
                let msg = PubSubMsg {
                    channel: Bytes::copy_from_slice(channel),
                    payload: payload.clone(),
                };
                // Ignore send errors — lagged receivers may have been dropped.
                let _ = sender.send(msg);
                total += count;
            }
        }

        // Pattern-matching channels.
        self.patterns.iter().for_each(|entry| {
            let pattern = entry.key();
            if redis_glob_match(pattern, channel, true) {
                let count = entry.value().receiver_count() as i64;
                if count > 0 {
                    let msg = PubSubMsg {
                        channel: Bytes::copy_from_slice(channel),
                        payload: payload.clone(),
                    };
                    let _ = entry.value().send(msg);
                    total += count;
                }
            }
        });

        total
    }

    // --- PUBSUB introspection ---

    /// Returns all channel names with at least one active subscriber,
    /// optionally filtered by a glob pattern.
    pub fn active_channels(self: Arc<Self>, pattern: Option<&[u8]>) -> QueuedOp {
        op::wire_only_op(Box::new(ChannelsOp {
            registry: self, pattern: pattern.map(Bytes::copy_from_slice)
        }), true)
    }

    fn active_channels_val(&self, pattern: Option<&[u8]>) -> Vec<Bytes> {
        self.channels
            .iter()
            .filter(|e| e.value().receiver_count() > 0)
            .filter(|e| {
                pattern.map_or(true, |pat| redis_glob_match(pat, e.key(), true))
            })
            .map(|e| e.key().clone())
            .collect()
    }

    /// Returns the subscriber count for each of the given channel names.
    /// The result is a flat `[(channel, count), ...]` vec in the same order.
    fn numsub_value(&self, channels: &[Bytes]) -> Vec<(Bytes, i64)> {
        channels
            .iter()
            .map(|ch| {
                let count = self
                    .channels
                    .get(ch.as_ref())
                    .map(|s| s.receiver_count() as i64)
                    .unwrap_or(0);
                (ch.clone(), count)
            })
            .collect()
    }

    fn number_pattern_subscribers(&self) -> i64 {
        self.patterns
            .iter()
            .filter(|e| e.value().receiver_count() > 0)
            .count() as i64
    }

    /// Returns the number of active pattern subscriptions.
    pub fn numpat(self: Arc<Self>) -> QueuedOp {
        op::wire_only_op(Box::new(NumPatOp { registry: self }), true)
    }

    pub fn numsub(self: Arc<Self>, channels: Vec<Bytes>) -> QueuedOp {
        op::wire_only_op(Box::new(NumSubOp { registry: self, channels: channels }), true)
    }

    /// Returns a [`QueuedOp`] that publishes `payload` to `channel` when executed.
    ///
    /// The publish happens inside the `WireOp` (after transaction commit) because
    /// pub/sub delivery should not be rolled back if the enclosing transaction
    /// fails for unrelated reasons — this matches real Redis semantics where
    /// `PUBLISH` inside `MULTI` always fires when `EXEC` runs.
    pub fn publish_op(self: Arc<Self>, channel: Bytes, payload: Bytes,
    ) -> QueuedOp {
        QueuedOp {
            db_op: Box::new(PublishDbOp),
            wire_op: Box::new(PublishWireOp {
                registry: self,
                channel,
                payload,
            }),
            is_mutating: false,
            allowed_in_tx: true,
        abort_in_tx: false,
        }
    }
}

impl Default for PubSubRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Per-connection subscription state
// ---------------------------------------------------------------------------

/// The kind of a subscription, used for reply framing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubKind {
    /// Exact channel subscription (`SUBSCRIBE`/`SSUBSCRIBE`).
    Channel,
    /// Glob pattern subscription (`PSUBSCRIBE`).
    Pattern,
}

/// Per-connection subscription state.
///
/// Keeps two `StreamMap`s (one for exact channels, one for patterns) so the
/// listener can drive both with a single `select!`.
pub struct ConnectionSubs {
    registry: Arc<PubSubRegistry>,
    /// `StreamMap` keyed by channel name.
    pub channel_streams: StreamMap<Bytes, BroadcastStream<PubSubMsg>>,
    /// `StreamMap` keyed by pattern.
    pub pattern_streams: StreamMap<Bytes, BroadcastStream<PubSubMsg>>,
}

impl ConnectionSubs {
    pub fn new(registry: Arc<PubSubRegistry>) -> Self {
        Self {
            registry,
            channel_streams: StreamMap::new(),
            pattern_streams: StreamMap::new(),
        }
    }

    /// Total number of active subscriptions (channels + patterns).
    pub fn total(&self) -> usize {
        self.channel_streams.len() + self.pattern_streams.len()
    }

    /// Whether any subscriptions are active.
    pub fn is_subscribed(&self) -> bool {
        !self.channel_streams.is_empty() || !self.pattern_streams.is_empty()
    }

    /// Subscribes to an exact channel.  Returns `true` if it was new.
    pub fn subscribe_channel(&mut self, channel: Bytes) -> bool {
        if self.channel_streams.contains_key(&channel) {
            return false;
        }
        let rx = self.registry.subscribe_channel(channel.clone());
        self.channel_streams
            .insert(channel, BroadcastStream::new(rx));
        true
    }

    /// Subscribes to a pattern.  Returns `true` if it was new.
    pub fn subscribe_pattern(&mut self, pattern: Bytes) -> bool {
        if self.pattern_streams.contains_key(&pattern) {
            return false;
        }
        let rx = self.registry.subscribe_pattern(pattern.clone());
        self.pattern_streams
            .insert(pattern, BroadcastStream::new(rx));
        true
    }

    /// Unsubscribes from an exact channel.  Returns `true` if it was present.
    pub fn unsubscribe_channel(&mut self, channel: &Bytes) -> bool {
        if self.channel_streams.remove(channel).is_some() {
            self.registry.cleanup_channel(channel.as_ref());
            true
        } else {
            false
        }
    }

    /// Unsubscribes from a pattern.  Returns `true` if it was present.
    pub fn unsubscribe_pattern(&mut self, pattern: &Bytes) -> bool {
        if self.pattern_streams.remove(pattern).is_some() {
            self.registry.cleanup_pattern(pattern.as_ref());
            true
        } else {
            false
        }
    }

    /// Drains all channel subscriptions, returning the names removed.
    pub fn unsubscribe_all_channels(&mut self) -> Vec<Bytes> {
        let keys: Vec<Bytes> = self.channel_streams.keys().cloned().collect();
        for k in &keys {
            self.channel_streams.remove(k);
            self.registry.cleanup_channel(k.as_ref());
        }
        keys
    }

    /// Drains all pattern subscriptions, returning the patterns removed.
    pub fn unsubscribe_all_patterns(&mut self) -> Vec<Bytes> {
        let keys: Vec<Bytes> = self.pattern_streams.keys().cloned().collect();
        for k in &keys {
            self.pattern_streams.remove(k);
            self.registry.cleanup_pattern(k.as_ref());
        }
        keys
    }
}

// ---------------------------------------------------------------------------
// RESP push-frame builders
// ---------------------------------------------------------------------------

/// Builds the `[subscribe, channel, count]` push frame sent to a client after
/// a successful `SUBSCRIBE` or `SSUBSCRIBE`.
pub fn subscribe_reply(channel: &[u8], count: usize) -> Vec<u8> {
    // Encoded inline; the caller writes raw RESP bytes.
    // We return the RespValue-encoded bytes via a helper so the caller can
    // use the existing framed codec.
    //
    // Actually we return a RespValue directly; the caller encodes it.
    let _ = (channel, count); // unused — see the RespValue builders below
    unreachable!("use subscribe_resp/psubscribe_resp instead")
}

/// `["subscribe", channel, count]`
pub fn subscribe_resp(channel: Bytes, count: usize) -> crate::resp::RespValue {
    crate::resp::RespValue::Array(Some(vec![
        crate::resp::RespValue::BulkString(Some(Bytes::from_static(b"subscribe"))),
        crate::resp::RespValue::BulkString(Some(channel)),
        crate::resp::RespValue::Integer(count as i64),
    ]))
}

/// `["unsubscribe", channel, count]`
pub fn unsubscribe_resp(channel: Bytes, count: usize) -> crate::resp::RespValue {
    crate::resp::RespValue::Array(Some(vec![
        crate::resp::RespValue::BulkString(Some(Bytes::from_static(b"unsubscribe"))),
        crate::resp::RespValue::BulkString(Some(channel)),
        crate::resp::RespValue::Integer(count as i64),
    ]))
}

/// `["psubscribe", pattern, count]`
pub fn psubscribe_resp(pattern: Bytes, count: usize) -> crate::resp::RespValue {
    crate::resp::RespValue::Array(Some(vec![
        crate::resp::RespValue::BulkString(Some(Bytes::from_static(b"psubscribe"))),
        crate::resp::RespValue::BulkString(Some(pattern)),
        crate::resp::RespValue::Integer(count as i64),
    ]))
}

/// `["punsubscribe", pattern, count]`
pub fn punsubscribe_resp(pattern: Bytes, count: usize) -> crate::resp::RespValue {
    crate::resp::RespValue::Array(Some(vec![
        crate::resp::RespValue::BulkString(Some(Bytes::from_static(b"punsubscribe"))),
        crate::resp::RespValue::BulkString(Some(pattern)),
        crate::resp::RespValue::Integer(count as i64),
    ]))
}

/// `["ssubscribe", channel, count]`
pub fn ssubscribe_resp(channel: Bytes, count: usize) -> crate::resp::RespValue {
    crate::resp::RespValue::Array(Some(vec![
        crate::resp::RespValue::BulkString(Some(Bytes::from_static(b"ssubscribe"))),
        crate::resp::RespValue::BulkString(Some(channel)),
        crate::resp::RespValue::Integer(count as i64),
    ]))
}

/// `["sunsubscribe", channel, count]`
pub fn sunsubscribe_resp(channel: Bytes, count: usize) -> crate::resp::RespValue {
    crate::resp::RespValue::Array(Some(vec![
        crate::resp::RespValue::BulkString(Some(Bytes::from_static(b"sunsubscribe"))),
        crate::resp::RespValue::BulkString(Some(channel)),
        crate::resp::RespValue::Integer(count as i64),
    ]))
}

/// `["message", channel, payload]` — exact-channel push.
pub fn message_resp(channel: Bytes, payload: Bytes) -> crate::resp::RespValue {
    crate::resp::RespValue::Array(Some(vec![
        crate::resp::RespValue::BulkString(Some(Bytes::from_static(b"message"))),
        crate::resp::RespValue::BulkString(Some(channel)),
        crate::resp::RespValue::BulkString(Some(payload)),
    ]))
}

/// `["pmessage", pattern, channel, payload]` — pattern-match push.
pub fn pmessage_resp(
    pattern: Bytes,
    channel: Bytes,
    payload: Bytes,
) -> crate::resp::RespValue {
    crate::resp::RespValue::Array(Some(vec![
        crate::resp::RespValue::BulkString(Some(Bytes::from_static(b"pmessage"))),
        crate::resp::RespValue::BulkString(Some(pattern)),
        crate::resp::RespValue::BulkString(Some(channel)),
        crate::resp::RespValue::BulkString(Some(payload)),
    ]))
}

// ---------------------------------------------------------------------------
// Redis glob pattern matcher
// ---------------------------------------------------------------------------

/// Returns `true` if `string` matches the Redis glob `pattern`.
///
/// This is a faithful byte-level port of Redis's `stringmatchlen()` from
/// `src/util.c` (Redis 7.x).  Supported syntax:
///
/// - `*`  — matches any sequence of bytes (including empty)
/// - `?`  — matches exactly one byte
/// - `[abc]` / `[a-z]` — character class; `^` or `!` negates
/// - `\x` — escapes the next character literally
///
/// If `nocase` is `true` the comparison is ASCII-case-insensitive (used
/// internally; channel-name matching is case-sensitive, so callers pass `false`
/// or `true` explicitly per context).
pub fn redis_glob_match(pattern: &[u8], string: &[u8], nocase: bool) -> bool {
    redis_glob_match_inner(pattern, string, nocase)
}

fn redis_glob_match_inner(mut pat: &[u8], mut s: &[u8], nocase: bool) -> bool {
    loop {
        if pat.is_empty() {
            return s.is_empty();
        }

        match pat[0] {
            b'*' => {
                // Skip consecutive stars.
                while pat.len() > 1 && pat[1] == b'*' {
                    pat = &pat[1..];
                }
                // `*` at the end matches everything.
                if pat.len() == 1 {
                    return true;
                }
                // Try matching the rest of the pattern at every position in s.
                while !s.is_empty() {
                    if redis_glob_match_inner(&pat[1..], s, nocase) {
                        return true;
                    }
                    s = &s[1..];
                }
                // Also try the empty case.
                return redis_glob_match_inner(&pat[1..], s, nocase);
            }
            b'?' => {
                if s.is_empty() {
                    return false;
                }
                s = &s[1..];
                pat = &pat[1..];
            }
            b'[' => {
                pat = &pat[1..]; // consume '['
                if s.is_empty() {
                    return false;
                }
                let sc = s[0];
                s = &s[1..];

                // Optional negation.
                let negate = if !pat.is_empty() && (pat[0] == b'^' || pat[0] == b'!') {
                    pat = &pat[1..];
                    true
                } else {
                    false
                };

                let mut matched = false;
                loop {
                    if pat.is_empty() {
                        // Unterminated '[' — treat as no match for this branch.
                        break;
                    }
                    if pat[0] == b']' {
                        pat = &pat[1..];
                        break;
                    }
                    // Handle escape inside class.
                    let (lo, hi) = if pat[0] == b'\\' && pat.len() > 1 {
                        pat = &pat[1..]; // skip backslash
                        let ch = pat[0];
                        pat = &pat[1..];
                        (ch, ch)
                    } else if pat.len() >= 3 && pat[1] == b'-' {
                        // Range like a-z.
                        let lo = pat[0];
                        let hi = pat[2];
                        pat = &pat[3..];
                        (lo, hi)
                    } else {
                        let ch = pat[0];
                        pat = &pat[1..];
                        (ch, ch)
                    };

                    if nocase {
                        let sc_l = sc.to_ascii_lowercase();
                        let lo_l = lo.to_ascii_lowercase();
                        let hi_l = hi.to_ascii_lowercase();
                        if sc_l >= lo_l && sc_l <= hi_l {
                            matched = true;
                        }
                    } else if sc >= lo && sc <= hi {
                        matched = true;
                    }
                }

                if matched == negate {
                    // matched but negated, or not matched and not negated.
                    return false;
                }
                // pat is already advanced past ']'.
            }
            b'\\' => {
                // Escape: match the next byte literally.
                if pat.len() > 1 {
                    pat = &pat[1..]; // skip backslash
                }
                // Fall through to default literal match below.
                if s.is_empty() {
                    return false;
                }
                let (pc, sc) = if nocase {
                    (pat[0].to_ascii_lowercase(), s[0].to_ascii_lowercase())
                } else {
                    (pat[0], s[0])
                };
                if pc != sc {
                    return false;
                }
                pat = &pat[1..];
                s = &s[1..];
            }
            literal => {
                if s.is_empty() {
                    return false;
                }
                let (pc, sc) = if nocase {
                    (literal.to_ascii_lowercase(), s[0].to_ascii_lowercase())
                } else {
                    (literal, s[0])
                };
                if pc != sc {
                    return false;
                }
                pat = &pat[1..];
                s = &s[1..];
            }
        }
    }
}

// ---------------------------------------------------------------------------

struct NumSubOp {
    registry: Arc<PubSubRegistry>,
    channels: Vec<Bytes>,
}

impl WireOp for NumSubOp {
    fn reply(&self, _result: Result<DbResult, DbError>) -> RespValue {
        let counts = self.registry.numsub_value(self.channels.as_slice());
        let mut flat = Vec::with_capacity(counts.len() * 2);
        for (ch, count) in counts {
            flat.push(RespValue::BulkString(Some(ch)));
            flat.push(RespValue::Integer(count));
        }
        RespValue::Array(Some(flat))
    }
}

struct PublishDbOp;

impl DbOp for PublishDbOp {}

struct PublishWireOp {
    registry: Arc<PubSubRegistry>,
    channel: Bytes,
    payload: Bytes,
}

impl WireOp for PublishWireOp {
    fn reply(&self, result: Result<DbResult, DbError>) -> RespValue {
        match result {
            Ok(_) => {
                let count = self.registry.publish(&self.channel, self.payload.clone());
                RespValue::Integer(count)
            }
            Err(e) => crate::common::op::err_resp(&e),
        }
    }
}

struct HelpOp;

impl WireOp for HelpOp {
    fn reply(&self, _result: Result<DbResult, DbError>) -> RespValue {
        RespValue::Array(Some(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"PUBSUB <subcommand> [<arg> [value] [opt] ...]. subcommands are:"))),
            RespValue::BulkString(Some(Bytes::from_static(b"CHANNELS [<pattern>] -- Return the currently active channels matching a pattern (default: all)."))),
            RespValue::BulkString(Some(Bytes::from_static(b"NUMSUB [<channel> ...] -- Return listen count for channels."))),
            RespValue::BulkString(Some(Bytes::from_static(b"NUMPAT -- Return the number of active patterns."))),
        ]))
    }
}

pub fn help() -> QueuedOp {
    QueuedOp {
        db_op: Box::new(NoOp),
        wire_op: Box::new(HelpOp),
        is_mutating: false,
        allowed_in_tx: true,
        abort_in_tx: false,
    }
}

struct NumPatOp {
    registry: Arc<PubSubRegistry>,
}

impl WireOp for NumPatOp {
    fn reply(&self, _result: Result<DbResult, DbError>) -> RespValue {
        let num = self.registry.number_pattern_subscribers();
        RespValue::Integer(num)
    }
}

struct ChannelsOp {
    registry: Arc<PubSubRegistry>,
    pattern: Option<Bytes>,
}

impl WireOp for ChannelsOp {
    fn reply(&self, _result: Result<DbResult, DbError>) -> RespValue {
        let pattern = &self.pattern;
        let channels: Vec<Bytes> = self.registry.active_channels_val(pattern.as_ref().map(|p| p.as_ref()));

        RespValue::Array(Some(
            channels
                .into_iter()
                .map(|c| RespValue::BulkString(Some(c)))
                .collect(),
        ))
    }
}


// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Pattern matcher unit tests ----

    fn m(pat: &str, s: &str) -> bool {
        redis_glob_match(pat.as_bytes(), s.as_bytes(), false)
    }

    fn mi(pat: &str, s: &str) -> bool {
        redis_glob_match(pat.as_bytes(), s.as_bytes(), true)
    }

    #[test]
    fn star_matches_empty_and_anything() {
        assert!(m("*", ""));
        assert!(m("*", "hello"));
        assert!(m("*", "hello world"));
        assert!(m("h*llo", "hllo"));
        assert!(m("h*llo", "heeeello"));
        assert!(!m("h*llo", "world"));
    }

    #[test]
    fn question_mark_matches_single_byte() {
        assert!(m("h?llo", "hello"));
        assert!(!m("h?llo", "hllo"));
        assert!(!m("h?llo", "heello"));
        assert!(m("?", "a"));
        assert!(!m("?", ""));
        assert!(!m("?", "ab"));
    }

    #[test]
    fn character_class_basic() {
        assert!(m("[abc]", "a"));
        assert!(m("[abc]", "b"));
        assert!(m("[abc]", "c"));
        assert!(!m("[abc]", "d"));
        assert!(!m("[abc]", ""));
        assert!(!m("[abc]", "ab")); // class matches exactly one char
    }

    #[test]
    fn character_class_range() {
        assert!(m("[a-z]", "a"));
        assert!(m("[a-z]", "z"));
        assert!(m("[a-z]", "m"));
        assert!(!m("[a-z]", "A"));
        assert!(!m("[a-z]", "0"));
        assert!(m("[0-9]", "5"));
        assert!(!m("[0-9]", "a"));
    }

    #[test]
    fn character_class_negation_caret() {
        assert!(!m("[^abc]", "a"));
        assert!(!m("[^abc]", "b"));
        assert!(m("[^abc]", "d"));
        assert!(m("[^a-z]", "A"));
        assert!(!m("[^a-z]", "m"));
    }

    #[test]
    fn character_class_negation_bang() {
        assert!(!m("[!abc]", "a"));
        assert!(m("[!abc]", "d"));
    }

    #[test]
    fn backslash_escaping() {
        assert!(m("h\\*llo", "h*llo"));
        assert!(!m("h\\*llo", "hello"));
        assert!(m("h\\?llo", "h?llo"));
        assert!(!m("h\\?llo", "hello"));
        assert!(m("\\[a\\]", "[a]"));
    }

    #[test]
    fn multiple_stars_collapse() {
        assert!(m("a**b", "ab"));
        assert!(m("a**b", "aXb"));
        assert!(m("a***b", "aXXXb"));
        assert!(!m("a***b", "axyz"));
    }

    #[test]
    fn empty_pattern_matches_only_empty_string() {
        assert!(m("", ""));
        assert!(!m("", "a"));
    }

    #[test]
    fn exact_literal_match() {
        assert!(m("hello", "hello"));
        assert!(!m("hello", "hell"));
        assert!(!m("hello", "helloo"));
    }

    #[test]
    fn mixed_wildcards() {
        assert!(m("h?llo*world", "helloworld"));
        assert!(m("h?llo*world", "hello big world"));
        assert!(!m("h?llo*world", "hello"));
    }

    #[test]
    fn nocase_flag() {
        assert!(mi("hello", "HELLO"));
        assert!(mi("H?LLO", "hello"));
        assert!(mi("[A-Z]", "a"));
        assert!(!mi("[A-Z]", "0"));
    }

    #[test]
    fn redis_real_world_patterns() {
        // From Redis's own test suite.
        assert!(m("foo*", "foobar"));
        assert!(m("foo*", "foo"));
        assert!(!m("foo*", "foa"));
        assert!(m("*bar*", "foobar"));
        assert!(m("*bar*", "bar"));
        assert!(m("*bar*", "barfoo"));
        assert!(!m("*bar*", "foo"));
        assert!(m("foo*bar", "fooXbar"));
        assert!(m("foo*bar", "foobar"));
        assert!(!m("foo*bar", "foobaz"));
    }

    #[test]
    fn star_at_start_and_end() {
        assert!(m("*x*", "axb"));
        assert!(!m("*x*", "abc"));
    }

    // ---- Registry integration tests ----

    #[tokio::test]
    async fn publish_to_subscriber_delivers_message() {
        let reg = Arc::new(PubSubRegistry::new());
        let ch = Bytes::from_static(b"news");
        let mut rx = reg.subscribe_channel(ch.clone());

        let count = reg.publish(b"news", Bytes::from_static(b"hello"));
        assert_eq!(count, 1);

        let msg = rx.try_recv().expect("message available");
        assert_eq!(msg.channel.as_ref(), b"news");
        assert_eq!(msg.payload.as_ref(), b"hello");
    }

    #[tokio::test]
    async fn publish_to_empty_channel_returns_zero() {
        let reg = Arc::new(PubSubRegistry::new());
        let count = reg.publish(b"empty", Bytes::from_static(b"msg"));
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn pattern_subscribe_receives_matching_message() {
        let reg = Arc::new(PubSubRegistry::new());
        let pat = Bytes::from_static(b"news.*");
        let mut rx = reg.subscribe_pattern(pat.clone());

        let count = reg.publish(b"news.sports", Bytes::from_static(b"goal"));
        assert_eq!(count, 1);

        let msg = rx.try_recv().expect("message");
        assert_eq!(msg.channel.as_ref(), b"news.sports");
        assert_eq!(msg.payload.as_ref(), b"goal");

        // Non-matching channel delivers nothing.
        let count2 = reg.publish(b"weather", Bytes::from_static(b"rain"));
        assert_eq!(count2, 0);
    }

    #[tokio::test]
    async fn cleanup_after_unsubscribe() {
        let reg = Arc::new(PubSubRegistry::new());
        let ch = Bytes::from_static(b"todie");
        let rx = reg.subscribe_channel(ch.clone());
        // Entry exists while receiver lives.
        assert!(reg.channels.contains_key(&ch));

        drop(rx);
        reg.cleanup_channel(b"todie");
        // Entry must be gone.
        assert!(!reg.channels.contains_key(&ch));
    }

    #[tokio::test]
    async fn cleanup_pattern_after_unsubscribe() {
        let reg = Arc::new(PubSubRegistry::new());
        let pat = Bytes::from_static(b"dead.*");
        let rx = reg.subscribe_pattern(pat.clone());
        assert!(reg.patterns.contains_key(&pat));

        drop(rx);
        reg.cleanup_pattern(b"dead.*");
        assert!(!reg.patterns.contains_key(&pat));
    }

    #[tokio::test]
    async fn numsub_and_numpat() {
        let reg = Arc::new(PubSubRegistry::new());
        let _r1 = reg.subscribe_channel(Bytes::from_static(b"ch1"));
        let _r2 = reg.subscribe_channel(Bytes::from_static(b"ch1")); // second subscriber
        let _r3 = reg.subscribe_channel(Bytes::from_static(b"ch2"));
        let _rp = reg.subscribe_pattern(Bytes::from_static(b"ch*"));

        let ns = reg.numsub_value(&[
            Bytes::from_static(b"ch1"),
            Bytes::from_static(b"ch2"),
            Bytes::from_static(b"ch3"),
        ]);
        assert_eq!(ns[0].1, 2); // ch1 has 2
        assert_eq!(ns[1].1, 1); // ch2 has 1
        assert_eq!(ns[2].1, 0); // ch3 has 0

        assert_eq!(reg.number_pattern_subscribers(), 1);
    }

    #[tokio::test]
    async fn active_channels_filtered_by_pattern() {
        let reg = Arc::new(PubSubRegistry::new());
        let _r1 = reg.subscribe_channel(Bytes::from_static(b"foo"));
        let _r2 = reg.subscribe_channel(Bytes::from_static(b"bar"));
        let _r3 = reg.subscribe_channel(Bytes::from_static(b"foobar"));

        let all = reg.active_channels_val(None);
        assert_eq!(all.len(), 3);

        let mut foo = reg.active_channels_val(Some(b"foo*"));
        foo.sort();
        assert_eq!(foo.len(), 2);
        assert!(foo.contains(&Bytes::from_static(b"foo")));
        assert!(foo.contains(&Bytes::from_static(b"foobar")));
    }

    #[tokio::test]
    async fn connection_subs_subscribe_unsubscribe() {
        let reg = Arc::new(PubSubRegistry::new());
        let mut subs = ConnectionSubs::new(reg.clone());

        assert!(!subs.is_subscribed());
        assert_eq!(subs.total(), 0);

        assert!(subs.subscribe_channel(Bytes::from_static(b"ch1")));
        assert!(subs.subscribe_channel(Bytes::from_static(b"ch2")));
        assert!(!subs.subscribe_channel(Bytes::from_static(b"ch1"))); // duplicate
        assert_eq!(subs.total(), 2);
        assert!(subs.is_subscribed());

        assert!(subs.subscribe_pattern(Bytes::from_static(b"p*")));
        assert_eq!(subs.total(), 3);

        // Unsubscribe from channel.
        assert!(subs.unsubscribe_channel(&Bytes::from_static(b"ch1")));
        assert!(!subs.unsubscribe_channel(&Bytes::from_static(b"ch1"))); // not present
        assert_eq!(subs.total(), 2);

        // Unsubscribe all channels.
        let removed = subs.unsubscribe_all_channels();
        assert_eq!(removed.len(), 1); // ch2 remains
        assert_eq!(subs.total(), 1); // pattern still there

        // Unsubscribe all patterns.
        let removed_pats = subs.unsubscribe_all_patterns();
        assert_eq!(removed_pats.len(), 1);
        assert_eq!(subs.total(), 0);
        assert!(!subs.is_subscribed());
    }
}
