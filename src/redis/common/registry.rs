//! In-memory tracking of clients blocked 'watching' keys for changes, in the
//! spirit of the Go `redis/common` watcher registry.
//!
//! Go relies on the fact that each connection's goroutine is pinned to a
//! thread and can block on a plain channel. Tokio makes no such promise, so
//! here the registry hands out a `tokio` channel instead: a blocked command
//! `await`s it, and a writer that claims the waiter delivers the result
//! through the channel without ever holding the registry's mutex across an
//! await point.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::oneshot;

/// Value delivered to a blocked waiter once a matching write lands.
#[derive(Debug, Clone, PartialEq)]
pub struct PopResult {
    pub key: Vec<u8>,
    pub member: Vec<u8>,
    pub score: f64,
}

/// A wake signal delivered to a blocked `XREAD` waiter.
///
/// The blocker (`XADD`) has appended an entry to [`Self::public_key`] and that
/// write has committed; the blocked XREAD re-reads the streams it registered
/// for and returns the fresh entries.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamResult {
    /// The public key of the stream that gained the new entry.
    pub public_key: Vec<u8>,
}

/// The kind of blocked operation a waiter belongs to. The writer claims a
/// waiter only from a queue it shares, so a queue never mixes kinds, but the
/// registry records the kind so a writer can confirm it is serving the right
/// kind of waiter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitKind {
    /// A `BZPOPMIN` (`want_min == true`) or `BZPOPMAX` waiter.
    Pop { want_min: bool },
    /// A blocked `XREAD` waiter.
    Stream,
}

/// The single value delivered to a blocked waiter by the writer that claims
/// it. One variant per blocking command, so the registry's register/claim/wake
/// bookkeeping stays identical as more blocking commands (`BLPOP`, `BRPOP`,
/// `BRPOPLPUSH`, &c.) are added — each gains a variant carrying the result its
/// waiter needs.
#[derive(Debug, Clone, PartialEq)]
pub enum BlockResult {
    /// The element popped on a `BZPOPMIN`/`BZPOPMAX` waiter's behalf.
    Pop(PopResult),
    /// A wake signal telling a blocked `XREAD` waiter to re-read its streams.
    Stream(StreamResult),
}

/// An in-flight blocked client waiting for a result on one or more keys.
struct Waiter {
    id: u64,
    /// Every public key this waiter is registered under, so it can be found
    /// and removed in O(queues) without an ownership registry.
    keys: Vec<Vec<u8>>,
    /// What the waiter is waiting for and how the writer must serve it.
    kind: WaitKind,
    /// The delivery channel; `None` once a claim has woken the waiter.
    tx: Mutex<Option<oneshot::Sender<BlockResult>>>,
}

/// A single decision recorded inside a [`crate::common::DbOp`]: "this waiter
/// will receive this result".
///
/// The writer's DbOp calls [`Claim::set_result`] after building the result on
/// the waiter's behalf, then either:
/// - [`Claim::wake`] once the transaction has committed, delivering the
///   result to the blocked client, or
/// - [`WatchRegistry::release_front`] on any failure, returning the waiter to
///   the front of its queues so it remains the longest-waiting client.
pub struct Claim {
    waiter: Arc<Waiter>,
    result: Option<BlockResult>,
}

impl Claim {
    /// `true` = BZPOPMIN, `false` = BZPOPMAX; informs the DbOp which element
    /// to pop. Only meaningful for pop waiters.
    pub fn want_min(&self) -> bool {
        matches!(self.waiter.kind, WaitKind::Pop { want_min: true })
    }

    /// Reports whether this is a blocked `XREAD` waiter rather than a pop
    /// waiter.
    pub fn is_stream(&self) -> bool {
        matches!(self.waiter.kind, WaitKind::Stream)
    }

    /// Attaches the claimed result to the claim. Call before [`Claim::wake`].
    pub fn set_result(&mut self, result: BlockResult) {
        self.result = Some(result);
    }

    /// Sends the claimed result to the blocked client while the write
    /// transaction that produced it has committed. Removes the delivery
    /// channel from the waiter afterwards.
    pub fn wake(&self) {
        if let WaitKind::Stream = self.waiter.kind {
            tracing::debug!(waiter_id = self.waiter.id, "stream waiter woken");
        }
        let sender = self.waiter.tx.lock().unwrap().take();
        if let (Some(sender), Some(result)) = (sender, &self.result) {
            let _ = sender.send(result.clone());
        }
    }
}

/// Process-wide registry mapping a public Redis key to the FIFO queue of
/// clients currently blocked waiting for a pop on that key.
///
/// Invariants:
/// - The longest-waiting client on any key is always the front of its queue.
/// - A single waiter may appear in multiple key queues (multi-key blocking).
/// - All mutations are guarded by a short-lived [`Mutex`]; no lock is ever
///   held across an await point.
pub struct WatchRegistry {
    inner: Mutex<Inner>,
}

struct Inner {
    waiters: HashMap<Vec<u8>, VecDeque<Arc<Waiter>>>,
    next_id: u64,
}

impl WatchRegistry {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                waiters: HashMap::new(),
                next_id: 1,
            }),
        }
    }

    /// Inspects the front of the queue for `public_key` and, if a waiter is
    /// present, removes it from ALL of its registered key queues and returns
    /// a [`Claim`] that the caller must later either [`Claim::wake`] (on
    /// commit success) or [`WatchRegistry::release_front`] (on any failure).
    ///
    /// Returns `None` when there are no waiters for the key.
    ///
    /// Must be called from inside a DbOp — i.e. while the transaction is still
    /// in progress — so the element consumed on behalf of the waiter is
    /// removed atomically with the persisted write.
    pub fn try_claim(&self, public_key: &[u8]) -> Option<Claim> {
        let mut inner = self.inner.lock().unwrap();
        let queue = inner.waiters.get_mut(public_key)?;
        let waiter = queue.pop_front()?;
        if matches!(waiter.kind, WaitKind::Stream) {
            tracing::debug!(
                waiter_id = waiter.id,
                key = %String::from_utf8_lossy(public_key),
                "stream waiter claimed by writer"
            );
        }
        if queue.is_empty() {
            inner.waiters.remove(public_key);
        }

        // Remove this waiter from every key it registered under.
        for key in &waiter.keys {
            if key == public_key {
                continue;
            }
            if let Some(queue) = inner.waiters.get_mut(key) {
                queue.retain(|w| w.id != waiter.id);
                if queue.is_empty() {
                    inner.waiters.remove(key);
                }
            }
        }

        Some(Claim {
            waiter,
            result: None,
        })
    }

    /// Pushes a released claim's waiter back to the FRONT of each key's queue
    /// so it remains the longest-waiting client. Call when a transaction (or a
    /// later op in the same batch) fails after [`WatchRegistry::try_claim`]
    /// already ran.
    pub fn release_front(&self, claim: &Claim) {
        let mut inner = self.inner.lock().unwrap();
        for key in &claim.waiter.keys {
            inner
                .waiters
                .entry(key.clone())
                .or_default()
                .push_front(claim.waiter.clone());
        }
    }

    /// Registers the caller as a `BZPOPMIN`/`BZPOPMAX` waiter on all `keys`,
    /// then blocks until one of them delivers a pop result or `timeout`
    /// elapses (`None` = wait indefinitely). See [`Self::block_inner`].
    pub async fn block(
        &self,
        keys: &[Vec<u8>],
        want_min: bool,
        timeout: Option<Duration>,
    ) -> Option<PopResult> {
        match self
            .block_inner(keys, WaitKind::Pop { want_min }, timeout)
            .await
        {
            Some(BlockResult::Pop(result)) => Some(result),
            Some(BlockResult::Stream(_)) => None,
            None => None,
        }
    }

    /// Registers the caller as a blocked XREAD waiter on all `keys`, then
    /// blocks until one of them gains a new entry or `timeout` elapses
    /// (`None` = wait indefinitely). See [`Self::block_inner`].
    pub async fn block_stream(
        &self,
        keys: &[Vec<u8>],
        timeout: Option<Duration>,
    ) -> Option<BlockResult> {
        tracing::debug!(keys = ?keys, timeout = ?timeout, "xread requesting block");
        self.block_inner(keys, WaitKind::Stream, timeout).await
    }

    /// Registers `kind` as a waiter on all `keys`, then blocks until a writer
    /// claims it and delivers a result, or `timeout` elapses (`None` = wait
    /// indefinitely).
    ///
    /// Returns `None` on a clean timeout. If a writer claimed this waiter
    /// concurrently, the registration is gone, so the channel is drained
    /// instead — discarding a real result that was already removed from the
    /// sorted set would lose data.
    async fn block_inner(
        &self,
        keys: &[Vec<u8>],
        kind: WaitKind,
        timeout: Option<Duration>,
    ) -> Option<BlockResult> {
        let (tx, mut rx) = oneshot::channel();
        let id = {
            let mut inner = self.inner.lock().unwrap();
            let id = inner.next_id;
            inner.next_id += 1;
            id
        };
        let waiter = Arc::new(Waiter {
            id,
            keys: keys.to_vec(),
            kind,
            tx: Mutex::new(Some(tx)),
        });
        {
            let mut inner = self.inner.lock().unwrap();
            for key in keys {
                inner
                    .waiters
                    .entry(key.clone())
                    .or_default()
                    .push_back(waiter.clone());
            }
        }
        if let WaitKind::Stream = kind {
            tracing::debug!(
                waiter_id = id,
                keys = ?keys,
                "stream waiter registered (xread blocking)"
            );
        }

        match timeout {
            Some(duration) => {
                match tokio::time::timeout(duration, &mut rx).await {
                    Ok(result) => result.ok(),
                    Err(_elapsed) => {
                        if self.remove_waiter(&waiter) {
                            if let WaitKind::Stream = waiter.kind {
                                tracing::debug!(
                                    waiter_id = waiter.id,
                                    keys = ?waiter.keys,
                                    "stream waiter timed out (wake missed)"
                                );
                            }
                            // Cleanly removed ourselves: a genuine timeout.
                            None
                        } else {
                            // Lost the race: a writer claimed us and is about
                            // to send (or already sent) into the channel.
                            rx.await.ok()
                        }
                    }
                }
            }
            None => rx.await.ok(),
        }
    }

    /// Removes `waiter` from every queue it appears in. Returns `true` if it
    /// was still registered everywhere (a clean timeout); `false` if a claim
    /// had already removed it.
    fn remove_waiter(&self, waiter: &Arc<Waiter>) -> bool {
        let mut inner = self.inner.lock().unwrap();
        let mut removed = false;
        for key in &waiter.keys {
            if let Some(queue) = inner.waiters.get_mut(key) {
                let before = queue.len();
                queue.retain(|w| w.id != waiter.id);
                if queue.len() != before {
                    removed = true;
                }
                if queue.is_empty() {
                    inner.waiters.remove(key);
                }
            }
        }
        removed
    }
}

impl Default for WatchRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn key() -> Vec<u8> {
        b"0:key".to_vec()
    }

    fn result(member: &[u8]) -> PopResult {
        PopResult {
            key: key(),
            member: member.to_vec(),
            score: 1.0,
        }
    }

    #[tokio::test]
    async fn try_claim_with_no_waiter_returns_none() {
        let registry = WatchRegistry::new();
        assert!(registry.try_claim(&key()).is_none());
    }

    #[tokio::test]
    async fn block_times_out_cleanly() {
        let registry = WatchRegistry::new();
        let result = registry
            .block(&[key()], true, Some(Duration::from_millis(20)))
            .await;
        assert!(result.is_none());
        // The waiter must have been deregistered.
        assert!(registry.try_claim(&key()).is_none());
    }

    #[tokio::test]
    async fn block_stream_wakes_blocked_reader() {
        let registry = Arc::new(WatchRegistry::new());
        let r = registry.clone();
        let keys = vec![key()];
        let handle =
            tokio::spawn(async move { r.block_stream(&keys, None).await });
        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut claim = registry.try_claim(&key()).expect("waiter registered");
        assert!(claim.is_stream());
        claim.set_result(BlockResult::Stream(StreamResult {
            public_key: key(),
        }));
        claim.wake();

        let delivered = handle.await.unwrap().expect("blocked client woke");
        match delivered {
            BlockResult::Stream(s) => assert_eq!(s.public_key, key()),
            other => panic!("expected stream result, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn claim_wakes_blocked_client() {
        let registry = Arc::new(WatchRegistry::new());
        let r = registry.clone();
        let keys = vec![key()];
        let handle = tokio::spawn(async move { r.block(&keys, true, None).await });
        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut claim = registry.try_claim(&key()).expect("waiter registered");
        assert!(claim.want_min());
        claim.set_result(BlockResult::Pop(result(b"m")));
        claim.wake();

        let delivered = handle.await.unwrap().expect("blocked client woke");
        assert_eq!(delivered.member, b"m");
    }

    #[tokio::test]
    async fn release_front_preserves_waiting_order() {
        let registry: Arc<WatchRegistry> = Arc::new(WatchRegistry::new());
        let a_keys = vec![key()];
        let b_keys = a_keys.clone();
        let ra = registry.clone();
        let rb = registry.clone();
        let ha = tokio::spawn(async move { ra.block(&a_keys, true, None).await });
        tokio::time::sleep(Duration::from_millis(5)).await;
        let hb = tokio::spawn(async move { rb.block(&b_keys, false, None).await });
        tokio::time::sleep(Duration::from_millis(5)).await;

        // A registered first and must be claimed first.
        let claim = registry.try_claim(&key()).expect("waiter A");
        assert!(claim.want_min());
        // Release it back to the front: it must stay the longest-waiting
        // client, ahead of B, even though it was already claimed once.
        registry.release_front(&claim);
        drop(claim);

        let mut again = registry.try_claim(&key()).expect("waiter A again");
        assert!(again.want_min());
        again.set_result(BlockResult::Pop(result(b"a")));
        again.wake();
        let a_result = ha.await.unwrap().expect("A delivered");
        assert_eq!(a_result.member, b"a");

        let mut bclaim = registry.try_claim(&key()).expect("waiter B");
        assert!(!bclaim.want_min());
        bclaim.set_result(BlockResult::Pop(result(b"b")));
        bclaim.wake();
        let b_result = hb.await.unwrap().expect("B delivered");
        assert_eq!(b_result.member, b"b");
    }

    #[tokio::test]
    async fn timeout_after_claim_drains_channel() {
        let registry: Arc<WatchRegistry> = Arc::new(WatchRegistry::new());
        let r = registry.clone();
        let keys = vec![key()];
        let handle =
            tokio::spawn(
                async move { r.block(&keys, true, Some(Duration::from_millis(30))).await },
            );
        tokio::time::sleep(Duration::from_millis(5)).await;

        // Claim removes the waiter from the registry before its timeout fires.
        let mut claim = registry.try_claim(&key()).expect("waiter registered");
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The block's timeout fired and it must be draining the channel, so a
        // result sent now still gets through rather than being discarded.
        claim.set_result(BlockResult::Pop(result(b"m")));
        claim.wake();

        let delivered = handle.await.unwrap().expect("result not discarded");
        assert_eq!(delivered.member, b"m");
    }
}
