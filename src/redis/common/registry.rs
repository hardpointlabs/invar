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

/// An in-flight blocked client waiting for a pop result on one or more keys.
struct Waiter {
    id: u64,
    /// Every public key this waiter is registered under, so it can be found
    /// and removed in O(queues) without an ownership registry.
    keys: Vec<Vec<u8>>,
    /// `true` = BZPOPMIN (pop the lowest score), `false` = BZPOPMAX.
    want_min: bool,
    /// The delivery channel; `None` once a claim has woken the waiter.
    tx: Mutex<Option<oneshot::Sender<PopResult>>>,
}

/// A single decision recorded inside a [`crate::common::DbOp`]: "this waiter
/// will receive this result".
///
/// The writer's DbOp calls [`Claim::set_result`] after consuming the element
/// on the waiter's behalf, then either:
/// - [`Claim::wake`] once the transaction has committed, delivering the
///   result to the blocked client, or
/// - [`WatchRegistry::release_front`] on any failure, returning the waiter to
///   the front of its queues so it remains the longest-waiting client.
pub struct Claim {
    waiter: Arc<Waiter>,
    result: Option<PopResult>,
}

impl Claim {
    /// `true` = BZPOPMIN, `false` = BZPOPMAX; informs the DbOp which element
    /// to pop.
    pub fn want_min(&self) -> bool {
        self.waiter.want_min
    }

    /// Attaches the popped result to the claim. Call before [`Claim::wake`].
    pub fn set_result(&mut self, result: PopResult) {
        self.result = Some(result);
    }

    /// Delivers the claimed result to the blocked client. Call from a wire op,
    /// i.e. only after the transaction committed.
    pub fn wake(&self) {
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

    /// Registers the caller as a waiter on all `keys`, then blocks until one
    /// of them delivers a pop result or `timeout` elapses (`None` = wait
    /// indefinitely).
    ///
    /// Returns `None` on a clean timeout. If a writer claimed this waiter
    /// concurrently, the registration is gone, so the channel is drained
    /// instead — discarding a real result that was already removed from the
    /// sorted set would lose data.
    pub async fn block(
        &self,
        keys: &[Vec<u8>],
        want_min: bool,
        timeout: Option<Duration>,
    ) -> Option<PopResult> {
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
            want_min,
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

        match timeout {
            Some(duration) => {
                match tokio::time::timeout(duration, &mut rx).await {
                    Ok(result) => result.ok(),
                    Err(_elapsed) => {
                        if self.remove_waiter(&waiter) {
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
    async fn claim_wakes_blocked_client() {
        let registry = Arc::new(WatchRegistry::new());
        let r = registry.clone();
        let keys = vec![key()];
        let handle = tokio::spawn(async move { r.block(&keys, true, None).await });
        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut claim = registry.try_claim(&key()).expect("waiter registered");
        assert!(claim.want_min());
        claim.set_result(result(b"m"));
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
        again.set_result(result(b"a"));
        again.wake();
        let a_result = ha.await.unwrap().expect("A delivered");
        assert_eq!(a_result.member, b"a");

        let mut bclaim = registry.try_claim(&key()).expect("waiter B");
        assert!(!bclaim.want_min());
        bclaim.set_result(result(b"b"));
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
        claim.set_result(result(b"m"));
        claim.wake();

        let delivered = handle.await.unwrap().expect("result not discarded");
        assert_eq!(delivered.member, b"m");
    }
}
