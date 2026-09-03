use std::time::{SystemTime, UNIX_EPOCH};

/// The current wall-clock time in milliseconds since the Unix epoch.
///
/// This is the single source of truth for "now" across the codebase. Command
/// group modules (currently `commands.rs`, `strings.rs`, `keys.rs`) used to
/// hand-roll this `SystemTime::now()...as_millis()` incantation whenever they
/// needed to convert between an absolute expiry timestamp and a remaining
/// `Duration`; that arithmetic now lives solely here, on [`Expiry`].
pub(crate) fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
