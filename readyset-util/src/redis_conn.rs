//! Auto-reconnecting Redis connections for the long-lived notifier tasks.
//!
//! The plain multiplexed connection has no reconnect path: after a Redis
//! restart the dead socket either errors (broken pipe) or hangs forever on
//! every subsequent command, and the owning task never recovers. Proven live
//! 2026-08-12: a Redis restart at 04:43 UTC silently killed Tier-1/2/3
//! invalidation AND the rs:lsn writer for three days — the row task's first
//! post-restart op hung on the stale socket without ever logging, so the
//! deployment degraded to a TTL-only cache with nothing in the logs but the
//! rs:lsn writer's broken-pipe warnings.
//!
//! `ConnectionManager` closes both holes: a connection-level failure still
//! returns an error for that command (every notifier op is droppable by
//! design — a later event heals the state), but the manager re-establishes
//! the connection in the background with exponential backoff, and the
//! response timeout bounds the dead-socket hang that produced the silent
//! variant.

use std::time::Duration;

use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::{Client, RedisResult};

/// Bound on a single command against a connection that may be silently dead.
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(2);

/// Bound on each (re)connection attempt.
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(2);

/// Cap on the exponential reconnect backoff, in milliseconds.
const MAX_RECONNECT_DELAY_MS: u64 = 5_000;

/// Open an auto-reconnecting connection for a long-lived notifier task.
///
/// The initial connect is still allowed to fail (callers keep their existing
/// drain-and-bail handling); it is the established connection that must
/// survive Redis restarts.
pub async fn reconnecting(client: &Client) -> RedisResult<ConnectionManager> {
    let config = ConnectionManagerConfig::new()
        .set_connection_timeout(CONNECTION_TIMEOUT)
        .set_response_timeout(RESPONSE_TIMEOUT)
        .set_max_delay(MAX_RECONNECT_DELAY_MS);
    ConnectionManager::new_with_config(client.clone(), config).await
}
