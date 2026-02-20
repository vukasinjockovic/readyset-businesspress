//! Redis pub/sub notifier for cache invalidation signals.
//!
//! Provides a background task that publishes invalidation messages to Redis channels.
//! Used by Reader nodes to signal when cached query results have changed (Tier 1).

use std::env;
use std::sync::OnceLock;

use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// Channel for sending invalidation messages to the background Redis publisher.
static INVALIDATION_TX: OnceLock<mpsc::UnboundedSender<String>> = OnceLock::new();

/// The Redis channel name for query-level invalidations (Tier 1).
const INVALIDATION_CHANNEL: &str = "readyset:invalidations";

/// Lazily initialize the background Redis publisher task.
/// Returns the sender half of the channel.
fn get_or_init_tx() -> &'static mpsc::UnboundedSender<String> {
    INVALIDATION_TX.get_or_init(|| {
        let (tx, mut rx) = mpsc::unbounded_channel::<String>();

        tokio::spawn(async move {
            let redis_url = env::var("READYSET_REDIS_URL")
                .unwrap_or_else(|_| "redis://127.0.0.1/".to_string());

            let client = match redis::Client::open(redis_url.as_str()) {
                Ok(c) => c,
                Err(e) => {
                    error!(%e, "Failed to create Redis client for invalidation notifier");
                    while rx.recv().await.is_some() {}
                    return;
                }
            };

            let mut conn = match client.get_multiplexed_async_connection().await {
                Ok(c) => {
                    info!(url = %redis_url, channel = INVALIDATION_CHANNEL, "Redis invalidation notifier connected");
                    c
                }
                Err(e) => {
                    error!(%e, "Failed to connect to Redis for invalidation notifier");
                    while rx.recv().await.is_some() {}
                    return;
                }
            };

            while let Some(msg) = rx.recv().await {
                if let Err(e) = redis::cmd("PUBLISH")
                    .arg(INVALIDATION_CHANNEL)
                    .arg(&msg)
                    .query_async::<i64>(&mut conn)
                    .await
                {
                    warn!(%e, %msg, "Failed to publish invalidation to Redis");
                }
            }
        });

        tx
    })
}

/// Send an invalidation message for a reader/cache node.
///
/// The message format is the cache name (from `CREATE CACHE ... FROM ...`).
/// Auto-initializes the Redis connection on first call.
/// Silently drops messages if the channel is closed.
pub fn notify_invalidation(cache_name: &str) {
    let _ = get_or_init_tx().send(cache_name.to_string());
}
