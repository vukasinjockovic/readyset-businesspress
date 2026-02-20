//! Redis pub/sub notifier for table-level change signals (Tier 2).
//!
//! Publishes table names to `readyset:table_changes` whenever a WAL write
//! (INSERT/UPDATE/DELETE) is detected for any replicated table.

use std::env;
use std::sync::OnceLock;

use tokio::sync::mpsc;
use tracing::{error, info, warn};

static TABLE_CHANGE_TX: OnceLock<mpsc::UnboundedSender<String>> = OnceLock::new();

const TABLE_CHANGES_CHANNEL: &str = "readyset:table_changes";

fn get_or_init_tx() -> &'static mpsc::UnboundedSender<String> {
    TABLE_CHANGE_TX.get_or_init(|| {
        let (tx, mut rx) = mpsc::unbounded_channel::<String>();

        tokio::spawn(async move {
            let redis_url = env::var("READYSET_REDIS_URL")
                .unwrap_or_else(|_| "redis://127.0.0.1/".to_string());

            let client = match redis::Client::open(redis_url.as_str()) {
                Ok(c) => c,
                Err(e) => {
                    error!(%e, "Failed to create Redis client for table change notifier");
                    while rx.recv().await.is_some() {}
                    return;
                }
            };

            let mut conn = match client.get_multiplexed_async_connection().await {
                Ok(c) => {
                    info!(url = %redis_url, channel = TABLE_CHANGES_CHANNEL, "Redis table change notifier connected");
                    c
                }
                Err(e) => {
                    error!(%e, "Failed to connect to Redis for table change notifier");
                    while rx.recv().await.is_some() {}
                    return;
                }
            };

            while let Some(msg) = rx.recv().await {
                if let Err(e) = redis::cmd("PUBLISH")
                    .arg(TABLE_CHANGES_CHANNEL)
                    .arg(&msg)
                    .query_async::<i64>(&mut conn)
                    .await
                {
                    warn!(%e, %msg, "Failed to publish table change to Redis");
                }
            }
        });

        tx
    })
}

/// Notify that a table has been modified via WAL replication.
/// Message format: `schema.table_name`
pub fn notify_table_change(schema: &str, table: &str) {
    let msg = format!("{}.{}", schema, table);
    let _ = get_or_init_tx().send(msg);
}
