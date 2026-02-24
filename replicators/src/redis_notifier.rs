//! Redis notifier for row-level change signals (Tier 2).
//!
//! When the WAL replicator detects a row-level change (INSERT/UPDATE/DELETE),
//! it calls `notify_row_change(schema, table, pk, op)`. This module:
//! 1. Publishes to `readyset:row_changes` (legacy, for diagnostics)
//! 2. Looks up dependent RSC cache keys via `SMEMBERS rs:row_deps:{table}:{pk}`
//! 3. Deletes those keys from Redis
//! 4. Cleans up reverse dependency mappings
//! 5. Publishes invalidation events to Centrifugo via Redis XADD

use std::collections::HashMap;
use std::env;
use std::sync::OnceLock;

use tokio::sync::mpsc;
use tracing::{error, info, warn, debug};

/// Message type for the row change notifier.
struct RowChangeMsg {
    /// JSON payload for legacy PUBLISH
    json: String,
    /// "{schema}.{table}:{pk}" for dep lookup
    row_key: String,
}

static ROW_CHANGE_TX: OnceLock<mpsc::UnboundedSender<RowChangeMsg>> = OnceLock::new();

const ROW_CHANGES_CHANNEL: &str = "readyset:row_changes";

fn get_or_init_row_tx() -> &'static mpsc::UnboundedSender<RowChangeMsg> {
    ROW_CHANGE_TX.get_or_init(|| {
        let (tx, mut rx) = mpsc::unbounded_channel::<RowChangeMsg>();

        tokio::spawn(async move {
            let redis_url = env::var("READYSET_REDIS_URL")
                .unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
            let prefix = env::var("READYSET_REDIS_PREFIX")
                .unwrap_or_default();
            let centrifugo_enabled = env::var("READYSET_CENTRIFUGO_ENABLED")
                .unwrap_or_else(|_| "true".to_string()) == "true";
            let centrifugo_stream = env::var("READYSET_CENTRIFUGO_STREAM")
                .unwrap_or_else(|_| "centrifugo:rsc".to_string());

            let client = match redis::Client::open(redis_url.as_str()) {
                Ok(c) => c,
                Err(e) => {
                    error!(%e, "Failed to create Redis client for row change notifier");
                    while rx.recv().await.is_some() {}
                    return;
                }
            };

            let mut conn = match client.get_multiplexed_async_connection().await {
                Ok(c) => {
                    info!(url = %redis_url, channel = ROW_CHANGES_CHANNEL,
                          centrifugo = centrifugo_enabled,
                          "Redis row change notifier connected (Tier 2)");
                    c
                }
                Err(e) => {
                    error!(%e, "Failed to connect to Redis for row change notifier");
                    while rx.recv().await.is_some() {}
                    return;
                }
            };

            while let Some(msg) = rx.recv().await {
                // 1. Legacy PUBLISH for diagnostics/PHP listener
                if let Err(e) = redis::cmd("PUBLISH")
                    .arg(ROW_CHANGES_CHANNEL)
                    .arg(&msg.json)
                    .query_async::<i64>(&mut conn)
                    .await
                {
                    warn!(%e, json = %msg.json, "Failed to publish row change to Redis");
                }

                // 2. Centrifugo bridge: row deps lookup + DEL + cleanup + XADD
                if centrifugo_enabled {
                    if let Err(e) = handle_row_invalidation(&mut conn, &msg.row_key, &prefix, &centrifugo_stream).await {
                        warn!(%e, row_key = %msg.row_key, "Centrifugo bridge: row invalidation failed");
                    }
                }
            }
        });

        tx
    })
}

/// Handle the full invalidation cycle for a row change.
async fn handle_row_invalidation(
    conn: &mut redis::aio::MultiplexedConnection,
    row_key: &str,
    prefix: &str,
    centrifugo_stream: &str,
) -> Result<(), redis::RedisError> {
    let dep_key = format!("{}rs:row_deps:{}", prefix, row_key);

    // 1. Get dependent RSC keys
    let dependent_keys: Vec<String> = redis::cmd("SMEMBERS")
        .arg(&dep_key)
        .query_async(conn)
        .await
        .unwrap_or_default();

    if dependent_keys.is_empty() {
        return Ok(());
    }

    debug!(row_key, keys = dependent_keys.len(), "Tier 2: invalidating dependent keys");

    // 2. DEL all dependent keys (with prefix)
    let prefixed_keys: Vec<String> = dependent_keys.iter()
        .map(|k| format!("{}{}", prefix, k))
        .collect();
    redis::cmd("DEL")
        .arg(&prefixed_keys)
        .query_async::<i64>(conn)
        .await?;

    // 3. Clean up reverse row deps
    for key in &dependent_keys {
        let key_row_deps_key = format!("{}rs:key_row_deps:{}", prefix, key);
        let row_deps: Vec<String> = redis::cmd("SMEMBERS")
            .arg(&key_row_deps_key)
            .query_async(conn)
            .await
            .unwrap_or_default();
        for row_dep in &row_deps {
            let row_dep_set_key = format!("{}rs:row_deps:{}", prefix, row_dep);
            redis::cmd("SREM")
                .arg(&row_dep_set_key)
                .arg(key)
                .query_async::<i64>(conn)
                .await
                .ok();
        }
        redis::cmd("DEL")
            .arg(&key_row_deps_key)
            .query_async::<i64>(conn)
            .await
            .ok();
    }

    // 4. Group by authHash and XADD to Centrifugo stream
    let mut grouped: HashMap<String, Vec<String>> = HashMap::new();
    for key in &dependent_keys {
        // Key format: rs:gql:{schema}:{authHash}:{queryHash}
        let parts: Vec<&str> = key.split(':').collect();
        if parts.len() >= 5 && parts[0] == "rs" && parts[1] == "gql" {
            let auth_hash = parts[3].to_string();
            grouped.entry(auth_hash).or_default().push(key.clone());
        }
    }

    for (auth_hash, keys) in &grouped {
        let channel = if auth_hash == "anon" {
            "rsc:anon".to_string()
        } else {
            format!("rsc:{}", auth_hash)
        };

        let payload = serde_json::json!({
            "channel": channel,
            "data": { "keys": keys }
        });

        redis::cmd("XADD")
            .arg(centrifugo_stream)
            .arg("*")
            .arg("method")
            .arg("publish")
            .arg("payload")
            .arg(payload.to_string())
            .query_async::<String>(conn)
            .await
            .ok();

        debug!(channel, key_count = keys.len(), "Tier 2: published to Centrifugo");
    }

    Ok(())
}

/// Notify that a specific row has been modified via WAL replication.
pub fn notify_row_change(schema: &str, table: &str, pk: &str, op: &str) {
    let json = serde_json::json!({
        "table": format!("{}.{}", schema, table),
        "pk": pk,
        "op": op,
    })
    .to_string();
    let row_key = format!("{}.{}:{}", schema, table, pk);
    let _ = get_or_init_row_tx().send(RowChangeMsg { json, row_key });
}
