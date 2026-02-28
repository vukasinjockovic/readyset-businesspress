//! Redis notifier for row-level change signals (Tier 2).
//!
//! When the WAL replicator detects a row-level change (INSERT/UPDATE/DELETE),
//! it calls `notify_row_change(schema, table, pk, op)`. This module:
//! 1. Looks up dependent RSC cache keys via `SMEMBERS rs:row_deps:{table}:{pk}`
//! 2. Deletes those keys from Redis
//! 3. Cleans up reverse dependency mappings
//! 4. Broadcasts invalidation events via:
//!    - Reverb HTTP batch API (default, READYSET_WEBSOCKET_SERVER=reverb)
//!    - Centrifugo Redis XADD (READYSET_WEBSOCKET_SERVER=centrifugo)
//! 5. Optionally publishes to legacy Redis PUBLISH channel (READYSET_PUBLISH_LEGACY=true)

use std::collections::HashMap;
use std::env;
use std::sync::OnceLock;
use std::time::Duration;

use readyset_util::reverb_http::{self, ReverbConfig, group_by_auth_hash};
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
            let ws_server = env::var("READYSET_WEBSOCKET_SERVER")
                .unwrap_or_else(|_| "centrifugo".to_string());
            let centrifugo_stream = env::var("READYSET_CENTRIFUGO_STREAM")
                .unwrap_or_else(|_| "centrifugo:rsc".to_string());
            let publish_legacy = env::var("READYSET_PUBLISH_LEGACY")
                .unwrap_or_else(|_| "false".to_string()) == "true";
            let debounce_ms: u64 = env::var("READYSET_DEBOUNCE_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(30);

            // Initialize Reverb HTTP client if configured
            let reverb_config = if ws_server == "reverb" {
                ReverbConfig::from_env()
            } else {
                None
            };
            let http_client = reverb_config.as_ref().map(|_| {
                reqwest::Client::builder()
                    .timeout(Duration::from_millis(500))
                    .build()
                    .expect("Failed to build HTTP client for Reverb")
            });

            if reverb_config.is_some() {
                info!("Reverb HTTP broadcaster initialized (Tier 2)");
            }

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
                          ws_server = %ws_server, publish_legacy = publish_legacy,
                          "Redis row change notifier connected (Tier 2)");
                    c
                }
                Err(e) => {
                    error!(%e, "Failed to connect to Redis for row change notifier");
                    while rx.recv().await.is_some() {}
                    return;
                }
            };

            info!(debounce_ms, "Debounce: {}ms window (0 = disabled)", debounce_ms);

            if debounce_ms == 0 {
                // No debounce — process each message immediately (original behavior)
                while let Some(msg) = rx.recv().await {
                    // 1. Legacy PUBLISH (optional, for diagnostics)
                    if publish_legacy {
                        if let Err(e) = redis::cmd("PUBLISH")
                            .arg(ROW_CHANGES_CHANNEL)
                            .arg(&msg.json)
                            .query_async::<i64>(&mut conn)
                            .await
                        {
                            warn!(%e, json = %msg.json, "Failed to publish row change to Redis");
                        }
                    }

                    // 2. Row deps lookup + DEL + cleanup + broadcast
                    if let Err(e) = handle_row_invalidation(
                        &mut conn,
                        &msg.row_key,
                        &prefix,
                        &ws_server,
                        &centrifugo_stream,
                        reverb_config.as_ref(),
                        http_client.as_ref(),
                    ).await {
                        warn!(%e, row_key = %msg.row_key, "Tier 2: row invalidation failed");
                    }
                }
            } else {
                // Debounce — collect messages within a time window, then batch-process
                let debounce_duration = Duration::from_millis(debounce_ms);

                loop {
                    // Wait for the first message (blocking)
                    let first = match rx.recv().await {
                        Some(msg) => msg,
                        None => break,
                    };

                    let mut batch = vec![first];

                    // Drain any additional messages within the debounce window
                    let deadline = tokio::time::Instant::now() + debounce_duration;
                    loop {
                        match tokio::time::timeout_at(deadline, rx.recv()).await {
                            Ok(Some(msg)) => batch.push(msg),
                            Ok(None) => break,   // channel closed
                            Err(_) => break,      // timeout — debounce window closed
                        }
                    }

                    // Deduplicate by row_key — keep the last message per row_key (latest op wins)
                    let mut dedup: HashMap<String, RowChangeMsg> = HashMap::new();
                    for msg in batch {
                        dedup.insert(msg.row_key.clone(), msg);
                    }

                    let unique_count = dedup.len();
                    debug!(debounce_ms, batch_size = unique_count, unique = unique_count, "Debounce: batched messages");

                    // Process each unique row_key
                    for (_row_key, msg) in dedup {
                        // 1. Legacy PUBLISH (optional, for diagnostics)
                        if publish_legacy {
                            if let Err(e) = redis::cmd("PUBLISH")
                                .arg(ROW_CHANGES_CHANNEL)
                                .arg(&msg.json)
                                .query_async::<i64>(&mut conn)
                                .await
                            {
                                warn!(%e, json = %msg.json, "Failed to publish row change to Redis");
                            }
                        }

                        // 2. Row deps lookup + DEL + cleanup + broadcast
                        if let Err(e) = handle_row_invalidation(
                            &mut conn,
                            &msg.row_key,
                            &prefix,
                            &ws_server,
                            &centrifugo_stream,
                            reverb_config.as_ref(),
                            http_client.as_ref(),
                        ).await {
                            warn!(%e, row_key = %msg.row_key, "Tier 2: row invalidation failed");
                        }
                    }
                }
            }
        });

        tx
    })
}

/// Handle the full invalidation cycle for a row change.
///
/// Uses Redis pipelines to minimize round-trips:
/// - Phase 1 (read): single SMEMBERS for row deps
/// - Phase 2 (read): pipeline all SMEMBERS for reverse row deps
/// - Phase 3 (write): pipeline all DEL + SREM + optional XADD in one round-trip
async fn handle_row_invalidation(
    conn: &mut redis::aio::MultiplexedConnection,
    row_key: &str,
    prefix: &str,
    ws_server: &str,
    centrifugo_stream: &str,
    reverb_config: Option<&ReverbConfig>,
    http_client: Option<&reqwest::Client>,
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

    // --- Phase 1 (read pipeline): fetch all reverse row deps in one round-trip ---
    let key_row_deps_keys: Vec<String> = dependent_keys.iter()
        .map(|k| format!("{}rs:key_row_deps:{}", prefix, k))
        .collect();

    let mut read_pipe = redis::pipe();
    for krdk in &key_row_deps_keys {
        read_pipe.cmd("SMEMBERS").arg(krdk);
    }
    let all_row_deps: Vec<Vec<String>> = read_pipe
        .query_async(conn)
        .await
        .unwrap_or_else(|_| vec![Vec::new(); dependent_keys.len()]);

    // --- Phase 2 (write pipeline): DEL + SREM in one round-trip ---
    let mut write_pipe = redis::pipe();

    // 2a. DEL all dependent keys (with prefix)
    let prefixed_keys: Vec<String> = dependent_keys.iter()
        .map(|k| format!("{}{}", prefix, k))
        .collect();
    write_pipe.cmd("DEL").arg(&prefixed_keys).ignore();

    // 2b. Clean up reverse row deps
    for (i, key) in dependent_keys.iter().enumerate() {
        let row_deps = all_row_deps.get(i).cloned().unwrap_or_default();
        for row_dep in &row_deps {
            let row_dep_set_key = format!("{}rs:row_deps:{}", prefix, row_dep);
            write_pipe.cmd("SREM").arg(&row_dep_set_key).arg(key).ignore();
        }
        // DEL the key_row_deps set itself
        write_pipe.cmd("DEL").arg(&key_row_deps_keys[i]).ignore();
    }

    // 2c. Centrifugo XADD (only when ws_server is centrifugo)
    let grouped = group_by_auth_hash(&dependent_keys);

    if ws_server == "centrifugo" {
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

            write_pipe.cmd("XADD")
                .arg(centrifugo_stream)
                .arg("*")
                .arg("method")
                .arg("publish")
                .arg("payload")
                .arg(payload.to_string())
                .ignore();

            debug!(channel, key_count = keys.len(), "Tier 2: published to Centrifugo");
        }
    }

    // Execute all writes in one round-trip
    write_pipe.query_async::<()>(conn).await.ok();

    // 2d. Reverb HTTP broadcast (after Redis writes complete)
    if ws_server == "reverb" {
        if let (Some(cfg), Some(client)) = (reverb_config, http_client) {
            reverb_http::broadcast_to_reverb(cfg, client, &grouped).await;
        }
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
