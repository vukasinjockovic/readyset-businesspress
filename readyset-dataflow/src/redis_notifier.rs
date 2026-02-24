//! Redis notifier for cache invalidation signals (Tier 1: query-level).
//!
//! When a Reader node detects that cached query results have changed, it calls
//! `notify_invalidation(cache_name, key_values)`. This module:
//! 1. Publishes to `readyset:invalidations` (legacy, for diagnostics)
//! 2. For parameterized queries (key_values non-empty):
//!    a. Looks up per-param deps via `SMEMBERS rs:pdeps:{cache_name}:{param_key}`
//!    b. Falls back to `SMEMBERS rs:deps:{cache_name}` if no per-param deps found
//! 3. For non-parameterized queries (key_values empty):
//!    a. Looks up `SMEMBERS rs:deps:{cache_name}` (broad invalidation)
//! 4. Deletes affected RSC cache keys from Redis
//! 5. Cleans up dependency mappings
//! 6. Publishes invalidation events to Centrifugo via Redis XADD

use std::collections::HashMap;
use std::env;
use std::sync::OnceLock;

use tokio::sync::mpsc;
use tracing::{error, info, warn, debug};

/// Invalidation message: cache name + optional per-parameter key values.
/// When key_values is non-empty, only the specific parameter values are invalidated.
/// When empty, the entire cache (all parameter values) is invalidated.
struct InvalidationMsg {
    cache_name: String,
    key_values: Vec<String>,
}

/// Channel for sending invalidation messages to the background Redis publisher.
static INVALIDATION_TX: OnceLock<mpsc::UnboundedSender<InvalidationMsg>> = OnceLock::new();

/// The Redis channel name for query-level invalidations (Tier 1).
const INVALIDATION_CHANNEL: &str = "readyset:invalidations";

/// Lazily initialize the background Redis publisher task.
fn get_or_init_tx() -> &'static mpsc::UnboundedSender<InvalidationMsg> {
    INVALIDATION_TX.get_or_init(|| {
        let (tx, mut rx) = mpsc::unbounded_channel::<InvalidationMsg>();

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
                    error!(%e, "Failed to create Redis client for invalidation notifier");
                    while rx.recv().await.is_some() {}
                    return;
                }
            };

            let mut conn = match client.get_multiplexed_async_connection().await {
                Ok(c) => {
                    info!(url = %redis_url, channel = INVALIDATION_CHANNEL,
                          centrifugo = centrifugo_enabled,
                          "Redis invalidation notifier connected (Tier 1 — granular)");
                    c
                }
                Err(e) => {
                    error!(%e, "Failed to connect to Redis for invalidation notifier");
                    while rx.recv().await.is_some() {}
                    return;
                }
            };

            while let Some(msg) = rx.recv().await {
                // 1. Centrifugo bridge: deps lookup + DEL + cleanup + XADD
                // MUST run BEFORE PUBLISH — otherwise PHP listener wins the race,
                // cleans deps, and this bridge finds nothing.
                if centrifugo_enabled {
                    if let Err(e) = handle_invalidation(
                        &mut conn,
                        &msg.cache_name,
                        &msg.key_values,
                        &prefix,
                        &centrifugo_stream,
                    ).await {
                        warn!(%e, cache_name = %msg.cache_name, "Centrifugo bridge: invalidation handling failed");
                    }
                }

                // 2. Legacy PUBLISH for PHP listener (handles core_meta row deps, Reverb fallback)
                if let Err(e) = redis::cmd("PUBLISH")
                    .arg(INVALIDATION_CHANNEL)
                    .arg(&msg.cache_name)
                    .query_async::<i64>(&mut conn)
                    .await
                {
                    warn!(%e, cache_name = %msg.cache_name, "Failed to publish invalidation to Redis");
                }
            }
        });

        tx
    })
}

/// Handle the full invalidation cycle for a cache name.
///
/// For parameterized queries, `key_values` contains the specific parameter values
/// that were affected (e.g., the exact `conversation_id`). We try per-param deps
/// first (`rs:pdeps:{cache_name}:{param_key}`), falling back to the broad dep key
/// (`rs:deps:{cache_name}`) if no per-param deps are registered.
///
/// For non-parameterized queries, `key_values` is empty and we always use the
/// broad dep key.
async fn handle_invalidation(
    conn: &mut redis::aio::MultiplexedConnection,
    cache_name: &str,
    key_values: &[String],
    prefix: &str,
    centrifugo_stream: &str,
) -> Result<(), redis::RedisError> {
    let dependent_keys: Vec<String>;

    if !key_values.is_empty() {
        // Parameterized query — try per-param deps first
        let mut all_keys: Vec<String> = Vec::new();

        for param_key in key_values {
            let pdep_key = format!("{}rs:pdeps:{}:{}", prefix, cache_name, param_key);
            let keys: Vec<String> = redis::cmd("SMEMBERS")
                .arg(&pdep_key)
                .query_async(conn)
                .await
                .unwrap_or_default();

            if !keys.is_empty() {
                debug!(
                    cache_name,
                    param_key,
                    keys = keys.len(),
                    "Tier 1 granular: found per-param deps"
                );
            }

            all_keys.extend(keys);
        }

        if all_keys.is_empty() {
            // No per-param deps found — fall back to broad dep key.
            // This handles the case where PHP hasn't registered per-param deps yet
            // (e.g., during upgrade rollout or for legacy registrations).
            debug!(cache_name, "Tier 1: no per-param deps, falling back to broad deps");
            let dep_key = format!("{}rs:deps:{}", prefix, cache_name);
            dependent_keys = redis::cmd("SMEMBERS")
                .arg(&dep_key)
                .query_async(conn)
                .await
                .unwrap_or_default();
        } else {
            dependent_keys = all_keys;
        }
    } else {
        // Non-parameterized query — use broad dep key (existing behavior)
        let dep_key = format!("{}rs:deps:{}", prefix, cache_name);
        dependent_keys = match redis::cmd("SMEMBERS")
            .arg(&dep_key)
            .query_async(conn)
            .await
        {
            Ok(keys) => keys,
            Err(e) => {
                warn!(%e, %cache_name, "Centrifugo bridge: SMEMBERS failed");
                return Ok(());
            }
        };
    }

    if dependent_keys.is_empty() {
        return Ok(());
    }

    debug!(
        cache_name,
        keys = dependent_keys.len(),
        granular = !key_values.is_empty(),
        "Tier 1: invalidating dependent keys"
    );

    // 2. DEL all affected RSC cache keys (with prefix)
    let prefixed_keys: Vec<String> = dependent_keys.iter()
        .map(|k| format!("{}{}", prefix, k))
        .collect();
    redis::cmd("DEL")
        .arg(&prefixed_keys)
        .query_async::<i64>(conn)
        .await?;

    // 3. Clean up dependency mappings for each invalidated key
    for key in &dependent_keys {
        // Clean up per-param dep sets (rs:pdeps:{cacheName}:{paramKey})
        let key_pdeps_key = format!("{}rs:key_pdeps:{}", prefix, key);
        let param_deps: Vec<String> = redis::cmd("SMEMBERS")
            .arg(&key_pdeps_key)
            .query_async(conn)
            .await
            .unwrap_or_default();
        for pdep in &param_deps {
            // pdep format: "{cacheName}:{paramKey}"
            let pdep_set_key = format!("{}rs:pdeps:{}", prefix, pdep);
            redis::cmd("SREM")
                .arg(&pdep_set_key)
                .arg(key)
                .query_async::<i64>(conn)
                .await
                .ok();
        }
        redis::cmd("DEL")
            .arg(&key_pdeps_key)
            .query_async::<i64>(conn)
            .await
            .ok();

        // Clean up broad dep sets (rs:deps:{cacheName}) — backward compat
        let key_deps_key = format!("{}rs:key_deps:{}", prefix, key);
        let reverse_deps: Vec<String> = redis::cmd("SMEMBERS")
            .arg(&key_deps_key)
            .query_async(conn)
            .await
            .unwrap_or_default();
        for dep in &reverse_deps {
            let dep_set_key = format!("{}rs:deps:{}", prefix, dep);
            redis::cmd("SREM")
                .arg(&dep_set_key)
                .arg(key)
                .query_async::<i64>(conn)
                .await
                .ok();
        }
        redis::cmd("DEL")
            .arg(&key_deps_key)
            .query_async::<i64>(conn)
            .await
            .ok();

        // Clean up cache name/params mappings
        let cache_name_key = format!("{}rs:cache_name:{}", prefix, key);
        redis::cmd("DEL")
            .arg(&cache_name_key)
            .query_async::<i64>(conn)
            .await
            .ok();
        let cache_params_key = format!("{}rs:cache_params:{}", prefix, key);
        redis::cmd("DEL")
            .arg(&cache_params_key)
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
            .arg("MAXLEN")
            .arg("~")
            .arg("10000")
            .arg("*")
            .arg("method")
            .arg("publish")
            .arg("payload")
            .arg(payload.to_string())
            .query_async::<String>(conn)
            .await
            .ok();

        debug!(channel, key_count = keys.len(), "Tier 1: published to Centrifugo");
    }

    Ok(())
}

/// Send an invalidation message for a reader/cache node.
///
/// `key_values` contains the specific parameter values that were affected,
/// extracted from the Reader's key columns. Each entry is a pipe-delimited
/// string of column values (e.g., "1ef9e7ee-12ad-4d1a-9568-230852a1f050"
/// for a single-column key, or "val1|val2" for composite keys).
///
/// When `key_values` is empty (non-parameterized query), the entire cache
/// is invalidated (all parameter values).
pub fn notify_invalidation(cache_name: &str, key_values: Vec<String>) {
    let _ = get_or_init_tx().send(InvalidationMsg {
        cache_name: cache_name.to_string(),
        key_values,
    });
}
