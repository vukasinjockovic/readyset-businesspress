//! Redis notifier for cache invalidation signals (Tier 1: query-level).
//!
//! When a Reader node detects that cached query results have changed, it calls
//! `notify_invalidation(cache_name, key_values)`. This module:
//! 1. For parameterized queries (key_values non-empty):
//!    a. Looks up per-param deps via `SMEMBERS rs:pdeps:{cache_name}:{param_key}`
//!    b. Falls back to `SMEMBERS rs:deps:{cache_name}` if no per-param deps found
//! 2. For non-parameterized queries (key_values empty):
//!    a. Looks up `SMEMBERS rs:deps:{cache_name}` (broad invalidation)
//! 3. Deletes affected RSC cache keys from Redis
//! 4. Cleans up dependency mappings
//! 5. Broadcasts invalidation events via:
//!    - Reverb HTTP batch API (default, READYSET_WEBSOCKET_SERVER=reverb)
//!    - Centrifugo Redis XADD (READYSET_WEBSOCKET_SERVER=centrifugo)
//! 6. Optionally publishes to legacy Redis PUBLISH channel (READYSET_PUBLISH_LEGACY=true)

use std::collections::HashMap;
use std::env;
use std::sync::OnceLock;
use std::time::Duration;

use readyset_util::reverb_http::{self, ReverbConfig, group_by_auth_hash};
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
                info!("Reverb HTTP broadcaster initialized (Tier 1)");
            }

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
                          ws_server = %ws_server, publish_legacy = publish_legacy,
                          "Redis invalidation notifier connected (Tier 1 — granular)");
                    c
                }
                Err(e) => {
                    error!(%e, "Failed to connect to Redis for invalidation notifier");
                    while rx.recv().await.is_some() {}
                    return;
                }
            };

            info!(debounce_ms, "Debounce: {}ms window (0 = disabled)", debounce_ms);

            if debounce_ms == 0 {
                // No debounce — process each message immediately (original behavior)
                while let Some(msg) = rx.recv().await {
                    // 1. Deps lookup + DEL + cleanup + broadcast
                    if let Err(e) = handle_invalidation(
                        &mut conn,
                        &msg.cache_name,
                        &msg.key_values,
                        &prefix,
                        &ws_server,
                        &centrifugo_stream,
                        reverb_config.as_ref(),
                        http_client.as_ref(),
                    ).await {
                        warn!(%e, cache_name = %msg.cache_name, "Tier 1: invalidation handling failed");
                    }

                    // 2. Legacy PUBLISH (optional, for diagnostics)
                    if publish_legacy {
                        if let Err(e) = redis::cmd("PUBLISH")
                            .arg(INVALIDATION_CHANNEL)
                            .arg(&msg.cache_name)
                            .query_async::<i64>(&mut conn)
                            .await
                        {
                            warn!(%e, cache_name = %msg.cache_name, "Failed to publish invalidation to Redis");
                        }
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

                    // Deduplicate by cache_name — merge key_values across messages
                    let mut dedup: HashMap<String, Vec<String>> = HashMap::new();
                    for msg in &batch {
                        let entry = dedup.entry(msg.cache_name.clone()).or_default();
                        for kv in &msg.key_values {
                            if !entry.contains(kv) {
                                entry.push(kv.clone());
                            }
                        }
                    }

                    debug!(debounce_ms, batch_size = batch.len(), unique = dedup.len(), "Debounce: batched Tier 1 messages");

                    // Process each unique cache_name
                    for (cache_name, merged_key_values) in &dedup {
                        // 1. Deps lookup + DEL + cleanup + broadcast
                        if let Err(e) = handle_invalidation(
                            &mut conn,
                            cache_name,
                            merged_key_values,
                            &prefix,
                            &ws_server,
                            &centrifugo_stream,
                            reverb_config.as_ref(),
                            http_client.as_ref(),
                        ).await {
                            warn!(%e, cache_name, "Tier 1: invalidation handling failed");
                        }

                        // 2. Legacy PUBLISH (optional, for diagnostics)
                        if publish_legacy {
                            if let Err(e) = redis::cmd("PUBLISH")
                                .arg(INVALIDATION_CHANNEL)
                                .arg(cache_name)
                                .query_async::<i64>(&mut conn)
                                .await
                            {
                                warn!(%e, cache_name, "Failed to publish invalidation to Redis");
                            }
                        }
                    }
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
///
/// Uses Redis pipelines to minimize round-trips:
/// - Phase 1 (read): pipeline SMEMBERS for dep resolution (pdeps or broad deps)
/// - Phase 2 (read): pipeline SMEMBERS for key_pdeps + key_deps reverse lookups
/// - Phase 3 (write): pipeline all DEL + SREM + broadcast in one round-trip
async fn handle_invalidation(
    conn: &mut redis::aio::MultiplexedConnection,
    cache_name: &str,
    key_values: &[String],
    prefix: &str,
    ws_server: &str,
    centrifugo_stream: &str,
    reverb_config: Option<&ReverbConfig>,
    http_client: Option<&reqwest::Client>,
) -> Result<(), redis::RedisError> {
    let dependent_keys: Vec<String>;

    if !key_values.is_empty() {
        // Parameterized query — pipeline all per-param dep lookups
        let mut pdep_keys: Vec<String> = Vec::new();
        let mut read_pipe = redis::pipe();
        for param_key in key_values {
            let pdep_key = format!("{}rs:pdeps:{}:{}", prefix, cache_name, param_key);
            read_pipe.cmd("SMEMBERS").arg(&pdep_key);
            pdep_keys.push(pdep_key);
        }
        let pdep_results: Vec<Vec<String>> = read_pipe
            .query_async(conn)
            .await
            .unwrap_or_else(|_| vec![Vec::new(); key_values.len()]);

        let mut all_keys: Vec<String> = Vec::new();
        for (i, keys) in pdep_results.iter().enumerate() {
            if !keys.is_empty() {
                debug!(
                    cache_name,
                    param_key = %key_values[i],
                    keys = keys.len(),
                    "Tier 1 granular: found per-param deps"
                );
            }
            all_keys.extend(keys.iter().cloned());
        }

        if all_keys.is_empty() {
            // No per-param deps found — fall back to broad dep key.
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
                warn!(%e, %cache_name, "Tier 1: SMEMBERS failed");
                return Ok(());
            }
        };
    }

    // Liveness telemetry: record that this Reader emitted a notification,
    // whether or not any dependent keys were registered. Lets the PHP side
    // (and humans battle-testing) audit which caches actually produce Tier 1
    // signals — "CREATE CACHE succeeded" does not guarantee delta delivery
    // (e.g. correlated-subselect dataflows serve reads but never notify).
    let _: Result<(), _> = redis::cmd("SET")
        .arg(format!("{}rs:t1_seen:{}", prefix, cache_name))
        .arg(format!("{}", std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)))
        .arg("EX")
        .arg(7 * 86400)
        .query_async(conn)
        .await;

    if dependent_keys.is_empty() {
        return Ok(());
    }

    debug!(
        cache_name,
        keys = dependent_keys.len(),
        granular = !key_values.is_empty(),
        "Tier 1: invalidating dependent keys"
    );

    // --- Write pipeline: DEL cache keys + broadcast (no dep cleanup) ---
    //
    // IMPORTANT: Dependency mappings (rs:deps, rs:pdeps, rs:row_deps, rs:key_deps,
    // rs:key_pdeps, rs:key_row_deps, rs:cache_name, rs:cache_params) are NOT cleaned
    // up on invalidation. They persist so subsequent WAL events for the same data
    // can still find dependent cache keys and broadcast invalidation events.
    //
    // Why: The browser must receive the WebSocket event → re-fetch from the server
    // (MISS path) → server re-registers deps. This cycle takes 200-500ms. If deps
    // are cleaned immediately, ALL WAL events during that gap fire into empty dep
    // sets, breaking the invalidation chain permanently (browser never gets notified,
    // never re-fetches, deps never re-registered — stuck until TTL or page reload).
    //
    // Stale entries are harmless:
    // - DEL on a non-existent cache key is a Redis no-op
    // - SADD on re-registration overwrites stale entries (idempotent)
    // - TTL on deps (set by PHP) provides natural cleanup
    // - Worker recycling (--max-requests) prevents unbounded accumulation
    let mut write_pipe = redis::pipe();

    // DEL all affected RSC cache keys (with prefix), PLUS their
    // stale-while-revalidate twins ("{key}:stale", kept by PHP at ~3x TTL).
    // SWR exists to smooth TTL expiry, not to serve known-stale data: without
    // this, concurrent clients could be served pre-change data from :stale
    // for up to the extended TTL if the rebuild fails.
    let prefixed_keys = reverb_http::expand_keys_for_del(prefix, &dependent_keys);
    write_pipe.cmd("DEL").arg(&prefixed_keys).ignore();

    // Revalidation markers: the PHP MISS path checks rs:reval:{key} and, when
    // present, routes the rebuild's reads to the direct Postgres write
    // connection instead of ReadySet — closing the window where a refetch
    // races the dataflow's replication lag and re-caches pre-change data.
    // 3s TTL: well above typical replication lag, well below cache TTL.
    for k in &dependent_keys {
        write_pipe.cmd("SETEX")
            .arg(format!("{}rs:reval:{}", prefix, k))
            .arg(3)
            .arg("1")
            .ignore();
    }

    // 3c. Centrifugo XADD (only when ws_server is centrifugo)
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
                .arg("MAXLEN")
                .arg("~")
                .arg("10000")
                .arg("*")
                .arg("method")
                .arg("publish")
                .arg("payload")
                .arg(payload.to_string())
                .ignore();

            debug!(channel, key_count = keys.len(), "Tier 1: published to Centrifugo");
        }
    }

    // Execute all writes in one round-trip
    write_pipe.query_async::<()>(conn).await.ok();

    // 3d. Reverb HTTP broadcast (after Redis writes complete)
    if ws_server == "reverb" {
        if let (Some(cfg), Some(client)) = (reverb_config, http_client) {
            reverb_http::broadcast_to_reverb(cfg, client, &grouped).await;
        }
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
