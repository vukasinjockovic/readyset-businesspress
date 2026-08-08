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
/// - Phase 2 (write): DEL cache keys + broadcast in one round-trip
///
/// IMPORTANT: row_deps (rs:row_deps:{table}:{pk}) are NOT cleaned up on
/// invalidation. They persist so that subsequent WAL events for the same row
/// can still find the dependent cache keys and broadcast invalidation events.
///
/// Why: The browser must receive the WebSocket event, re-fetch from the server
/// (MISS path), and the server must re-register deps — a 200-500ms cycle.
/// If row_deps are cleaned immediately, ALL WAL events during that gap fire
/// into empty deps and the browser never gets notified, breaking the
/// invalidation chain permanently until TTL expiry or page reload.
///
/// Stale entries are harmless: they point to cache keys that no longer exist.
/// DEL on a non-existent key is a Redis no-op. The PHP MISS path overwrites
/// deps via SADD (idempotent) when it re-registers.
///
/// Cleanup happens naturally:
/// - row_deps have TTL matching the cache TTL (set by PHP)
/// - key_row_deps are overwritten on each MISS re-registration
/// - Worker recycling (--max-requests) prevents unbounded accumulation
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

    // --- Write pipeline: DEL cache keys + broadcast (no dep cleanup) ---
    let mut write_pipe = redis::pipe();

    // 2a. DEL all dependent cache keys (with prefix), PLUS their
    // stale-while-revalidate twins ("{key}:stale", kept by PHP at ~3x TTL).
    // SWR exists to smooth TTL expiry, not to serve known-stale data: without
    // this, concurrent clients could be served pre-change data from :stale
    // for up to the extended TTL if the rebuild fails.
    let prefixed_keys = reverb_http::expand_keys_for_del(prefix, &dependent_keys);
    write_pipe.cmd("DEL").arg(&prefixed_keys).ignore();

    // 2b. Revalidation markers: the PHP MISS path checks rs:reval:{key} and,
    // when present, routes the rebuild's reads to the direct Postgres write
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
                .arg("MAXLEN")
                .arg("~")
                .arg("10000")
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

/// WAL ops for which Tier 3 (table-level declared deps) fires.
///
/// Default: INSERT only. UPDATEs/DELETEs of rows that are actually displayed
/// somewhere are already precisely covered by Tier 2 row_deps
/// (rs:row_deps:{schema}.{table}:{pk}), so firing Tier 3 for them is pure
/// over-invalidation: on a hot table every UPDATE would flush every declared
/// listing cache for all users (cache-hit collapse + broadcast storm) with no
/// correctness gain. Tier 3's irreplaceable job is the INSERT phantom: a
/// brand-new row that should appear in a cached listing has no row_dep yet,
/// so only the table-level declared dep can catch it.
///
/// Override with READYSET_TIER3_OPS (comma-separated, case-insensitive, e.g.
/// "INSERT,DELETE"). Setting it to an empty/blank value disables Tier 3
/// entirely. Parsed once on first row event.
static TIER3_OPS: OnceLock<Vec<String>> = OnceLock::new();

fn tier3_ops() -> &'static [String] {
    TIER3_OPS.get_or_init(|| {
        let ops = parse_tier3_ops(env::var("READYSET_TIER3_OPS").ok().as_deref());
        info!(?ops, "Tier 3 declared-dep invalidation fires for ops");
        ops
    })
}

/// Parse the READYSET_TIER3_OPS value. `None` (unset) defaults to INSERT-only;
/// an explicitly set empty/blank value yields an empty set (Tier 3 disabled).
fn parse_tier3_ops(raw: Option<&str>) -> Vec<String> {
    match raw {
        None => vec!["INSERT".to_string()],
        Some(s) => s
            .split(',')
            .map(|op| op.trim().to_ascii_uppercase())
            .filter(|op| !op.is_empty())
            .collect(),
    }
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

    // Tier 3: table-level declared deps. PHP's withDeps($cacheName, $deps, ...)
    // registers cache keys under rs:deps:{dep} where {dep} is a table name the
    // caller declared (bare or schema-qualified). This tier was originally
    // consumed by the PHP `readyset:listen` daemon; when broadcasting moved
    // into this bridge (d36ae9945) the table-level lookup was dropped and the
    // declared-dep API silently went dead. Re-enqueue here — the debounce loop
    // dedups per table per window, and SMEMBERS on a non-existent dep key is a
    // sub-ms no-op, so tables without declared deps cost effectively nothing.
    //
    // Op-gated (default INSERT-only, see TIER3_OPS): updates/deletes of
    // displayed rows are already precisely invalidated by Tier 2 row_deps
    // above; inserts are the phantom case Tier 2 cannot see.
    if tier3_ops().iter().any(|o| o.eq_ignore_ascii_case(op)) {
        notify_table_change(schema, table);
    }
}

// ---------------------------------------------------------------------------
// Tier 3: table-level declared-dep invalidation
// ---------------------------------------------------------------------------

/// Message type for the table-level change notifier (Tier 3).
struct TableChangeMsg {
    /// Bare table name, e.g. "bp_messages"
    table: String,
    /// Schema-qualified name, e.g. "public.bp_messages"
    qualified: String,
}

static TABLE_CHANGE_TX: OnceLock<mpsc::UnboundedSender<TableChangeMsg>> = OnceLock::new();

fn get_or_init_table_tx() -> &'static mpsc::UnboundedSender<TableChangeMsg> {
    TABLE_CHANGE_TX.get_or_init(|| {
        let (tx, mut rx) = mpsc::unbounded_channel::<TableChangeMsg>();

        tokio::spawn(async move {
            let redis_url = env::var("READYSET_REDIS_URL")
                .unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
            let prefix = env::var("READYSET_REDIS_PREFIX")
                .unwrap_or_default();
            let ws_server = env::var("READYSET_WEBSOCKET_SERVER")
                .unwrap_or_else(|_| "centrifugo".to_string());
            let centrifugo_stream = env::var("READYSET_CENTRIFUGO_STREAM")
                .unwrap_or_else(|_| "centrifugo:rsc".to_string());
            let debounce_ms: u64 = env::var("READYSET_DEBOUNCE_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(30);

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
                    info!(url = %redis_url, ws_server = %ws_server,
                          "Redis table change notifier connected (Tier 3 — declared deps)");
                    c
                }
                Err(e) => {
                    error!(%e, "Failed to connect to Redis for table change notifier");
                    while rx.recv().await.is_some() {}
                    return;
                }
            };

            // Table-level events arrive once per WAL row event, so the
            // debounce window is essential here: dedup by qualified name.
            let debounce_duration = Duration::from_millis(debounce_ms.max(10));

            loop {
                let first = match rx.recv().await {
                    Some(msg) => msg,
                    None => break,
                };

                let mut dedup: HashMap<String, TableChangeMsg> = HashMap::new();
                dedup.insert(first.qualified.clone(), first);

                let deadline = tokio::time::Instant::now() + debounce_duration;
                loop {
                    match tokio::time::timeout_at(deadline, rx.recv()).await {
                        Ok(Some(msg)) => {
                            dedup.insert(msg.qualified.clone(), msg);
                        }
                        Ok(None) => break,
                        Err(_) => break,
                    }
                }

                for (_qualified, msg) in dedup {
                    if let Err(e) = handle_table_invalidation(
                        &mut conn,
                        &msg.table,
                        &msg.qualified,
                        &prefix,
                        &ws_server,
                        &centrifugo_stream,
                        reverb_config.as_ref(),
                        http_client.as_ref(),
                    ).await {
                        warn!(%e, table = %msg.qualified, "Tier 3: table invalidation failed");
                    }
                }
            }
        });

        tx
    })
}

/// Handle the invalidation cycle for declared table-level deps.
///
/// Looks up BOTH `rs:deps:{table}` (bare) and `rs:deps:{schema}.{table}`
/// (qualified) — PHP callers have historically declared deps in either form.
///
/// Same policy as Tiers 1/2: cache VALUES are deleted, dep mappings persist
/// (see handle_row_invalidation for the re-registration race rationale).
async fn handle_table_invalidation(
    conn: &mut redis::aio::MultiplexedConnection,
    table: &str,
    qualified: &str,
    prefix: &str,
    ws_server: &str,
    centrifugo_stream: &str,
    reverb_config: Option<&ReverbConfig>,
    http_client: Option<&reqwest::Client>,
) -> Result<(), redis::RedisError> {
    let mut read_pipe = redis::pipe();
    read_pipe.cmd("SMEMBERS").arg(format!("{}rs:deps:{}", prefix, table));
    read_pipe.cmd("SMEMBERS").arg(format!("{}rs:deps:{}", prefix, qualified));
    let (bare_keys, qualified_keys): (Vec<String>, Vec<String>) =
        read_pipe.query_async(conn).await.unwrap_or_default();

    let mut dependent_keys = bare_keys;
    for key in qualified_keys {
        if !dependent_keys.contains(&key) {
            dependent_keys.push(key);
        }
    }

    if dependent_keys.is_empty() {
        return Ok(());
    }

    debug!(table = %qualified, keys = dependent_keys.len(), "Tier 3: invalidating declared-dep keys");

    let mut write_pipe = redis::pipe();
    // DEL values AND their ":stale" SWR twins — see handle_row_invalidation.
    let prefixed_keys = reverb_http::expand_keys_for_del(prefix, &dependent_keys);
    write_pipe.cmd("DEL").arg(&prefixed_keys).ignore();

    // Revalidation markers (rs:reval:{key}, 3s TTL): tell the PHP MISS path
    // to rebuild from the direct Postgres connection instead of ReadySet,
    // closing the replication-lag re-cache race — see handle_row_invalidation.
    for k in &dependent_keys {
        write_pipe.cmd("SETEX")
            .arg(format!("{}rs:reval:{}", prefix, k))
            .arg(3)
            .arg("1")
            .ignore();
    }

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
        }
    }

    write_pipe.query_async::<()>(conn).await.ok();

    if ws_server == "reverb" {
        if let (Some(cfg), Some(client)) = (reverb_config, http_client) {
            reverb_http::broadcast_to_reverb(cfg, client, &grouped).await;
        }
    }

    Ok(())
}

/// Notify that some row in a table changed (Tier 3 declared-dep fan-out).
/// Called from notify_row_change for ops in TIER3_OPS (default INSERT-only)
/// — deduped per debounce window downstream.
pub fn notify_table_change(schema: &str, table: &str) {
    let _ = get_or_init_table_tx().send(TableChangeMsg {
        table: table.to_string(),
        qualified: format!("{}.{}", schema, table),
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tier3_ops_default_is_insert_only() {
        assert_eq!(parse_tier3_ops(None), vec!["INSERT".to_string()]);
    }

    #[test]
    fn test_tier3_ops_custom_list_normalized() {
        assert_eq!(
            parse_tier3_ops(Some("insert, Delete")),
            vec!["INSERT".to_string(), "DELETE".to_string()]
        );
    }

    #[test]
    fn test_tier3_ops_explicit_empty_disables_tier3() {
        assert!(parse_tier3_ops(Some("")).is_empty());
        assert!(parse_tier3_ops(Some(" , ")).is_empty());
    }

    #[test]
    fn test_tier3_ops_matching_is_case_insensitive() {
        let ops = parse_tier3_ops(Some("INSERT"));
        assert!(ops.iter().any(|o| o.eq_ignore_ascii_case("insert")));
        assert!(!ops.iter().any(|o| o.eq_ignore_ascii_case("UPDATE")));
        assert!(!ops.iter().any(|o| o.eq_ignore_ascii_case("DELETE")));
    }
}
