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

/// TTL in milliseconds for `rs:reval:{key}` revalidation markers
/// (READYSET_REVAL_TTL_MS, default 3000). The 3s legacy size predates the
/// latency work — write→invalidation is measured at p50 11.4ms / max 16.9ms,
/// so deployments size this down (250ms ≈ 15x the measured max) to stop the
/// reval window from destroying the HIT rate under write churn. Parsed once.
static REVAL_TTL_MS: OnceLock<u64> = OnceLock::new();

fn reval_ttl_ms() -> u64 {
    *REVAL_TTL_MS.get_or_init(|| {
        let ttl = parse_reval_ttl_ms(env::var("READYSET_REVAL_TTL_MS").ok().as_deref());
        info!(reval_ttl_ms = ttl, "rs:reval marker TTL: {}ms", ttl);
        ttl
    })
}

/// Parse the READYSET_REVAL_TTL_MS value. Unset, unparsable or zero all fall
/// back to the legacy 3000ms default (a 0ms marker would expire before any
/// concurrent observer could see it, silently voiding the reval guard).
fn parse_reval_ttl_ms(raw: Option<&str>) -> u64 {
    raw.and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(3000)
}

/// Queue a `SET {prefix}rs:reval:{key} 1 PX {ttl_ms}` marker on `pipe`.
/// PX (millisecond expiry) replaces the old `SETEX <key> 3 1` — SETEX cannot
/// express sub-second TTLs. Phase 5B ORDER CONTRACT unchanged: callers must
/// queue markers BEFORE the DELs in the same pipeline.
fn add_reval_marker(pipe: &mut redis::Pipeline, prefix: &str, key: &str, ttl_ms: u64) {
    pipe.cmd("SET")
        .arg(format!("{}rs:reval:{}", prefix, key))
        .arg("1")
        .arg("PX")
        .arg(ttl_ms)
        .ignore();
}

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

    // 2a. Revalidation markers: the PHP MISS path checks rs:reval:{key} and,
    // when present, routes the rebuild's reads to the direct Postgres write
    // connection instead of ReadySet — closing the window where a refetch
    // races the dataflow's replication lag and re-caches pre-change data.
    // TTL = READYSET_REVAL_TTL_MS (default 3000ms): well above replication
    // lag (measured max 16.9ms), well below cache TTL.
    //
    // Phase 5B ORDER CONTRACT: markers BEFORE the DELs, so marker visibility
    // >= deletion visibility for every concurrent observer (the PHP
    // post-store guard and the GET-nil/EXISTS MISS path both rely on it).
    let reval_ttl = reval_ttl_ms();
    for k in &dependent_keys {
        add_reval_marker(&mut write_pipe, prefix, k, reval_ttl);
    }

    // 2b. DEL all dependent cache keys (with prefix), PLUS their
    // stale-while-revalidate twins ("{key}:stale", kept by PHP at ~3x TTL).
    // SWR exists to smooth TTL expiry, not to serve known-stale data: without
    // this, concurrent clients could be served pre-change data from :stale
    // for up to the extended TTL if the rebuild fails.
    let prefixed_keys = reverb_http::expand_keys_for_del(prefix, &dependent_keys);
    write_pipe.cmd("DEL").arg(&prefixed_keys).ignore();

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
            // Per-channel broadcast epochs: INCR rs:epoch:{authHash} once per
            // channel for this delivered batch, executed and AWAITED before
            // the HTTP POST (INCR visible before any client can receive the
            // event). After the DEL pipeline — marker-before-DEL untouched.
            let epochs = reverb_http::incr_epochs(conn, prefix, &grouped).await;
            reverb_http::broadcast_to_reverb(cfg, client, &grouped, &epochs).await;
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
    // Revalidation markers (rs:reval:{key}, TTL READYSET_REVAL_TTL_MS,
    // default 3000ms): tell the PHP MISS path to rebuild from the direct
    // Postgres connection instead of ReadySet, closing the replication-lag
    // re-cache race — see handle_row_invalidation.
    // Phase 5B order contract: markers BEFORE the DELs (see 2a there).
    let reval_ttl = reval_ttl_ms();
    for k in &dependent_keys {
        add_reval_marker(&mut write_pipe, prefix, k, reval_ttl);
    }

    // DEL values AND their ":stale" SWR twins — see handle_row_invalidation.
    let prefixed_keys = reverb_http::expand_keys_for_del(prefix, &dependent_keys);
    write_pipe.cmd("DEL").arg(&prefixed_keys).ignore();

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
            // Per-channel broadcast epochs — INCR awaited before the POST
            // (see handle_row_invalidation 2d for the ordering contract).
            let epochs = reverb_http::incr_epochs(conn, prefix, &grouped).await;
            reverb_http::broadcast_to_reverb(cfg, client, &grouped, &epochs).await;
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

// ---------------------------------------------------------------------------
// rs:lsn — engine-applied WAL position (consistency-token groundwork)
// ---------------------------------------------------------------------------
//
// `{prefix}rs:lsn` holds, as a decimal u64 string (pg LSN = (hi<<32)|lo), a
// WAL position whose effects are guaranteed visible through the engine's
// readers (port 5435 SELECTs). PHP stamps RSC cache values with this at
// rebuild start and serves a cached value to a client bearing token LSN T
// only if stamp >= T.
//
// THE INVARIANT (absolute): rs:lsn must NEVER exceed the LSN whose effects
// are reader-visible. Over-stating = stale data served as fresh. Under-
// stating = a client briefly bypasses cache (safe). When in doubt, lag.
//
// MECHANISM (holdback publisher — the "double-buffer lag" fallback,
// strengthened from one-batch to a fixed time bound):
//
// Readers carry no replication offsets — dataflow Update/eviction packets are
// offset-less, so the Tier-1 notifier cannot know which WAL position a reader
// delta corresponds to. The position IS known in the replicator's main loop:
// after `handle_action` returns Ok, the batch's table ops have been
// `perform_all().await`-acked by the base-table domains. From there the only
// remaining lag is inter-domain propagation to the readers, measured end-to-
// end (upstream write -> Tier-1 invalidation fired, which is strictly AFTER
// reader publish) at p50 11.4ms / max 16.9ms on this deployment.
//
// So: the main loop feeds every applied batch position into a single writer
// task, which publishes a position only after READYSET_LSN_HOLDBACK_MS
// (default 250ms ≈ 15x the measured max propagation, same margin rationale as
// READYSET_REVAL_TTL_MS) has elapsed since that batch was acked. Two
// additional guards:
//
// - COMPLETE TRANSACTIONS ONLY: a mid-transaction position (event lsn <
//   commit_lsn) is never published — publishing commit_lsn while only part of
//   the transaction's rows were applied would over-state within the batch.
//   Only commit-end / keepalive positions (lsn >= commit_lsn) qualify, and
//   what is published is commit_lsn itself (the conservative end of the pair;
//   any PHP token captured inside that transaction is < its COMMIT record).
// - MONOTONIC: the writer keeps a local high-watermark (single-threaded task
//   => no CAS needed), seeded from any existing rs:lsn value at startup
//   (read-modify-write), and never writes a smaller value.
//
// LAG BOUND: holdback + poll tick, i.e. ~250-313ms with defaults — during
// that window after a write, token-bearing clients bypass cache. That window
// is deliberately aligned with the rs:reval window (250ms) in which the PHP
// rebuild path bypasses the engine anyway.
//
// A failed SET simply drops that publish; a later (larger) position heals it
// — again the safe direction. TTL 7d, refreshed on every write; expiry on a
// >7d-idle deployment makes PHP treat every token as unsatisfiable (bypass —
// safe).

/// Channel into the rs:lsn writer task. Carries already-gated (commit-end
/// only) u64 LSNs.
static LSN_TX: OnceLock<mpsc::UnboundedSender<u64>> = OnceLock::new();

/// TTL for the rs:lsn key: 7 days, refreshed on every write.
const LSN_TTL_SECS: u64 = 7 * 86400;

/// Parse READYSET_LSN_HOLDBACK_MS. Unset, unparsable or zero fall back to
/// 250ms (a 0ms holdback would publish positions whose effects may not yet
/// be reader-visible — the unsafe direction).
pub(crate) fn parse_lsn_holdback_ms(raw: Option<&str>) -> u64 {
    raw.and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(250)
}

/// Extract the publishable applied-LSN from a replication offset.
///
/// Returns `Some(commit_lsn as u64)` only for Postgres positions at or past
/// their transaction's commit end (`lsn >= commit_lsn`): batch positions
/// mid-transaction (data-record LSNs are strictly below the COMMIT record's)
/// return None so a partially applied transaction is never claimed visible.
/// MySQL/GTID offsets return None (feature is Postgres-only).
pub(crate) fn applied_lsn_from_offset(
    offset: &replication_offset::ReplicationOffset,
) -> Option<u64> {
    match offset {
        replication_offset::ReplicationOffset::Postgres(p)
            if p.lsn.as_i64() >= p.commit_lsn.as_i64() =>
        {
            u64::try_from(p.commit_lsn.as_i64()).ok()
        }
        _ => None,
    }
}

/// Holdback + monotonic-watermark state for the rs:lsn writer. Pure (no I/O)
/// so the publish gating is unit-testable.
pub(crate) struct LsnHoldback {
    holdback: Duration,
    /// (lsn, first-observed) in strictly increasing lsn order — `observe`
    /// drops non-increasing positions, so the front is always the oldest AND
    /// smallest pending entry.
    pending: std::collections::VecDeque<(u64, std::time::Instant)>,
    newest_seen: u64,
    /// Last value published to Redis (or adopted from it at startup). Never
    /// decreases.
    watermark: u64,
}

impl LsnHoldback {
    pub(crate) fn new(holdback: Duration, initial_watermark: u64) -> Self {
        Self {
            holdback,
            pending: Default::default(),
            newest_seen: initial_watermark,
            watermark: initial_watermark,
        }
    }

    /// Record an applied position. Non-increasing positions are dropped (the
    /// replicator feed is monotonic per PostgresPosition ordering; duplicates
    /// arrive from keepalive re-reports).
    pub(crate) fn observe(&mut self, lsn: u64, now: std::time::Instant) {
        if lsn > self.newest_seen {
            self.newest_seen = lsn;
            self.pending.push_back((lsn, now));
        }
    }

    /// The largest pending position whose holdback has fully elapsed, if it
    /// beats the watermark. Advances the watermark — the caller MUST attempt
    /// the Redis write for every returned value (a failed SET under-states
    /// until the next larger position, which is the safe direction).
    pub(crate) fn publishable(&mut self, now: std::time::Instant) -> Option<u64> {
        let mut ripe = None;
        while let Some(&(lsn, seen)) = self.pending.front() {
            if now.duration_since(seen) >= self.holdback {
                ripe = Some(lsn);
                self.pending.pop_front();
            } else {
                break;
            }
        }
        match ripe {
            Some(lsn) if lsn > self.watermark => {
                self.watermark = lsn;
                Some(lsn)
            }
            _ => None,
        }
    }
}

/// Feed an applied batch position into the rs:lsn writer. Called from the
/// replication main loop AFTER `handle_action` returns Ok (i.e. after
/// `perform_all().await` acked every table op in the batch). Cheap no-op for
/// mid-transaction or non-Postgres positions.
pub fn notify_applied_position(offset: &replication_offset::ReplicationOffset) {
    if let Some(lsn) = applied_lsn_from_offset(offset) {
        let _ = get_or_init_lsn_tx().send(lsn);
    }
}

fn get_or_init_lsn_tx() -> &'static mpsc::UnboundedSender<u64> {
    LSN_TX.get_or_init(|| {
        let (tx, mut rx) = mpsc::unbounded_channel::<u64>();

        tokio::spawn(async move {
            let redis_url = env::var("READYSET_REDIS_URL")
                .unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
            let prefix = env::var("READYSET_REDIS_PREFIX").unwrap_or_default();
            let holdback_ms =
                parse_lsn_holdback_ms(env::var("READYSET_LSN_HOLDBACK_MS").ok().as_deref());

            let client = match redis::Client::open(redis_url.as_str()) {
                Ok(c) => c,
                Err(e) => {
                    error!(%e, "Failed to create Redis client for rs:lsn writer");
                    while rx.recv().await.is_some() {}
                    return;
                }
            };
            let mut conn = match client.get_multiplexed_async_connection().await {
                Ok(c) => c,
                Err(e) => {
                    error!(%e, "Failed to connect to Redis for rs:lsn writer");
                    while rx.recv().await.is_some() {}
                    return;
                }
            };

            let key = format!("{}rs:lsn", prefix);

            // Monotonic guard across restarts: adopt any existing value as the
            // floor (read-modify-write; safe because this task is the single
            // writer). The pre-restart value was reader-visible when written
            // and base tables restore at-or-past it (positions are persisted
            // with the applied data itself).
            let initial: u64 = redis::cmd("GET")
                .arg(&key)
                .query_async::<Option<String>>(&mut conn)
                .await
                .ok()
                .flatten()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);

            let mut hb = LsnHoldback::new(Duration::from_millis(holdback_ms), initial);
            // Poll tick: flushes ripe positions when the feed goes quiet.
            let tick = Duration::from_millis((holdback_ms / 4).clamp(25, 250));

            info!(holdback_ms, initial_watermark = initial, key = %key,
                  "rs:lsn writer started (applied-position holdback publisher)");

            loop {
                tokio::select! {
                    msg = rx.recv() => match msg {
                        Some(lsn) => hb.observe(lsn, std::time::Instant::now()),
                        None => break,
                    },
                    _ = tokio::time::sleep(tick) => {}
                }
                if let Some(lsn) = hb.publishable(std::time::Instant::now()) {
                    if let Err(e) = redis::cmd("SET")
                        .arg(&key)
                        .arg(lsn.to_string())
                        .arg("EX")
                        .arg(LSN_TTL_SECS)
                        .query_async::<()>(&mut conn)
                        .await
                    {
                        warn!(%e, lsn, "rs:lsn SET failed — will heal on next larger position");
                    }
                }
            }
        });

        tx
    })
}

/// Phase 5B: one-shot cold-start flush of the twin layer, fired when
/// streaming replication (re)starts.
///
/// Tier-1 invalidation is EVICTION-sourced. A process restart clears all
/// materialized reader state; a warm `rs:hyd` twin then short-circuits the
/// PHP read path (no SQL executes), the reader never re-materializes, and no
/// delta ever drops that twin again — writes go dark for every warm
/// twin-mode surface until its TTL (proven live in the Phase 5B battery:
/// post-restart sentinel writes produced ZERO twin invalidation while the
/// pilot kept serving HITs). Read-set (HYD) payloads are worse off: they
/// skipped Tier-2 row_deps by design, so their ONLY invalidation source is
/// the twin-drop cascade that just went silent.
///
/// The flush deletes all twins, twin indexes and hyd_deps sets, plus the
/// payloads those sets reference (rs:reval markers first — same order
/// contract as live invalidation), then broadcasts the dropped payload keys
/// so live browsers refetch. The next read of each surface MISSes ->
/// re-executes SQL -> re-materializes its reader -> Tier-1 is alive again.
pub fn startup_twin_flush() {
    tokio::spawn(async move {
        if let Err(e) = run_startup_twin_flush().await {
            warn!(%e, "Startup twin flush failed (twin-mode surfaces may serve stale reads until TTL)");
        }
    });
}

async fn run_startup_twin_flush() -> Result<(), redis::RedisError> {
    let redis_url = env::var("READYSET_REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
    let prefix = env::var("READYSET_REDIS_PREFIX").unwrap_or_default();
    let ws_server = env::var("READYSET_WEBSOCKET_SERVER")
        .unwrap_or_else(|_| "centrifugo".to_string());

    let client = redis::Client::open(redis_url.as_str())?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    // NOTE "rs:hyd:*" does not glob-match "rs:hyd_idx:*"/"rs:hyd_deps:*".
    let twins = scan_keys(&mut conn, &format!("{}rs:hyd:*", prefix)).await?;
    let idx_sets = scan_keys(&mut conn, &format!("{}rs:hyd_idx:*", prefix)).await?;
    let dep_sets = scan_keys(&mut conn, &format!("{}rs:hyd_deps:*", prefix)).await?;

    // Liveness / echo / promotion flags must be RE-PROVEN after a restart,
    // not carried over: restart clears reader state, and a cache may not
    // even exist in the rebuilt engine (upstream-sync 2026-08-10 finding:
    // after a restore failure the stale rs:twin_on + rs:t1_live let
    // TwinStore keep twin-serving a reader-less cache — reads were rebuilt
    // from proxied SQL, twins re-stored, and Tier-1 stayed permanently
    // dark). Dropping the flags forces every surface back through observed
    // delta-liveness before it can promote again; rs:twin_off (operator
    // demotion) is deliberately kept.
    let live_flags = scan_keys(&mut conn, &format!("{}rs:t1_live:*", prefix)).await?;
    let echo_flags = scan_keys(&mut conn, &format!("{}rs:t1_echo:*", prefix)).await?;
    let promo_flags = scan_keys(&mut conn, &format!("{}rs:twin_on:*", prefix)).await?;

    // Collect the (unprefixed) payload keys the hyd_deps sets reference.
    let mut payloads: Vec<String> = Vec::new();
    if !dep_sets.is_empty() {
        let mut read_pipe = redis::pipe();
        for s in &dep_sets {
            read_pipe.cmd("SMEMBERS").arg(s);
        }
        let results: Vec<Vec<String>> = read_pipe.query_async(&mut conn).await?;
        for members in results {
            for m in members {
                if !payloads.contains(&m) {
                    payloads.push(m);
                }
            }
        }
    }

    if twins.is_empty()
        && idx_sets.is_empty()
        && dep_sets.is_empty()
        && live_flags.is_empty()
        && echo_flags.is_empty()
        && promo_flags.is_empty()
    {
        info!("Startup twin flush: twin layer already cold, nothing to do");
        return Ok(());
    }

    let mut write_pipe = redis::pipe();
    // Order contract: rs:reval markers BEFORE the DELs.
    let reval_ttl = reval_ttl_ms();
    for t in &twins {
        let unprefixed = t.strip_prefix(prefix.as_str()).unwrap_or(t);
        add_reval_marker(&mut write_pipe, &prefix, unprefixed, reval_ttl);
    }
    for p in &payloads {
        add_reval_marker(&mut write_pipe, &prefix, p, reval_ttl);
    }
    if !twins.is_empty() {
        write_pipe.cmd("DEL").arg(&twins).ignore();
    }
    if !payloads.is_empty() {
        // expand_keys_for_del adds the prefix AND the ":stale" companions.
        let prefixed = reverb_http::expand_keys_for_del(&prefix, &payloads);
        write_pipe.cmd("DEL").arg(&prefixed).ignore();
    }
    if !idx_sets.is_empty() {
        write_pipe.cmd("DEL").arg(&idx_sets).ignore();
    }
    if !dep_sets.is_empty() {
        write_pipe.cmd("DEL").arg(&dep_sets).ignore();
    }
    for flags in [&live_flags, &echo_flags, &promo_flags] {
        if !flags.is_empty() {
            write_pipe.cmd("DEL").arg(flags.as_slice()).ignore();
        }
    }
    write_pipe.query_async::<()>(&mut conn).await?;

    info!(twins = twins.len(), idx_sets = idx_sets.len(),
          hyd_deps_sets = dep_sets.len(), payloads = payloads.len(),
          live_flags = live_flags.len(), echo_flags = echo_flags.len(),
          promo_flags = promo_flags.len(),
          "Startup twin flush: cold-start twin layer cleared (Tier-1 is eviction-sourced; readers must re-materialize and liveness must be re-proven)");

    // Broadcast the dropped payload keys so live browsers refetch.
    if ws_server == "reverb" && !payloads.is_empty() {
        if let Some(cfg) = ReverbConfig::from_env() {
            if let Ok(http) = reqwest::Client::builder()
                .timeout(Duration::from_millis(500))
                .build()
            {
                let grouped = group_by_auth_hash(&payloads);
                // Same epoch contract as live invalidation: INCR per channel,
                // awaited before the POST.
                let epochs = reverb_http::incr_epochs(&mut conn, &prefix, &grouped).await;
                reverb_http::broadcast_to_reverb(&cfg, &http, &grouped, &epochs).await;
            }
        }
    }

    Ok(())
}

/// Full incremental SCAN (never KEYS) for a match pattern.
async fn scan_keys(
    conn: &mut redis::aio::MultiplexedConnection,
    pattern: &str,
) -> Result<Vec<String>, redis::RedisError> {
    let mut keys = Vec::new();
    let mut cursor: u64 = 0;
    loop {
        let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(500)
            .query_async(conn)
            .await?;
        keys.extend(batch);
        if next == 0 {
            break;
        }
        cursor = next;
    }
    Ok(keys)
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

    /// READYSET_REVAL_TTL_MS parsing: unset/garbage/zero fall back to the
    /// legacy 3000ms default; a set value is honored (deployment: 250).
    #[test]
    fn test_reval_ttl_ms_parses_env_with_3000_default() {
        assert_eq!(parse_reval_ttl_ms(None), 3000);
        assert_eq!(parse_reval_ttl_ms(Some("250")), 250);
        assert_eq!(parse_reval_ttl_ms(Some("")), 3000);
        assert_eq!(parse_reval_ttl_ms(Some("abc")), 3000);
        assert_eq!(parse_reval_ttl_ms(Some("0")), 3000);
    }

    /// READYSET_LSN_HOLDBACK_MS parsing: unset/garbage/zero fall back to the
    /// 250ms default (0 would publish positions before their effects are
    /// reader-visible — the unsafe direction).
    #[test]
    fn test_parse_lsn_holdback_ms_default_250() {
        assert_eq!(parse_lsn_holdback_ms(None), 250);
        assert_eq!(parse_lsn_holdback_ms(Some("100")), 100);
        assert_eq!(parse_lsn_holdback_ms(Some(" 500 ")), 500);
        assert_eq!(parse_lsn_holdback_ms(Some("")), 250);
        assert_eq!(parse_lsn_holdback_ms(Some("abc")), 250);
        assert_eq!(parse_lsn_holdback_ms(Some("0")), 250);
        assert_eq!(parse_lsn_holdback_ms(Some("-1")), 250);
    }

    /// Commit-end gate: only Postgres positions with lsn >= commit_lsn are
    /// publishable (complete transactions / keepalives), and what is
    /// published is commit_lsn itself. Mid-transaction and non-Postgres
    /// offsets are rejected.
    #[test]
    fn test_applied_lsn_gates_on_commit_end() {
        use replication_offset::ReplicationOffset;
        use replication_offset::mysql::MySqlPosition;
        use replication_offset::postgres::{CommitLsn, PostgresPosition};

        let commit = CommitLsn::from(0x16_B374D848_i64);

        // commit_end: lsn == commit_lsn -> publish commit_lsn
        let off: ReplicationOffset = PostgresPosition::commit_end(commit).into();
        assert_eq!(applied_lsn_from_offset(&off), Some(0x16_B374D848_u64));

        // keepalive-style: lsn PAST the commit record -> still commit_lsn
        let off: ReplicationOffset = PostgresPosition::commit_end(commit)
            .with_lsn(0x16_B374D900_i64)
            .into();
        assert_eq!(applied_lsn_from_offset(&off), Some(0x16_B374D848_u64));

        // mid-transaction: data-record lsn below the commit record -> None
        let off: ReplicationOffset = PostgresPosition::commit_start(commit)
            .with_lsn(0x16_B374D000_i64)
            .into();
        assert_eq!(applied_lsn_from_offset(&off), None);

        // commit_start: (commit_lsn, 0) -> None
        let off: ReplicationOffset = PostgresPosition::commit_start(commit).into();
        assert_eq!(applied_lsn_from_offset(&off), None);

        // MySQL offsets never publish
        let off = ReplicationOffset::MySql(
            MySqlPosition::from_file_name_and_position("binlog.000001".to_string(), 4)
                .expect("valid binlog position"),
        );
        assert_eq!(applied_lsn_from_offset(&off), None);
    }

    /// Holdback gating: a position becomes publishable only after the full
    /// holdback has elapsed, the newest ripe position wins, and the watermark
    /// (incl. one adopted from Redis at startup) is strictly monotonic.
    #[test]
    fn test_lsn_holdback_delay_and_monotonic_watermark() {
        use std::time::Instant;
        let hold = Duration::from_millis(250);
        let t0 = Instant::now();

        let mut hb = LsnHoldback::new(hold, 0);
        hb.observe(100, t0);
        assert_eq!(hb.publishable(t0), None, "not ripe yet");
        assert_eq!(
            hb.publishable(t0 + Duration::from_millis(249)),
            None,
            "still inside the holdback"
        );
        assert_eq!(hb.publishable(t0 + hold), Some(100), "ripe at exactly holdback");
        assert_eq!(hb.publishable(t0 + hold), None, "already published — watermark holds");

        // Burst: multiple ripe positions collapse to the newest.
        hb.observe(110, t0 + Duration::from_millis(300));
        hb.observe(120, t0 + Duration::from_millis(301));
        hb.observe(120, t0 + Duration::from_millis(302)); // duplicate keepalive — dropped
        hb.observe(115, t0 + Duration::from_millis(303)); // non-increasing — dropped
        let later = t0 + Duration::from_millis(600);
        assert_eq!(hb.publishable(later), Some(120), "newest ripe position wins");
        assert_eq!(hb.publishable(later), None);

        // Startup adoption: positions at or below the adopted Redis value
        // never re-publish (never write a smaller value than present).
        let mut hb = LsnHoldback::new(hold, 500);
        hb.observe(400, t0);
        hb.observe(500, t0);
        assert_eq!(hb.publishable(t0 + hold), None, "<= adopted watermark stays unpublished");
        hb.observe(600, t0 + hold);
        assert_eq!(hb.publishable(t0 + hold + hold), Some(600));
    }

    /// The marker command is `SET {prefix}rs:reval:{key} 1 PX {ttl_ms}` —
    /// PX millisecond expiry (SETEX cannot express sub-second TTLs), value
    /// and key name unchanged from the legacy SETEX shape.
    #[test]
    fn test_reval_marker_emits_set_px_with_configured_ttl() {
        let mut pipe = redis::pipe();
        add_reval_marker(&mut pipe, "pfx-", "rs:gql:dashboard:en:anon:q1", parse_reval_ttl_ms(Some("250")));
        let packed = String::from_utf8_lossy(&pipe.get_packed_pipeline()).into_owned();
        assert!(packed.contains("SET"), "must use SET (not SETEX)");
        assert!(!packed.contains("SETEX"), "SETEX cannot express sub-second TTLs");
        assert!(packed.contains("pfx-rs:reval:rs:gql:dashboard:en:anon:q1"), "marker key unchanged");
        assert!(packed.contains("PX"), "must use PX millisecond expiry");
        assert!(packed.contains("250"), "configured TTL must be emitted");
    }
}
