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
//!
//! Tier 3 (table-level declared deps, `rs:deps:{table}`) fires on INSERT to
//! catch phantoms Tier 2 structurally cannot see, and predicate deps
//! (`rs:pred_deps:{schema}.{table}:{column}:{value}`, flag READYSET_PRED_DEPS)
//! narrow that same phantom case from whole-table to only the envelopes whose
//! scanned FK predicates match the inserted row.

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

            let mut conn = match readyset_util::redis_conn::reconnecting(&client).await {
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
    conn: &mut (impl redis::aio::ConnectionLike + Send),
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

    invalidate_dependent_keys(
        conn,
        &dependent_keys,
        prefix,
        ws_server,
        centrifugo_stream,
        reverb_config,
        http_client,
    )
    .await;

    Ok(())
}

/// Build the shared invalidation WRITE pipeline.
///
/// Extracted verbatim from the (previously duplicated) Tier-2 and Tier-3 write
/// phases so all invalidation sources — row deps, declared table deps and
/// predicate deps — emit byte-identical command shapes. Pure (no I/O) so the
/// packed RESP bytes are unit-testable.
///
/// Queue order is a contract, not an implementation detail:
/// 1. `SET {prefix}rs:reval:{key} 1 PX {ttl}` per key. The PHP MISS path checks
///    rs:reval:{key} and, when present, routes the rebuild's reads to the direct
///    Postgres write connection instead of ReadySet — closing the window where a
///    refetch races the dataflow's replication lag and re-caches pre-change data.
///    TTL = READYSET_REVAL_TTL_MS (default 3000ms): well above replication lag
///    (measured max 16.9ms), well below cache TTL.
/// 2. ONE `DEL` covering every dependent cache key AND its stale-while-
///    revalidate twin (`{key}:stale`, kept by PHP at ~3x TTL). SWR exists to
///    smooth TTL expiry, not to serve known-stale data: without the twin DEL,
///    concurrent clients could be served pre-change data from `:stale` for up to
///    the extended TTL when the rebuild fails.
/// 3. Centrifugo `XADD` per authHash channel — only when ws_server is centrifugo.
///
/// Phase 5B ORDER CONTRACT: markers BEFORE the DELs, so marker visibility >=
/// deletion visibility for every concurrent observer (the PHP post-store guard
/// and the GET-nil/EXISTS MISS path both rely on it).
fn build_invalidation_pipe(
    prefix: &str,
    dependent_keys: &[String],
    ws_server: &str,
    centrifugo_stream: &str,
    grouped: &HashMap<String, Vec<String>>,
    reval_ttl: u64,
) -> redis::Pipeline {
    let mut write_pipe = redis::pipe();

    for k in dependent_keys {
        add_reval_marker(&mut write_pipe, prefix, k, reval_ttl);
    }

    let prefixed_keys = reverb_http::expand_keys_for_del(prefix, dependent_keys);
    write_pipe.cmd("DEL").arg(&prefixed_keys).ignore();

    if ws_server == "centrifugo" {
        for (auth_hash, keys) in grouped {
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

            debug!(channel, key_count = keys.len(), "Published invalidation batch to Centrifugo");
        }
    }

    write_pipe
}

/// Execute the shared invalidation write phase for a resolved set of dependent
/// cache keys: reval markers + DEL (+ optional Centrifugo XADD) in ONE
/// round-trip, then the Reverb epoch INCR and HTTP broadcast.
///
/// The Reverb epoch INCR is executed AND awaited before the HTTP POST: the
/// counter must be visible to any Redis reader before a WebSocket client can
/// receive the event carrying that epoch. After the DEL pipeline —
/// marker-before-DEL untouched.
///
/// Shared by Tier 2 (row deps), Tier 3 (declared table deps) and predicate deps.
async fn invalidate_dependent_keys(
    conn: &mut (impl redis::aio::ConnectionLike + Send),
    dependent_keys: &[String],
    prefix: &str,
    ws_server: &str,
    centrifugo_stream: &str,
    reverb_config: Option<&ReverbConfig>,
    http_client: Option<&reqwest::Client>,
) {
    if dependent_keys.is_empty() {
        return;
    }

    let grouped = group_by_auth_hash(dependent_keys);

    let write_pipe = build_invalidation_pipe(
        prefix,
        dependent_keys,
        ws_server,
        centrifugo_stream,
        &grouped,
        reval_ttl_ms(),
    );

    // Execute all writes in one round-trip
    write_pipe.query_async::<()>(conn).await.ok();

    if ws_server == "reverb" {
        if let (Some(cfg), Some(client)) = (reverb_config, http_client) {
            let epochs = reverb_http::incr_epochs(conn, prefix, &grouped).await;
            reverb_http::broadcast_to_reverb(cfg, client, &grouped, &epochs).await;
        }
    }
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

            let mut conn = match readyset_util::redis_conn::reconnecting(&client).await {
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
    conn: &mut (impl redis::aio::ConnectionLike + Send),
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

    invalidate_dependent_keys(
        conn,
        &dependent_keys,
        prefix,
        ws_server,
        centrifugo_stream,
        reverb_config,
        http_client,
    )
    .await;

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
// Predicate deps — precise INSERT-phantom invalidation
// ---------------------------------------------------------------------------
//
// THE GAP. Tier 2 (rs:row_deps:{schema}.{table}:{pk}) is precise but blind to
// INSERTs: a brand-new row has no dep registered against it, because nothing
// has ever rendered it. Tier 3 catches the phantom, but at whole-table
// granularity — one attach on a join table flushes every declared listing
// cache for every user.
//
// THE FIX. PHP knows the FK predicates each cached envelope actually scanned
// (e.g. `subject_id IN (...)`) and registers them:
//
//     rs:pred_deps:{schema}.{table}:{column}:{value} -> SET of cache keys
//
// plus, per table, the set of columns worth looking at:
//
//     rs:pred_cols:{schema}.{table} -> SET of column names
//
// On a WAL INSERT we read the new row's values for the advertised columns and
// invalidate only the cache keys registered against those exact (column, value)
// pairs. An attach then invalidates only the envelopes that displayed that
// parent.
//
// STALENESS IS FAIL-SAFE. The advertised-column map is a periodically refreshed
// snapshot; an unknown table or a not-yet-seen column simply means "no lookup",
// i.e. the pre-feature status quo (Tier 3 still fires). It can never
// over-invalidate or serve stale data on its own.
//
// FLAG-GATED. READYSET_PRED_DEPS, default OFF. When off, `notify_pred_change`
// returns after one atomic load and the poll task never starts.

/// READYSET_PRED_DEPS gate. Parsed once on the first INSERT.
static PRED_DEPS_ENABLED: OnceLock<bool> = OnceLock::new();

fn pred_deps_enabled() -> bool {
    *PRED_DEPS_ENABLED.get_or_init(|| {
        let enabled = parse_pred_deps_enabled(env::var("READYSET_PRED_DEPS").ok().as_deref());
        info!(enabled, "Predicate deps (precise INSERT-phantom invalidation)");
        enabled
    })
}

/// Parse READYSET_PRED_DEPS. OFF unless explicitly "1" or "true"
/// (case-insensitive, whitespace-tolerant). Deliberately narrower than the
/// usual truthy zoo: a hot-path WAL hook must not switch on because someone
/// wrote "yes".
fn parse_pred_deps_enabled(raw: Option<&str>) -> bool {
    match raw {
        None => false,
        Some(s) => {
            let s = s.trim();
            s == "1" || s.eq_ignore_ascii_case("true")
        }
    }
}

/// Which WAL ops feed the predicate-dep pipeline (READYSET_PRED_DEPS_OPS,
/// subordinate to the READYSET_PRED_DEPS master flag). Parsed once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PredDepsOps {
    insert: bool,
    update: bool,
}

static PRED_DEPS_OPS: OnceLock<PredDepsOps> = OnceLock::new();

fn pred_deps_ops() -> PredDepsOps {
    *PRED_DEPS_OPS.get_or_init(|| {
        let ops = parse_pred_deps_ops(env::var("READYSET_PRED_DEPS_OPS").ok().as_deref());
        info!(?ops, "Predicate deps fire for ops");
        ops
    })
}

/// Parse READYSET_PRED_DEPS_OPS: comma-separated op names, case-insensitive,
/// whitespace-tolerant. `None` (unset) defaults to INSERT-only — exactly the
/// pre-feature behavior, so the update path ships dark. Unknown tokens are
/// ignored (an explicitly set garbage-only value therefore disables every op,
/// same fail-dark posture as the master flag).
fn parse_pred_deps_ops(raw: Option<&str>) -> PredDepsOps {
    match raw {
        None => PredDepsOps {
            insert: true,
            update: false,
        },
        Some(s) => {
            let mut ops = PredDepsOps {
                insert: false,
                update: false,
            };
            for tok in s.split(',') {
                let tok = tok.trim();
                if tok.eq_ignore_ascii_case("insert") {
                    ops.insert = true;
                } else if tok.eq_ignore_ascii_case("update") {
                    ops.update = true;
                }
            }
            ops
        }
    }
}

/// Parse READYSET_PRED_COLS_POLL_MS (advertised-column refresh interval).
/// Unset/garbage -> 30s; floored at 1s so a typo cannot turn the poller into a
/// SCAN hot loop.
fn parse_pred_cols_poll_ms(raw: Option<&str>) -> u64 {
    raw.and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(30_000)
        .max(1_000)
}

/// Snapshot of `rs:pred_cols:*`: qualified table -> advertised column names.
/// Replaced WHOLESALE by the poll task (never mutated in place), so a reader
/// either sees the previous complete snapshot or the next one.
static PRED_COLS: OnceLock<std::sync::Mutex<HashMap<String, std::sync::Arc<[String]>>>> =
    OnceLock::new();

fn pred_cols_map() -> &'static std::sync::Mutex<HashMap<String, std::sync::Arc<[String]>>> {
    PRED_COLS.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Recover the qualified table name from a `{prefix}rs:pred_cols:{qualified}`
/// key. Returns None for anything that is not such a key (the SCAN pattern is
/// authoritative, but a mis-parse would poison the advertised-column map).
fn pred_cols_table_from_key(prefix: &str, key: &str) -> Option<String> {
    let rest = key.strip_prefix(prefix)?.strip_prefix("rs:pred_cols:")?;
    if rest.is_empty() {
        return None;
    }
    Some(rest.to_string())
}

/// Refresh the advertised-column snapshot: SCAN for `rs:pred_cols:*`, one
/// pipelined SMEMBERS batch for all of them, then swap the map. Returns the
/// number of tables in the new snapshot.
async fn refresh_pred_cols(
    conn: &mut (impl redis::aio::ConnectionLike + Send),
    prefix: &str,
) -> Result<usize, redis::RedisError> {
    let keys = scan_keys(conn, &format!("{}rs:pred_cols:*", prefix)).await?;

    let mut snapshot: HashMap<String, std::sync::Arc<[String]>> = HashMap::new();

    if !keys.is_empty() {
        let mut read_pipe = redis::pipe();
        for k in &keys {
            read_pipe.cmd("SMEMBERS").arg(k);
        }
        let results: Vec<Vec<String>> = read_pipe.query_async(conn).await?;

        for (key, cols) in keys.iter().zip(results) {
            if cols.is_empty() {
                continue;
            }
            if let Some(table) = pred_cols_table_from_key(prefix, key) {
                snapshot.insert(table, cols.into());
            }
        }
    }

    let n = snapshot.len();
    if let Ok(mut guard) = pred_cols_map().lock() {
        *guard = snapshot;
    }
    Ok(n)
}

/// Start the advertised-column poll task exactly once (only ever reached with
/// the flag on). Refreshes immediately, then every
/// READYSET_PRED_COLS_POLL_MS.
fn ensure_pred_cols_poller() {
    static STARTED: OnceLock<()> = OnceLock::new();
    STARTED.get_or_init(|| {
        tokio::spawn(async move {
            let redis_url = env::var("READYSET_REDIS_URL")
                .unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
            let prefix = env::var("READYSET_REDIS_PREFIX").unwrap_or_default();
            let poll_ms =
                parse_pred_cols_poll_ms(env::var("READYSET_PRED_COLS_POLL_MS").ok().as_deref());

            let client = match redis::Client::open(redis_url.as_str()) {
                Ok(c) => c,
                Err(e) => {
                    error!(%e, "Failed to create Redis client for rs:pred_cols poller — predicate deps stay dark");
                    return;
                }
            };
            let mut conn = match readyset_util::redis_conn::reconnecting(&client).await {
                Ok(c) => c,
                Err(e) => {
                    error!(%e, "Failed to connect to Redis for rs:pred_cols poller — predicate deps stay dark");
                    return;
                }
            };

            info!(poll_ms, "rs:pred_cols poller started (advertised predicate columns)");

            let mut last_count = usize::MAX;
            loop {
                match refresh_pred_cols(&mut conn, &prefix).await {
                    Ok(n) => {
                        if n != last_count {
                            info!(tables = n, "Predicate deps: advertised-column snapshot refreshed");
                            last_count = n;
                        } else {
                            debug!(tables = n, "Predicate deps: advertised-column snapshot refreshed");
                        }
                    }
                    Err(e) => {
                        // Fail-safe: keep the previous snapshot. Worst case is
                        // the pre-feature status quo.
                        warn!(%e, "Predicate deps: rs:pred_cols refresh failed, keeping previous snapshot");
                    }
                }
                tokio::time::sleep(Duration::from_millis(poll_ms)).await;
            }
        });
    });
}

/// Message type for the predicate-dep notifier: one row event's worth of
/// (column, value) pairs for the columns its table advertised.
struct PredChangeMsg {
    /// Schema-qualified table, e.g. "public.bp_term_relationships"
    table: String,
    /// (column, Display-encoded value) pairs
    pairs: Vec<(String, String)>,
    /// Originating WAL op ("insert" | "update") — carried so the invalidation
    /// log line can attribute what fired it (live kill-tests grep it).
    op: &'static str,
}

static PRED_CHANGE_TX: OnceLock<mpsc::UnboundedSender<PredChangeMsg>> = OnceLock::new();

/// Pair the advertised columns with this row's values.
///
/// `columns[i]` names `tuple[i]` (alignment guaranteed by the WAL Relation
/// mapping — see `Relation::col_names`). NULLs are skipped: `DfValue::None`
/// Displays as the literal "NULL", which PHP never registers as a predicate
/// value, and treating it as one would conflate NULL with the text 'NULL'.
fn build_pred_pairs(
    advertised: &[String],
    columns: &[String],
    tuple: &[readyset_data::DfValue],
) -> Vec<(String, String)> {
    if advertised.is_empty() {
        return Vec::new();
    }

    let mut pairs = Vec::with_capacity(advertised.len());
    for (i, col) in columns.iter().enumerate() {
        if i >= tuple.len() {
            break;
        }
        if matches!(tuple[i], readyset_data::DfValue::None) {
            continue;
        }
        if advertised.iter().any(|a| a == col) {
            // Same Display encoding the Tier-2 pk path uses, which is what PHP
            // registers (bare uuid/text/int, no quoting).
            pairs.push((col.clone(), format!("{}", tuple[i])));
        }
    }
    pairs
}

/// UPDATE-by-key variant of `build_pred_pairs`: pair the advertised columns
/// with the NEW values an UpdateByKey carries.
///
/// `set` is full-width and positionally aligned with `columns` (one
/// `Modification` per relation column — see the WalEvent::UpdateByKey
/// construction sites). Only `Modification::Set(v)` emits: `None` means the
/// WAL message carried no new value for that column (unchanged TOASTed
/// value), and `Apply` never comes from the replication path. NULL new values
/// are skipped for the same reason as on INSERT (a row re-parented to NULL
/// joins no parent's envelope).
fn build_pred_pairs_update_by_key(
    advertised: &[String],
    columns: &[String],
    set: &[readyset_client::Modification],
) -> Vec<(String, String)> {
    if advertised.is_empty() {
        return Vec::new();
    }

    let mut pairs = Vec::with_capacity(advertised.len());
    for (i, col) in columns.iter().enumerate() {
        if i >= set.len() {
            break;
        }
        let readyset_client::Modification::Set(v) = &set[i] else {
            continue;
        };
        if matches!(v, readyset_data::DfValue::None) {
            continue;
        }
        if advertised.iter().any(|a| a == col) {
            pairs.push((col.clone(), format!("{}", v)));
        }
    }
    pairs
}

/// UPDATE-with-full-row variant of `build_pred_pairs` (UpdateRow only occurs
/// under REPLICA IDENTITY FULL, so the complete old tuple is available):
/// emit a (column, NEW value) pair only where the advertised column's value
/// actually CHANGED — firing on unchanged FK values would over-invalidate
/// every parent envelope on every row touch. NULL new values are skipped as
/// everywhere else.
///
/// If the old tuple is unusable (arity mismatch — should not happen, but a
/// defensive upstream bug must not turn into a missed invalidation), fall
/// back to emitting the new values for all advertised columns; over-
/// invalidation is safe, a stale envelope is not.
fn build_pred_pairs_update_row(
    advertised: &[String],
    columns: &[String],
    old_tuple: &[readyset_data::DfValue],
    new_tuple: &[readyset_data::DfValue],
) -> Vec<(String, String)> {
    if advertised.is_empty() {
        return Vec::new();
    }

    if old_tuple.len() != new_tuple.len() {
        return build_pred_pairs(advertised, columns, new_tuple);
    }

    let mut pairs = Vec::with_capacity(advertised.len());
    for (i, col) in columns.iter().enumerate() {
        if i >= new_tuple.len() {
            break;
        }
        if matches!(new_tuple[i], readyset_data::DfValue::None) {
            continue;
        }
        if new_tuple[i] == old_tuple[i] {
            continue;
        }
        if advertised.iter().any(|a| a == col) {
            pairs.push((col.clone(), format!("{}", new_tuple[i])));
        }
    }
    pairs
}

/// The Redis key PHP registers cache keys under for a predicate:
/// `{prefix}rs:pred_deps:{schema}.{table}:{column}:{value}`.
fn pred_lookup_key(prefix: &str, table: &str, col: &str, value: &str) -> String {
    format!("{}rs:pred_deps:{}:{}:{}", prefix, table, col, value)
}

/// Collapse a whole debounce window into ONE dedup'd list of lookup keys.
///
/// Tier-1 merge semantics, deliberately NOT Tier-2's per-message sequential
/// loop: M inserts in a window become one pipelined SMEMBERS batch (O(1-2)
/// round trips) instead of M. First-seen order is preserved so the batch is
/// deterministic and testable.
fn merge_pred_lookup_keys(prefix: &str, msgs: &[PredChangeMsg]) -> Vec<String> {
    let mut keys: Vec<String> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

    for msg in msgs {
        for (col, value) in &msg.pairs {
            let key = pred_lookup_key(prefix, &msg.table, col, value);
            if seen.insert(key.clone()) {
                keys.push(key);
            }
        }
    }
    keys
}

/// One pipelined SMEMBERS per merged lookup key — the whole read phase for a
/// debounce window in a single round trip.
fn build_pred_smembers_pipe(lookup_keys: &[String]) -> redis::Pipeline {
    let mut pipe = redis::pipe();
    for k in lookup_keys {
        pipe.cmd("SMEMBERS").arg(k);
    }
    pipe
}

fn get_or_init_pred_tx() -> &'static mpsc::UnboundedSender<PredChangeMsg> {
    PRED_CHANGE_TX.get_or_init(|| {
        let (tx, mut rx) = mpsc::unbounded_channel::<PredChangeMsg>();

        tokio::spawn(async move {
            let redis_url = env::var("READYSET_REDIS_URL")
                .unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
            let prefix = env::var("READYSET_REDIS_PREFIX").unwrap_or_default();
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
                    error!(%e, "Failed to create Redis client for predicate dep notifier");
                    while rx.recv().await.is_some() {}
                    return;
                }
            };

            let mut conn = match readyset_util::redis_conn::reconnecting(&client).await {
                Ok(c) => {
                    info!(url = %redis_url, ws_server = %ws_server,
                          "Redis predicate dep notifier connected (precise INSERT phantoms)");
                    c
                }
                Err(e) => {
                    error!(%e, "Failed to connect to Redis for predicate dep notifier");
                    while rx.recv().await.is_some() {}
                    return;
                }
            };

            // Predicate events arrive once per advertised INSERT, so the
            // debounce window carries real weight: a bulk attach of M rows to
            // one parent collapses to a single lookup key.
            let debounce_duration = Duration::from_millis(debounce_ms.max(10));

            loop {
                let first = match rx.recv().await {
                    Some(msg) => msg,
                    None => break,
                };

                let mut batch = vec![first];

                let deadline = tokio::time::Instant::now() + debounce_duration;
                loop {
                    match tokio::time::timeout_at(deadline, rx.recv()).await {
                        Ok(Some(msg)) => batch.push(msg),
                        Ok(None) => break,
                        Err(_) => break,
                    }
                }

                if let Err(e) = handle_pred_invalidation(
                    &mut conn,
                    &batch,
                    &prefix,
                    &ws_server,
                    &centrifugo_stream,
                    reverb_config.as_ref(),
                    http_client.as_ref(),
                )
                .await
                {
                    warn!(%e, batch = batch.len(), "Predicate deps: invalidation failed");
                }
            }
        });

        tx
    })
}

/// Handle one debounce window's worth of predicate changes.
///
/// Merge -> ONE pipelined SMEMBERS batch -> union/dedup of the returned cache
/// keys -> ONE shared write phase. Same policy as Tiers 1/2/3: cache VALUES are
/// deleted, the `rs:pred_deps:*` mappings persist (they are PHP-owned,
/// self-healing, and re-registered on the next MISS — see
/// handle_row_invalidation for the re-registration race rationale).
async fn handle_pred_invalidation(
    conn: &mut (impl redis::aio::ConnectionLike + Send),
    batch: &[PredChangeMsg],
    prefix: &str,
    ws_server: &str,
    centrifugo_stream: &str,
    reverb_config: Option<&ReverbConfig>,
    http_client: Option<&reqwest::Client>,
) -> Result<(), redis::RedisError> {
    let lookup_keys = merge_pred_lookup_keys(prefix, batch);
    if lookup_keys.is_empty() {
        return Ok(());
    }

    debug!(
        batch = batch.len(),
        lookups = lookup_keys.len(),
        "Predicate deps: resolving merged predicate lookups"
    );

    let member_sets: Vec<Vec<String>> = build_pred_smembers_pipe(&lookup_keys)
        .query_async(conn)
        .await
        .unwrap_or_default();

    // Union + dedup: one parent may back many envelopes, and one envelope may
    // be registered under several predicates in the same window.
    let mut dependent_keys: Vec<String> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for members in member_sets {
        for k in members {
            if seen.insert(k.clone()) {
                dependent_keys.push(k);
            }
        }
    }

    if dependent_keys.is_empty() {
        return Ok(());
    }

    // Op attribution: live kill-tests grep this line to prove which WAL op
    // (insert vs update) drove an invalidation.
    let inserts = batch.iter().filter(|m| m.op == "insert").count();
    let updates = batch.len() - inserts;
    info!(
        batch = batch.len(),
        inserts,
        updates,
        lookups = lookup_keys.len(),
        keys = dependent_keys.len(),
        "Predicate deps: invalidating envelopes matching changed rows"
    );

    invalidate_dependent_keys(
        conn,
        &dependent_keys,
        prefix,
        ws_server,
        centrifugo_stream,
        reverb_config,
        http_client,
    )
    .await;

    Ok(())
}

/// Notify that a row was INSERTed, for precise predicate-dep invalidation.
///
/// Called from the Postgres connector's Insert arm alongside
/// `notify_row_change` — the column names and full tuple only exist there.
///
/// Hot path, so the bail-outs come first and in cost order: flag (one atomic
/// load), empty snapshot (one mutex lock), unknown table (one hash miss). Only
/// a table that PHP has explicitly advertised ever allocates.
pub fn notify_pred_change(
    schema: &str,
    table: &str,
    columns: &[String],
    tuple: &[readyset_data::DfValue],
) {
    if !pred_deps_enabled() || !pred_deps_ops().insert {
        return;
    }

    let Some((qualified, advertised)) = pred_advertised_columns(schema, table) else {
        return;
    };

    send_pred_pairs(qualified, build_pred_pairs(&advertised, columns, tuple), "insert");
}

/// Notify that a row was UPDATEd (by-key form), for precise predicate-dep
/// invalidation of FK re-parenting: the parent GAINING the row cached a world
/// without it — the same phantom shape as INSERT. Same hot-path bail-out
/// ordering as `notify_pred_change`, plus the "update" op gate
/// (READYSET_PRED_DEPS_OPS — the update path ships dark).
///
/// Only the new-parent side: the losing parent's old FK value is structurally
/// absent from an UpdateByKey message (it would need REPLICA IDENTITY FULL)
/// and its displayed rows are already covered by Tier-2 row deps.
pub fn notify_pred_change_update_by_key(
    schema: &str,
    table: &str,
    columns: &[String],
    set: &[readyset_client::Modification],
) {
    if !pred_deps_enabled() || !pred_deps_ops().update {
        return;
    }

    let Some((qualified, advertised)) = pred_advertised_columns(schema, table) else {
        return;
    };

    send_pred_pairs(
        qualified,
        build_pred_pairs_update_by_key(&advertised, columns, set),
        "update",
    );
}

/// Notify that a row was UPDATEd (full-row form, REPLICA IDENTITY FULL), for
/// precise predicate-dep invalidation of FK re-parenting. The old tuple
/// enables a changed-only comparison — see `build_pred_pairs_update_row`.
pub fn notify_pred_change_update_row(
    schema: &str,
    table: &str,
    columns: &[String],
    old_tuple: &[readyset_data::DfValue],
    new_tuple: &[readyset_data::DfValue],
) {
    if !pred_deps_enabled() || !pred_deps_ops().update {
        return;
    }

    let Some((qualified, advertised)) = pred_advertised_columns(schema, table) else {
        return;
    };

    send_pred_pairs(
        qualified,
        build_pred_pairs_update_row(&advertised, columns, old_tuple, new_tuple),
        "update",
    );
}

/// Shared gate tail for every predicate-dep entry point: resolve the table's
/// advertised predicate columns, or bail as cheaply as possible.
///
/// Poller note: started here rather than on first send — the send path is
/// gated on a map hit, so a poller started from the sender would never run.
fn pred_advertised_columns(
    schema: &str,
    table: &str,
) -> Option<(String, std::sync::Arc<[String]>)> {
    ensure_pred_cols_poller();

    let guard = pred_cols_map().lock().ok()?;
    if guard.is_empty() {
        // No table advertises predicate columns (or the first poll has not
        // landed yet) — bail before allocating the qualified name.
        return None;
    }
    let qualified = format!("{}.{}", schema, table);
    let cols = guard.get(&qualified).map(std::sync::Arc::clone)?;
    Some((qualified, cols))
}

/// Shared send tail for every predicate-dep entry point.
fn send_pred_pairs(qualified: String, pairs: Vec<(String, String)>, op: &'static str) {
    if pairs.is_empty() {
        return;
    }

    debug!(
        table = %qualified,
        pairs = pairs.len(),
        op,
        "Predicate deps: queued predicates"
    );

    let _ = get_or_init_pred_tx().send(PredChangeMsg {
        table: qualified,
        pairs,
        op,
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
            let mut conn = match readyset_util::redis_conn::reconnecting(&client).await {
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
    conn: &mut (impl redis::aio::ConnectionLike + Send),
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

    // -----------------------------------------------------------------------
    // Predicate deps (INSERT-phantom narrowing)
    // -----------------------------------------------------------------------

    /// READYSET_PRED_DEPS gating: OFF unless explicitly set to "1"/"true"
    /// (case-insensitive, whitespace-tolerant). Anything else — including
    /// "yes", "on" and garbage — leaves the feature dark, because a
    /// half-understood truthy value must not silently enable a hot-path hook.
    #[test]
    fn test_pred_deps_flag_defaults_off_and_parses_truthy() {
        assert!(!parse_pred_deps_enabled(None));
        assert!(parse_pred_deps_enabled(Some("1")));
        assert!(parse_pred_deps_enabled(Some("true")));
        assert!(parse_pred_deps_enabled(Some("TRUE")));
        assert!(parse_pred_deps_enabled(Some("True")));
        assert!(parse_pred_deps_enabled(Some(" true ")));
        assert!(!parse_pred_deps_enabled(Some("0")));
        assert!(!parse_pred_deps_enabled(Some("false")));
        assert!(!parse_pred_deps_enabled(Some("")));
        assert!(!parse_pred_deps_enabled(Some("yes")));
        assert!(!parse_pred_deps_enabled(Some("on")));
    }

    /// READYSET_PRED_DEPS_OPS gating: unset defaults to INSERT-only — byte-
    /// identical to the pre-feature behavior, so the update path ships dark.
    #[test]
    fn test_pred_deps_ops_default_is_insert_only() {
        assert_eq!(
            parse_pred_deps_ops(None),
            PredDepsOps {
                insert: true,
                update: false,
            }
        );
    }

    /// Explicit op lists: comma-separated, case-insensitive, whitespace-
    /// tolerant; unknown tokens are ignored (a garbage-only value therefore
    /// disables every op — fail-dark, like the master flag).
    #[test]
    fn test_pred_deps_ops_parses_lists() {
        let both = PredDepsOps {
            insert: true,
            update: true,
        };
        let update_only = PredDepsOps {
            insert: false,
            update: true,
        };
        let neither = PredDepsOps {
            insert: false,
            update: false,
        };

        assert_eq!(parse_pred_deps_ops(Some("insert,update")), both);
        assert_eq!(parse_pred_deps_ops(Some("update")), update_only);
        assert_eq!(parse_pred_deps_ops(Some("UPDATE")), update_only, "case-insensitive");
        assert_eq!(parse_pred_deps_ops(Some(" Update , INSERT ")), both, "whitespace-tolerant");
        assert_eq!(
            parse_pred_deps_ops(Some("insert,delete,garbage")),
            PredDepsOps {
                insert: true,
                update: false,
            },
            "unknown tokens ignored"
        );
        assert_eq!(parse_pred_deps_ops(Some("bogus")), neither);
        assert_eq!(parse_pred_deps_ops(Some("")), neither, "explicit empty disables");
    }

    /// UpdateByKey extraction: `set` is full-width and column-order-aligned;
    /// only `Modification::Set` emits (None = no new value in the WAL
    /// message), non-advertised columns are dropped, NULL new values are
    /// skipped, and a short `set` must not panic.
    #[test]
    fn test_build_pred_pairs_update_by_key_set_only_and_aligned() {
        use readyset_client::Modification;
        use readyset_data::DfValue;

        let columns: Vec<String> = ["id", "shop_id", "author_id", "note"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let advertised: Vec<String> = ["shop_id", "note"].iter().map(|s| s.to_string()).collect();

        let set = vec![
            // Key column changed too — not advertised, must be ignored.
            Modification::Set(DfValue::from("row-uuid")),
            Modification::Set(DfValue::from("new-shop-uuid")),
            // Advertised elsewhere? No — author_id is not advertised here, and
            // even a Set on it must not emit.
            Modification::Set(DfValue::from("author-uuid")),
            // Advertised but carried no new value — must not emit.
            Modification::None,
        ];
        assert_eq!(
            build_pred_pairs_update_by_key(&advertised, &columns, &set),
            vec![("shop_id".to_string(), "new-shop-uuid".to_string())]
        );

        // NULL new value: re-parenting to no parent joins no envelope.
        let set_null = vec![
            Modification::Set(DfValue::from("row-uuid")),
            Modification::Set(DfValue::None),
            Modification::None,
            Modification::None,
        ];
        assert!(build_pred_pairs_update_by_key(&advertised, &columns, &set_null).is_empty());

        // Nothing advertised -> nothing emitted, no allocation churn.
        assert!(build_pred_pairs_update_by_key(&[], &columns, &set).is_empty());

        // Short set (defensive) must not index out of bounds.
        let short = vec![Modification::Set(DfValue::from("row-uuid"))];
        assert!(build_pred_pairs_update_by_key(&advertised, &columns, &short).is_empty());
    }

    /// UpdateRow extraction (REPLICA IDENTITY FULL): only advertised columns
    /// whose value actually CHANGED emit — firing on unchanged FK values
    /// would over-invalidate every parent envelope on every row touch. If the
    /// old tuple is unusable (arity mismatch), fall back to emitting the new
    /// values (over-invalidation is safe, a stale envelope is not).
    #[test]
    fn test_build_pred_pairs_update_row_changed_only() {
        use readyset_data::DfValue;

        let columns: Vec<String> = ["id", "shop_id", "author_id"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let advertised: Vec<String> = ["shop_id", "author_id"]
            .iter()
            .map(|s| s.to_string())
            .collect();

        let old_tuple = vec![
            DfValue::from("row-uuid"),
            DfValue::from("old-shop-uuid"),
            DfValue::from("author-uuid"),
        ];
        let new_tuple = vec![
            DfValue::from("row-uuid"),
            DfValue::from("new-shop-uuid"),
            DfValue::from("author-uuid"), // unchanged — must not emit
        ];
        assert_eq!(
            build_pred_pairs_update_row(&advertised, &columns, &old_tuple, &new_tuple),
            vec![("shop_id".to_string(), "new-shop-uuid".to_string())]
        );

        // Nothing changed -> nothing emitted.
        assert!(
            build_pred_pairs_update_row(&advertised, &columns, &old_tuple, &old_tuple).is_empty()
        );

        // NULL -> value counts as changed; value -> NULL is skipped (NULL
        // joins no parent's envelope).
        let old_null = vec![
            DfValue::from("row-uuid"),
            DfValue::None,
            DfValue::from("author-uuid"),
        ];
        assert_eq!(
            build_pred_pairs_update_row(&advertised, &columns, &old_null, &new_tuple),
            vec![("shop_id".to_string(), "new-shop-uuid".to_string())]
        );
        assert!(
            build_pred_pairs_update_row(&advertised, &columns, &new_tuple, &old_null).is_empty()
        );

        // Unusable old tuple (arity mismatch) -> fall back to new values.
        let short_old = vec![DfValue::from("row-uuid")];
        assert_eq!(
            build_pred_pairs_update_row(&advertised, &columns, &short_old, &new_tuple),
            vec![
                ("shop_id".to_string(), "new-shop-uuid".to_string()),
                ("author_id".to_string(), "author-uuid".to_string()),
            ]
        );
    }

    /// End-to-end shape for an update-derived pair: it must ride the SAME
    /// merge -> pipelined-SMEMBERS path as inserts, producing an exact
    /// `SMEMBERS rs:pred_deps:{schema}.{table}:{column}:{value}` lookup.
    #[test]
    fn test_pred_smembers_pipeline_for_update_derived_pair() {
        use readyset_client::Modification;
        use readyset_data::DfValue;

        let columns: Vec<String> = ["id", "shop_id"].iter().map(|s| s.to_string()).collect();
        let advertised: Vec<String> = vec!["shop_id".to_string()];
        let set = vec![
            Modification::None,
            Modification::Set(DfValue::from("new-shop-uuid")),
        ];

        let msgs = vec![PredChangeMsg {
            table: "public.bp_products".to_string(),
            pairs: build_pred_pairs_update_by_key(&advertised, &columns, &set),
            op: "update",
        }];

        let keys = merge_pred_lookup_keys("gz:", &msgs);
        assert_eq!(
            keys,
            vec!["gz:rs:pred_deps:public.bp_products:shop_id:new-shop-uuid".to_string()]
        );

        let packed =
            String::from_utf8_lossy(&build_pred_smembers_pipe(&keys).get_packed_pipeline())
                .into_owned();
        assert!(packed.contains("SMEMBERS"));
        assert!(packed.contains("rs:pred_deps:public.bp_products:shop_id:new-shop-uuid"));
    }

    /// The lookup key PHP registers under. Exact shape is a cross-layer
    /// contract: `{prefix}rs:pred_deps:{schema}.{table}:{column}:{value}`.
    #[test]
    fn test_pred_lookup_key_format_is_exact() {
        assert_eq!(
            pred_lookup_key(
                "",
                "public.bp_term_relationships",
                "subject_id",
                "0198e0d2-6a1f-7000-8000-1c2f3b4a5d6e"
            ),
            "rs:pred_deps:public.bp_term_relationships:subject_id:0198e0d2-6a1f-7000-8000-1c2f3b4a5d6e"
        );
        // Deployment prefix rides in front of the whole key, as everywhere else.
        assert_eq!(
            pred_lookup_key("gz:", "public.bp_posts", "author_id", "42"),
            "gz:rs:pred_deps:public.bp_posts:author_id:42"
        );
    }

    /// Poll interval: unset/garbage -> 30s, floored at 1s so a typo cannot
    /// turn the advertised-column poller into a SCAN hot loop.
    #[test]
    fn test_pred_cols_poll_ms_default_and_floor() {
        assert_eq!(parse_pred_cols_poll_ms(None), 30_000);
        assert_eq!(parse_pred_cols_poll_ms(Some("5000")), 5_000);
        assert_eq!(parse_pred_cols_poll_ms(Some(" 60000 ")), 60_000);
        assert_eq!(parse_pred_cols_poll_ms(Some("abc")), 30_000);
        assert_eq!(parse_pred_cols_poll_ms(Some("0")), 30_000);
        assert_eq!(parse_pred_cols_poll_ms(Some("10")), 1_000, "floored");
    }

    /// Advertised columns select which (column, value) pairs leave the hot
    /// path. Positional alignment with the tuple must be honoured, unadvertised
    /// columns dropped, NULLs skipped (DfValue::None Displays as the literal
    /// "NULL", which PHP never registers as a predicate value), and
    /// out-of-range advertised columns must not panic.
    #[test]
    fn test_build_pred_pairs_selects_advertised_columns_only() {
        use readyset_data::DfValue;

        let columns: Vec<String> = ["id", "subject_id", "term_id", "note"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let tuple = vec![
            DfValue::from("row-uuid"),
            DfValue::from("subject-uuid"),
            DfValue::from(7i64),
            DfValue::None,
        ];

        let advertised: Vec<String> = ["subject_id", "term_id", "note", "missing_col"]
            .iter()
            .map(|s| s.to_string())
            .collect();

        assert_eq!(
            build_pred_pairs(&advertised, &columns, &tuple),
            vec![
                ("subject_id".to_string(), "subject-uuid".to_string()),
                ("term_id".to_string(), "7".to_string()),
            ]
        );

        // Nothing advertised for this table -> no pairs, no allocation churn.
        assert!(build_pred_pairs(&[], &columns, &tuple).is_empty());

        // Short tuple (defensive: mapping/tuple arity mismatch upstream) must
        // not index out of bounds.
        let short = vec![DfValue::from("row-uuid")];
        assert!(build_pred_pairs(&advertised, &columns, &short).is_empty());
    }

    /// A debounce window merges EVERY message into ONE dedup'd lookup-key set
    /// (Tier-1 merge semantics, not Tier-2's per-key sequential loop): M
    /// inserts touching the same parent collapse to a single SMEMBERS.
    #[test]
    fn test_merge_pred_lookup_keys_dedups_across_messages() {
        let msgs = vec![
            PredChangeMsg {
                table: "public.bp_term_relationships".to_string(),
                pairs: vec![
                    ("subject_id".to_string(), "aaa".to_string()),
                    ("term_id".to_string(), "7".to_string()),
                ],
                op: "insert",
            },
            // Same parent attached again in the same window — must not duplicate.
            PredChangeMsg {
                table: "public.bp_term_relationships".to_string(),
                pairs: vec![("subject_id".to_string(), "aaa".to_string())],
                op: "insert",
            },
            // Different value on the same column — distinct lookup. An
            // update-derived message merges identically to an insert-derived
            // one (the op tag is log attribution only).
            PredChangeMsg {
                table: "public.bp_term_relationships".to_string(),
                pairs: vec![("subject_id".to_string(), "bbb".to_string())],
                op: "update",
            },
            // Different table, same column/value — distinct lookup.
            PredChangeMsg {
                table: "public.bp_comments".to_string(),
                pairs: vec![("subject_id".to_string(), "aaa".to_string())],
                op: "insert",
            },
        ];

        let keys = merge_pred_lookup_keys("gz:", &msgs);
        assert_eq!(
            keys,
            vec![
                "gz:rs:pred_deps:public.bp_term_relationships:subject_id:aaa".to_string(),
                "gz:rs:pred_deps:public.bp_term_relationships:term_id:7".to_string(),
                "gz:rs:pred_deps:public.bp_term_relationships:subject_id:bbb".to_string(),
                "gz:rs:pred_deps:public.bp_comments:subject_id:aaa".to_string(),
            ],
            "5 pairs across 4 messages must collapse to 4 unique lookups, in first-seen order"
        );
        assert!(merge_pred_lookup_keys("gz:", &[]).is_empty());
    }

    /// The read phase is ONE pipelined SMEMBERS batch covering every merged
    /// lookup key — bounding a window of M inserts to O(1) read round-trips.
    #[test]
    fn test_pred_smembers_pipeline_batches_every_lookup_key() {
        let keys = vec![
            "gz:rs:pred_deps:public.bp_term_relationships:subject_id:aaa".to_string(),
            "gz:rs:pred_deps:public.bp_term_relationships:subject_id:bbb".to_string(),
        ];
        let pipe = build_pred_smembers_pipe(&keys);
        let packed = String::from_utf8_lossy(&pipe.get_packed_pipeline()).into_owned();

        for k in &keys {
            assert!(packed.contains(k.as_str()), "missing lookup key {k} in batch");
        }
        assert_eq!(packed.matches("SMEMBERS").count(), 2, "one SMEMBERS per unique key");
    }

    /// `rs:pred_cols:{qualified}` -> `{qualified}`, prefix stripped. Keys that
    /// are not pred_cols keys are rejected (the SCAN pattern is authoritative,
    /// but a mis-parse would silently poison the advertised-column map).
    #[test]
    fn test_pred_cols_table_from_key() {
        assert_eq!(
            pred_cols_table_from_key("gz:", "gz:rs:pred_cols:public.bp_term_relationships"),
            Some("public.bp_term_relationships".to_string())
        );
        assert_eq!(
            pred_cols_table_from_key("", "rs:pred_cols:public.bp_posts"),
            Some("public.bp_posts".to_string())
        );
        assert_eq!(pred_cols_table_from_key("gz:", "rs:pred_cols:public.x"), None);
        assert_eq!(pred_cols_table_from_key("", "rs:pred_deps:public.x:c:v"), None);
        assert_eq!(pred_cols_table_from_key("", "rs:pred_cols:"), None);
    }

    /// REFACTOR PIN: the shared write phase extracted from the Tier-2 and
    /// Tier-3 handlers must still emit, in this order, a `SET rs:reval:{key}`
    /// marker per key and ONE `DEL` covering both the value key and its
    /// `:stale` SWR twin. Any drift here is a live invalidation regression.
    #[test]
    fn test_invalidation_pipe_emits_reval_markers_then_del_with_stale_twins() {
        let keys = vec![
            "rs:gql:dashboard:en:anon:q1".to_string(),
            "rs:lw:anon:Comp_x:p1".to_string(),
        ];
        let grouped = group_by_auth_hash(&keys);
        let pipe = build_invalidation_pipe("gz:", &keys, "reverb", "centrifugo:rsc", &grouped, 250);
        let packed = String::from_utf8_lossy(&pipe.get_packed_pipeline()).into_owned();

        // Markers, one per key, with the configured PX TTL.
        assert_eq!(packed.matches("rs:reval:").count(), 2);
        assert!(packed.contains("gz:rs:reval:rs:gql:dashboard:en:anon:q1"));
        assert!(packed.contains("gz:rs:reval:rs:lw:anon:Comp_x:p1"));
        assert!(packed.contains("PX"));
        assert!(packed.contains("250"));

        // Value keys AND their :stale twins, in a single DEL.
        assert_eq!(packed.matches("DEL").count(), 1);
        assert!(packed.contains("gz:rs:gql:dashboard:en:anon:q1:stale"));
        assert!(packed.contains("gz:rs:lw:anon:Comp_x:p1:stale"));

        // Order contract (Phase 5B): every marker is queued BEFORE the DEL.
        let del_at = packed.find("DEL").expect("DEL present");
        assert!(
            packed.match_indices("rs:reval:").all(|(i, _)| i < del_at),
            "rs:reval markers must precede the DEL"
        );

        // ws_server=reverb must NOT queue a Centrifugo XADD.
        assert!(!packed.contains("XADD"));
    }

    /// Same builder under Centrifugo: one XADD per authHash channel, appended
    /// after the DEL — unchanged from the pre-refactor handlers.
    #[test]
    fn test_invalidation_pipe_adds_centrifugo_xadd_when_configured() {
        let keys = vec!["rs:gql:dashboard:en:anon:q1".to_string()];
        let grouped = group_by_auth_hash(&keys);
        let pipe = build_invalidation_pipe("", &keys, "centrifugo", "centrifugo:rsc", &grouped, 3000);
        let packed = String::from_utf8_lossy(&pipe.get_packed_pipeline()).into_owned();

        assert_eq!(packed.matches("XADD").count(), 1);
        assert!(packed.contains("centrifugo:rsc"));
        assert!(packed.contains("rsc:anon"));
        assert!(
            packed.find("DEL").unwrap() < packed.find("XADD").unwrap(),
            "XADD is queued after the DEL, as in the original handlers"
        );
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
