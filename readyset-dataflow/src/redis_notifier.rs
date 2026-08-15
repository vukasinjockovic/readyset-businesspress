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
pub(crate) struct InvalidationMsg {
    cache_name: String,
    key_values: Vec<String>,
}

/// Channel for sending invalidation messages to the background Redis publisher.
static INVALIDATION_TX: OnceLock<mpsc::UnboundedSender<InvalidationMsg>> = OnceLock::new();

/// The Redis channel name for query-level invalidations (Tier 1).
const INVALIDATION_CHANNEL: &str = "readyset:invalidations";

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
pub(crate) fn parse_reval_ttl_ms(raw: Option<&str>) -> u64 {
    raw.and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(3000)
}

/// Queue a `SET {prefix}rs:reval:{key} 1 PX {ttl_ms}` marker on `pipe`.
/// PX (millisecond expiry) replaces the old `SETEX <key> 3 1` — SETEX cannot
/// express sub-second TTLs. Phase 5B ORDER CONTRACT unchanged: callers must
/// queue markers BEFORE the DELs in the same pipeline.
pub(crate) fn add_reval_marker(
    pipe: &mut redis::Pipeline,
    prefix: &str,
    key: &str,
    ttl_ms: u64,
) {
    pipe.cmd("SET")
        .arg(format!("{}rs:reval:{}", prefix, key))
        .arg("1")
        .arg("PX")
        .arg(ttl_ms)
        .ignore();
}

/// Lazily initialize the background Redis publisher task.
///
/// LIFECYCLE (critical): the first `notify_invalidation` call arrives on a
/// DOMAIN thread, and each domain runs its own current-thread tokio runtime
/// (readyset-server worker/mod.rs `new_current_thread`) that is DROPPED when
/// that domain shuts down (cache removal, rebalance). A task `tokio::spawn`ed
/// from here would die with that runtime while the static sender lives on —
/// every later Tier-1 message process-wide would be silently dropped forever
/// (observed live 2026-08-11: DROP CACHE of the probe cache that happened to
/// initialize the notifier killed Tier-1 until restart). The notifier loop
/// therefore runs on its OWN dedicated thread with its own runtime, tied to
/// the process, not to whichever domain fired the first delta.
fn get_or_init_tx() -> &'static mpsc::UnboundedSender<InvalidationMsg> {
    INVALIDATION_TX.get_or_init(|| {
        let (tx, rx) = mpsc::unbounded_channel::<InvalidationMsg>();

        if let Err(e) = std::thread::Builder::new()
            .name("rsc-tier1-notifier".to_string())
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        error!(%e, "Tier 1 notifier: failed to build runtime — Tier 1 disabled");
                        return;
                    }
                };
                rt.block_on(notifier_loop(rx));
            })
        {
            error!(%e, "Tier 1 notifier: failed to spawn thread — Tier 1 disabled");
        }

        tx
    })
}

/// The Tier-1 notifier receive loop (runs on the dedicated notifier thread).
async fn notifier_loop(mut rx: mpsc::UnboundedReceiver<InvalidationMsg>) {
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

            let mut conn = match readyset_util::redis_conn::reconnecting(&client).await {
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

                    // Deduplicate by cache_name — merge key_values across messages.
                    // Broad (empty key_values) is ABSORBING per cache_name: see
                    // merge_debounce_batch.
                    let dedup = merge_debounce_batch(&batch);

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
}

/// Append delta-liveness telemetry commands to an existing pipeline so they
/// ride an already-scheduled round trip (zero extra round trips).
///
/// Written on EVERY delivered Reader delta, whether or not any dependent keys
/// are registered — "CREATE CACHE succeeded" does not guarantee delta delivery
/// (e.g. correlated-subselect dataflows serve reads but never notify).
/// Debounced/dedup'd batches call handle_invalidation once per unique
/// cache_name per flush, so this is set once per dedup'd cache_name.
///
/// - `rs:t1_seen:{cache_name}`  — legacy audit key (kept for existing tooling)
/// - `rs:t1_live:{cache_name}`  — ground-truth delta-liveness registry; the
///   Phase-2 twin eligibility gate consumes this ("this cache's delta path is
///   alive"). Value = unix timestamp of the last delivered delta, TTL 7 days.
/// - `rs:t1_echo:{cache_name}`  — identity echo (Phase-5 condition C1): a
///   sample of the param_key THIS ENGINE actually emits for the cache, written
///   on GRANULAR deltas only (`echo_sample = Some(...)`). PHP's promotion gate
///   compares its own binding arity against the echoed arity before granting
///   `rs:twin_on` — a shape whose PHP param encoding disagrees with the
///   engine's emission (inlined-literal params, multi-value collapsed-IN
///   reads) must never store twins the engine cannot drop. Broad deltas carry
///   no param_key and write no echo.
fn add_liveness_cmds(
    pipe: &mut redis::Pipeline,
    prefix: &str,
    cache_name: &str,
    echo_sample: Option<&str>,
) {
    let now_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    pipe.cmd("SET")
        .arg(format!("{}rs:t1_seen:{}", prefix, cache_name))
        .arg(now_ts)
        .arg("EX")
        .arg(7 * 86400)
        .ignore();
    pipe.cmd("SETEX")
        .arg(format!("{}rs:t1_live:{}", prefix, cache_name))
        .arg(7 * 86400)
        .arg(now_ts)
        .ignore();
    if let Some(sample) = echo_sample {
        pipe.cmd("SETEX")
            .arg(format!("{}{}", prefix, t1_echo_key(cache_name)))
            .arg(7 * 86400)
            .arg(sample)
            .ignore();
    }
}

/// Unprefixed identity-echo key (see `add_liveness_cmds`). PHP twin:
/// `RscTwinIdentity::echoKey` — the promotion gate GETs this and compares
/// arity (`substr_count('|') + 1`) against its own binding count.
pub(crate) fn t1_echo_key(cache_name: &str) -> String {
    format!("rs:t1_echo:{cache_name}")
}

/// Unprefixed hydrated-twin value key for a `(cache_name, param_key)` pair.
///
/// Contract-locked (`twin_identities` in fixtures/rsc-contract.json; PHP twin:
/// `RscTwinIdentity::twinKey`): `rs:hyd:{cache_name}:{param_key}`, and the
/// literal `_` segment when `param_key` is empty (non-parameterized cache).
/// `param_key` itself is the pipe-joined, placeholder-ordered DfValue Display
/// encoding produced by reader.rs (extract_key_column_values /
/// eviction_key_values) — PHP normalizes TO that encoding.
pub(crate) fn twin_key(cache_name: &str, param_key: &str) -> String {
    if param_key.is_empty() {
        format!("rs:hyd:{cache_name}:_")
    } else {
        format!("rs:hyd:{cache_name}:{param_key}")
    }
}

/// Unprefixed per-cache index SET of twin keys PHP has stored (SADDed by
/// TwinStore at store time, 7d TTL). Broad invalidation drains this set
/// instead of SCANning the keyspace in the hot path.
pub(crate) fn twin_idx_key(cache_name: &str) -> String {
    format!("rs:hyd_idx:{cache_name}")
}

/// Phase 3 — unprefixed read-set dependents SET for one twin identity:
/// `rs:hyd_deps:{cache_name}:{param_key}` (the `_` placeholder mirrors
/// `twin_key()` for the empty/non-parameterized form).
///
/// Contract-locked (`expected_hyd_deps_key` in fixtures/rsc-contract.json;
/// PHP twin: `RscTwinIdentity::hydDepsKey`). Members are composed-payload
/// cache keys (rs:gql:*) whose MISS composition read EXACTLY this identity
/// while its whole read set was twin-promoted; PHP registers them with
/// SADD + 7d EXPIRE and re-registers on every re-composition.
pub(crate) fn hyd_deps_key(cache_name: &str, param_key: &str) -> String {
    if param_key.is_empty() {
        format!("rs:hyd_deps:{cache_name}:_")
    } else {
        format!("rs:hyd_deps:{cache_name}:{param_key}")
    }
}

/// Map a twin VALUE key (`rs:hyd:{cache_name}:{param_key}`, `_` form
/// included) to its read-set dependents SET key. Returns None for non-twin
/// input (defensive — twin_dels only ever contains twin keys).
pub(crate) fn hyd_deps_key_for_twin(twin_key: &str) -> Option<String> {
    twin_key
        .strip_prefix("rs:hyd:")
        .map(|identity| format!("rs:hyd_deps:{identity}"))
}

/// Merge one debounce window's messages per cache_name into the key_values
/// list handle_invalidation consumes (empty = broad, whole-cache).
///
/// Broad is ABSORBING: one broad message (empty key_values) makes the merged
/// entry broad, regardless of what granular messages share the window. The
/// pre-fix code merged by appending key_values only, so a broad message
/// merged with granular ones for the SAME cache_name was silently demoted to
/// granular — the idx drain (`rs:hyd_idx` members for params outside the
/// window), the broad `rs:deps` lookup and the hyd_deps sets of un-named
/// param twins were all SKIPPED for that window, and nothing ever replayed
/// them (a "delayed" broad drop only happens if a later window is broad-only).
/// Broad ⊇ granular for every artifact class (PHP always registers
/// `rs:deps:{cache}` alongside pdeps — RscGraphQLCache::registerDeps — and
/// `rs:hyd_idx` indexes every stored twin), so absorbing into broad can only
/// over-invalidate, never under-invalidate.
pub(crate) fn merge_debounce_batch(batch: &[InvalidationMsg]) -> HashMap<String, Vec<String>> {
    let mut dedup: HashMap<String, (bool, Vec<String>)> = HashMap::new();
    for msg in batch {
        let entry = dedup
            .entry(msg.cache_name.clone())
            .or_insert_with(|| (false, Vec::new()));
        if msg.key_values.is_empty() {
            // Broad message — absorb: drop any granular keys already merged
            // and ignore any that follow in this window.
            entry.0 = true;
            entry.1.clear();
        } else if !entry.0 {
            for kv in &msg.key_values {
                if !entry.1.contains(kv) {
                    entry.1.push(kv.clone());
                }
            }
        }
    }
    dedup.into_iter().map(|(name, (_, kvs))| (name, kvs)).collect()
}

/// Merge `extra` into `dest`, skipping duplicates (payload keys may be found
/// both via pdeps/deps AND via a hyd_deps cascade — they must be DELed and
/// broadcast exactly once).
pub(crate) fn merge_unique(dest: &mut Vec<String>, extra: impl IntoIterator<Item = String>) {
    for k in extra {
        if !dest.contains(&k) {
            dest.push(k);
        }
    }
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
/// Hydrated twins (`rs:hyd:{cache_name}:{param_key}`) are dropped alongside,
/// UNCONDITIONALLY (they exist independently of any registered payload deps):
/// - granular: one DEL per affected param_key, plus the `_` (empty-params) twin
/// - broad: the `_` twin plus every member of `rs:hyd_idx:{cache_name}` (the
///   per-cache index SET maintained by PHP at store time), then the index itself
/// Every dropped twin also gets a `rs:reval:{twin_key}` marker (TTL
/// READYSET_REVAL_TTL_MS, default 3000ms) so the PHP refill inside the
/// replication-lag window routes to direct Postgres and does NOT store (same
/// guard as the payload keys). Twin keys are server-side only and are never
/// broadcast.
///
/// Uses Redis pipelines to minimize round-trips:
/// - Phase 1 (read): pipeline SMEMBERS for dep resolution (pdeps or broad deps
///   + hyd_idx on the broad path)
/// - Phase 2 (read): pipeline SMEMBERS for key_pdeps + key_deps reverse lookups
/// - Phase 3 (write): pipeline all DEL + SREM + broadcast in one round-trip
async fn handle_invalidation(
    conn: &mut (impl redis::aio::ConnectionLike + Send),
    cache_name: &str,
    key_values: &[String],
    prefix: &str,
    ws_server: &str,
    centrifugo_stream: &str,
    reverb_config: Option<&ReverbConfig>,
    http_client: Option<&reqwest::Client>,
) -> Result<(), redis::RedisError> {
    let mut dependent_keys: Vec<String>;
    // Unprefixed twin keys to drop in the write pipeline (+ whether the
    // per-cache twin index set itself must be dropped — broad path only).
    let mut twin_dels: Vec<String>;
    let mut drop_twin_idx = false;

    if !key_values.is_empty() {
        // Parameterized query — pipeline all per-param dep lookups
        // (plus the delta-liveness telemetry, piggybacked on the same round trip)
        let mut pdep_keys: Vec<String> = Vec::new();
        let mut read_pipe = redis::pipe();
        // Granular delta — echo a sample emitted param_key (all emissions for
        // one cache share the reader's key-column arity, so any sample works).
        add_liveness_cmds(&mut read_pipe, prefix, cache_name, Some(&key_values[0]));
        for param_key in key_values {
            let pdep_key = format!("{}rs:pdeps:{}:{}", prefix, cache_name, param_key);
            read_pipe.cmd("SMEMBERS").arg(&pdep_key);
            pdep_keys.push(pdep_key);
        }
        let pdep_results: Vec<Vec<String>> = read_pipe
            .query_async(conn)
            .await
            .unwrap_or_else(|_| vec![Vec::new(); key_values.len()]);

        // Granular twin drop: the exact affected param twins + the `_` twin
        // (a non-parameterized composition over the same cache would be keyed
        // `_`; cheap DEL no-op when absent).
        twin_dels = key_values
            .iter()
            .map(|pk| twin_key(cache_name, pk))
            .collect();
        twin_dels.push(twin_key(cache_name, ""));

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
        // Non-parameterized (broad) invalidation — broad dep key + the twin
        // index SET, all on the same round trip as the liveness telemetry.
        let dep_key = format!("{}rs:deps:{}", prefix, cache_name);
        let idx_key = format!("{}{}", prefix, twin_idx_key(cache_name));
        let mut read_pipe = redis::pipe();
        // Broad delta — no param_key exists, so no identity echo is written.
        add_liveness_cmds(&mut read_pipe, prefix, cache_name, None);
        read_pipe.cmd("SMEMBERS").arg(&dep_key);
        read_pipe.cmd("SMEMBERS").arg(&idx_key);
        let idx_members: Vec<String>;
        (dependent_keys, idx_members) =
            match read_pipe.query_async::<(Vec<String>, Vec<String>)>(conn).await {
                Ok((keys, idx)) => (keys, idx),
                Err(e) => {
                    warn!(%e, %cache_name, "Tier 1: SMEMBERS failed");
                    return Ok(());
                }
            };

        // Broad twin drop: SCAN in the hot path is unacceptable, so PHP
        // maintains rs:hyd_idx:{cache_name} — drain it, plus the `_` twin
        // (self-healing if the index lapsed before the twin), plus the index
        // itself so a stale index never masks future broad drops.
        twin_dels = idx_members;
        let underscore = twin_key(cache_name, "");
        if !twin_dels.contains(&underscore) {
            twin_dels.push(underscore);
        }
        drop_twin_idx = true;
    }

    // --- Phase 3: read-set cascade -----------------------------------------
    // Every dropped twin may carry a `rs:hyd_deps:{cache}:{param_key}` SET of
    // composed-payload keys whose MISS composition read exactly that identity
    // (PHP registers them ONLY when the payload's whole read set was
    // twin-promoted — see RscGraphQLCache::registerHydDeps). Drain them in one
    // pipelined SMEMBERS round trip, merge the members into dependent_keys —
    // they then ride the EXISTING write pipeline (DEL key + :stale expansion,
    // rs:reval marker) and the EXISTING broadcast grouping (authHash
    // extraction) exactly like pdeps-resolved keys — and DEL the sets
    // themselves in the write pipeline (payloads re-register on their next
    // composition). Applies uniformly to granular twins, the `_` twin, and
    // broad idx-drain twins, and rides the same debounce batching.
    let hyd_deps_keys: Vec<String> = twin_dels
        .iter()
        .filter_map(|t| hyd_deps_key_for_twin(t))
        .collect();
    if !hyd_deps_keys.is_empty() {
        let mut cascade_pipe = redis::pipe();
        for k in &hyd_deps_keys {
            cascade_pipe.cmd("SMEMBERS").arg(format!("{}{}", prefix, k));
        }
        match cascade_pipe.query_async::<Vec<Vec<String>>>(conn).await {
            Ok(member_sets) => {
                let n: usize = member_sets.iter().map(|m| m.len()).sum();
                if n > 0 {
                    debug!(
                        cache_name,
                        payload_keys = n,
                        hyd_deps_sets = hyd_deps_keys.len(),
                        "Phase 3: read-set cascade — twin drop fans out to composed payloads"
                    );
                }
                for members in member_sets {
                    merge_unique(&mut dependent_keys, members);
                }
            }
            Err(e) => {
                warn!(%e, cache_name, "Phase 3: hyd_deps SMEMBERS failed — cascade skipped this round");
            }
        }
    }

    // Twin drops proceed even with zero registered payload deps — twins exist
    // independently of rs:gql/rs:lw registrations. (twin_dels is never empty:
    // the `_` twin is always included; DEL on absent keys is a no-op.)
    if dependent_keys.is_empty() && twin_dels.is_empty() {
        return Ok(());
    }

    debug!(
        cache_name,
        keys = dependent_keys.len(),
        twins = twin_dels.len(),
        granular = !key_values.is_empty(),
        "Tier 1: invalidating dependent keys + twins"
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
    let reval_ttl = reval_ttl_ms();

    if !dependent_keys.is_empty() {
        // DEL all affected RSC cache keys (with prefix), PLUS their
        // stale-while-revalidate twins ("{key}:stale", kept by PHP at ~3x TTL).
        // SWR exists to smooth TTL expiry, not to serve known-stale data: without
        // this, concurrent clients could be served pre-change data from :stale
        // for up to the extended TTL if the rebuild fails.
        let prefixed_keys = reverb_http::expand_keys_for_del(prefix, &dependent_keys);

        // Revalidation markers: the PHP MISS path checks rs:reval:{key} and, when
        // present, routes the rebuild's reads to the direct Postgres write
        // connection instead of ReadySet — closing the window where a refetch
        // races the dataflow's replication lag and re-caches pre-change data.
        // TTL = READYSET_REVAL_TTL_MS (default 3000ms): well above replication
        // lag (measured max 16.9ms), well below cache TTL.
        //
        // Phase 5B ORDER CONTRACT: markers are queued BEFORE the DELs so that
        // marker visibility >= deletion visibility for every concurrent
        // observer. Pipelined commands are not atomic — with DEL first, a PHP
        // reader could observe the deletion (GET nil / its post-store re-check)
        // while EXISTS rs:reval still returns 0, and rebuild-or-keep a
        // pre-delta value that nothing ever invalidates again.
        for k in &dependent_keys {
            add_reval_marker(&mut write_pipe, prefix, k, reval_ttl);
        }
        write_pipe.cmd("DEL").arg(&prefixed_keys).ignore();
    }

    // Hydrated twins: DEL each affected twin (no `:stale` companions — twins
    // have none) + the same rs:reval marker so a TwinStore refill inside
    // the replication-lag window reads direct Postgres and skips storing.
    // Twin keys are server-side only: intentionally NOT broadcast.
    if !twin_dels.is_empty() {
        let twin_prefixed: Vec<String> = twin_dels
            .iter()
            .map(|k| format!("{}{}", prefix, k))
            .collect();
        // Same Phase 5B order contract as above: marker before DEL.
        for k in &twin_dels {
            add_reval_marker(&mut write_pipe, prefix, k, reval_ttl);
        }
        write_pipe.cmd("DEL").arg(&twin_prefixed).ignore();
    }
    if drop_twin_idx {
        write_pipe.cmd("DEL")
            .arg(format!("{}{}", prefix, twin_idx_key(cache_name)))
            .ignore();
    }

    // Phase 3: DEL the drained hyd_deps sets — their member payloads were
    // just DELed above, and each payload re-registers its (possibly changed)
    // read set on its next MISS composition. A stale set surviving here would
    // re-DEL already-recomposed payloads on the next delta.
    if !hyd_deps_keys.is_empty() {
        let prefixed: Vec<String> = hyd_deps_keys
            .iter()
            .map(|k| format!("{}{}", prefix, k))
            .collect();
        write_pipe.cmd("DEL").arg(&prefixed).ignore();
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
            // Per-channel broadcast epochs: INCR rs:epoch:{authHash} once per
            // channel for this delivered batch, in its own small pipeline,
            // executed and AWAITED before the HTTP POST (ordering contract:
            // the counter must be visible in Redis before any client can
            // receive the event carrying it). Runs after the DEL pipeline, so
            // the Phase 5B marker-before-DEL order is untouched. Fail-open:
            // on INCR failure the events ship without the epoch field.
            let epochs = reverb_http::incr_epochs(conn, prefix, &grouped).await;
            reverb_http::broadcast_to_reverb(cfg, client, &grouped, &epochs).await;
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

#[cfg(test)]
mod tests {
    use readyset_data::DfValue;

    use super::*;

    /// Vendored copy of the cross-layer contract fixture (canonical source:
    /// packages/businesspress/core/tests/Fixtures/rsc-contract.json — byte
    /// sync is guarded by reverb_http.rs test_contract_fixture_in_sync).
    fn load_fixture() -> serde_json::Value {
        let raw = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../fixtures/rsc-contract.json"),
        )
        .expect("vendored fixtures/rsc-contract.json missing — re-vendor from PHP core");
        serde_json::from_str(&raw).expect("rsc-contract.json is not valid JSON")
    }

    /// Map a typed JSON fixture param onto the DfValue the dataflow would
    /// carry for it (PG bool replicates as 0/1, JSON null as DfValue::None).
    fn df_from_json(v: &serde_json::Value) -> DfValue {
        match v {
            serde_json::Value::Null => DfValue::None,
            serde_json::Value::Bool(b) => DfValue::from(*b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    DfValue::Int(i)
                } else {
                    DfValue::Double(n.as_f64().expect("numeric fixture param"))
                }
            }
            serde_json::Value::String(s) => DfValue::from(s.as_str()),
            other => panic!("unsupported twin_identities param type: {other:?}"),
        }
    }

    /// Contract: the param_key the Reader paths emit (DfValue Display joined
    /// with '|' in placeholder order — reader.rs extract_key_column_values and
    /// eviction_key_values both use `format!("{}", v)` + `join("|")`) must
    /// match the fixture byte-for-byte. PHP normalizes TO this encoding.
    #[test]
    fn twin_identities_param_encoding_matches_fixture() {
        let fixture = load_fixture();
        for row in fixture["twin_identities"].as_array().expect("twin_identities") {
            let params = row["params"].as_array().unwrap();
            let encoded: String = params
                .iter()
                .map(|p| format!("{}", df_from_json(p)))
                .collect::<Vec<_>>()
                .join("|");
            let expected = row["expected_param_key"].as_str().unwrap();
            assert_eq!(
                encoded, expected,
                "DfValue Display param encoding drifted for params {params:?}"
            );
        }
    }

    /// Contract: twin key construction (incl. the `_` empty-params form) and
    /// the per-cache index key.
    #[test]
    fn twin_identities_key_building_matches_fixture() {
        let fixture = load_fixture();
        for row in fixture["twin_identities"].as_array().expect("twin_identities") {
            let cache_name = row["cache_name"].as_str().unwrap();
            let param_key = row["expected_param_key"].as_str().unwrap();
            let expected_twin = row["expected_twin_key"].as_str().unwrap();
            assert_eq!(
                twin_key(cache_name, param_key),
                expected_twin,
                "twin_key drifted for cache {cache_name:?} param_key {param_key:?}"
            );
            if let Some(expected_idx) = row["expected_idx_key"].as_str() {
                assert_eq!(
                    twin_idx_key(cache_name),
                    expected_idx,
                    "twin_idx_key drifted for cache {cache_name:?}"
                );
            }

            // Phase 3: the read-set dependents SET key mirrors the twin key
            // (rs:hyd_deps:{cache}:{param_key}, `_` for empty params), both
            // when built from the identity AND when mapped from the twin key
            // (the broad idx-drain path only has twin keys in hand).
            if let Some(expected_hyd_deps) = row["expected_hyd_deps_key"].as_str() {
                assert_eq!(
                    hyd_deps_key(cache_name, param_key),
                    expected_hyd_deps,
                    "hyd_deps_key drifted for cache {cache_name:?} param_key {param_key:?}"
                );
                assert_eq!(
                    hyd_deps_key_for_twin(expected_twin).as_deref(),
                    Some(expected_hyd_deps),
                    "hyd_deps_key_for_twin drifted for twin {expected_twin:?}"
                );
            }
        }
    }

    #[test]
    fn twin_key_empty_param_uses_underscore_placeholder() {
        assert_eq!(twin_key("c", ""), "rs:hyd:c:_");
        assert_eq!(twin_key("c", "a|0"), "rs:hyd:c:a|0");
        assert_eq!(twin_idx_key("c"), "rs:hyd_idx:c");
        assert_eq!(hyd_deps_key("c", ""), "rs:hyd_deps:c:_");
        assert_eq!(hyd_deps_key("c", "a|0"), "rs:hyd_deps:c:a|0");
        assert_eq!(t1_echo_key("c"), "rs:t1_echo:c");
    }

    /// C1 identity-echo gate: granular deltas write the echo (a sample
    /// emitted param_key), broad deltas do not — the echo's ABSENCE is what
    /// keeps never-granular parameterized shapes unpromotable on the PHP side.
    #[test]
    fn liveness_cmds_echo_only_on_granular() {
        let mut granular = redis::pipe();
        add_liveness_cmds(&mut granular, "pfx-", "c1", Some("not_sent|0"));
        let packed = String::from_utf8_lossy(&granular.get_packed_pipeline()).into_owned();
        assert!(packed.contains("pfx-rs:t1_echo:c1"), "granular must write the echo key");
        assert!(packed.contains("not_sent|0"), "echo value must be the emitted param_key sample");
        assert!(packed.contains("pfx-rs:t1_live:c1"));

        let mut broad = redis::pipe();
        add_liveness_cmds(&mut broad, "pfx-", "c1", None);
        let packed = String::from_utf8_lossy(&broad.get_packed_pipeline()).into_owned();
        assert!(!packed.contains("rs:t1_echo"), "broad must NOT write an echo");
        assert!(packed.contains("pfx-rs:t1_live:c1"));
    }

    /// READYSET_REVAL_TTL_MS parsing: unset/garbage/zero fall back to the
    /// legacy 3000ms default; a set value is honored (deployment: 250).
    #[test]
    fn reval_ttl_ms_parses_env_with_3000_default() {
        assert_eq!(parse_reval_ttl_ms(None), 3000);
        assert_eq!(parse_reval_ttl_ms(Some("250")), 250);
        assert_eq!(parse_reval_ttl_ms(Some(" 250 ")), 250);
        assert_eq!(parse_reval_ttl_ms(Some("3000")), 3000);
        assert_eq!(parse_reval_ttl_ms(Some("")), 3000);
        assert_eq!(parse_reval_ttl_ms(Some("abc")), 3000);
        assert_eq!(parse_reval_ttl_ms(Some("-1")), 3000);
        // 0 would expire before any observer sees the marker — rejected
        assert_eq!(parse_reval_ttl_ms(Some("0")), 3000);
    }

    /// The marker command is `SET {prefix}rs:reval:{key} 1 PX {ttl_ms}` —
    /// PX millisecond expiry (SETEX cannot express sub-second TTLs), value
    /// and key name unchanged from the legacy SETEX shape.
    #[test]
    fn reval_marker_emits_set_px_with_configured_ttl() {
        let mut pipe = redis::pipe();
        add_reval_marker(&mut pipe, "pfx-", "rs:hyd:c1:a|0", parse_reval_ttl_ms(Some("250")));
        let packed = String::from_utf8_lossy(&pipe.get_packed_pipeline()).into_owned();
        assert!(packed.contains("SET"), "must use SET (not SETEX)");
        assert!(!packed.contains("SETEX"), "SETEX cannot express sub-second TTLs");
        assert!(packed.contains("pfx-rs:reval:rs:hyd:c1:a|0"), "marker key unchanged");
        assert!(packed.contains("PX"), "must use PX millisecond expiry");
        assert!(packed.contains("250"), "configured TTL must be emitted");
    }

    /// Phase 3: twin-key → hyd_deps-key mapping is prefix-anchored (param
    /// keys may contain `:` and `|` freely) and rejects non-twin input.
    #[test]
    fn hyd_deps_key_for_twin_maps_and_rejects() {
        assert_eq!(
            hyd_deps_key_for_twin("rs:hyd:gql_dashboard_abc:not_sent|0").as_deref(),
            Some("rs:hyd_deps:gql_dashboard_abc:not_sent|0")
        );
        // `_` (non-parameterized) form maps to the `_` deps set
        assert_eq!(
            hyd_deps_key_for_twin("rs:hyd:gql_dashboard_abc:_").as_deref(),
            Some("rs:hyd_deps:gql_dashboard_abc:_")
        );
        // colons inside the param segment pass through verbatim
        assert_eq!(
            hyd_deps_key_for_twin("rs:hyd:c:val:with:colons").as_deref(),
            Some("rs:hyd_deps:c:val:with:colons")
        );
        // non-twin keys are rejected (incl. the idx and deps namespaces)
        assert_eq!(hyd_deps_key_for_twin("rs:hyd_idx:c"), None);
        assert_eq!(hyd_deps_key_for_twin("rs:gql:dashboard:en:a:b"), None);
        assert_eq!(hyd_deps_key_for_twin("rs:hyd_deps:c:x"), None);
    }

    fn msg(cache: &str, kvs: &[&str]) -> InvalidationMsg {
        InvalidationMsg {
            cache_name: cache.to_string(),
            key_values: kvs.iter().map(|s| s.to_string()).collect(),
        }
    }

    /// Lifecycle regression (2026-08-11): the notifier receiver must NOT die
    /// with the runtime of whichever domain thread initialized it. Each
    /// domain runs a private current-thread runtime that is dropped on
    /// domain shutdown (cache removal); when the receive loop was
    /// tokio::spawn'ed from that context, dropping the runtime killed Tier-1
    /// process-wide while the static sender lived on. The loop now runs on
    /// its own dedicated thread, so the channel must still accept sends
    /// after the initializing runtime is gone.
    #[test]
    fn tier1_notifier_survives_initializing_runtime_drop() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build test runtime");
        // Initialize the notifier from inside a short-lived runtime, exactly
        // like the first reader delta arriving on a doomed domain thread.
        rt.block_on(async {
            notify_invalidation("lifecycle_probe_cache", vec![]);
        });
        drop(rt);
        std::thread::sleep(Duration::from_millis(100));
        assert!(
            get_or_init_tx().send(msg("lifecycle_probe_cache", &[])).is_ok(),
            "Tier 1 notifier receiver died with the initializing domain runtime"
        );
    }

    /// Phase 5A fix: a broad message (empty key_values) in a debounce window
    /// must make the merged entry for that cache_name broad — in EITHER
    /// arrival order. The pre-fix append-only merge demoted broad to granular,
    /// skipping the idx drain / broad deps for that window with no replay.
    #[test]
    fn debounce_merge_broad_absorbs_granular() {
        // granular then broad
        let merged = merge_debounce_batch(&[
            msg("c1", &["a|0"]),
            msg("c1", &[]),
        ]);
        assert_eq!(merged["c1"], Vec::<String>::new(), "broad after granular must stay broad");

        // broad then granular
        let merged = merge_debounce_batch(&[
            msg("c1", &[]),
            msg("c1", &["a|0", "b|1"]),
        ]);
        assert_eq!(merged["c1"], Vec::<String>::new(), "granular after broad must not resurrect granular");

        // granular sandwiched by broad, plus an unrelated granular cache
        let merged = merge_debounce_batch(&[
            msg("c1", &["a|0"]),
            msg("c2", &["x"]),
            msg("c1", &[]),
            msg("c1", &["b|1"]),
        ]);
        assert_eq!(merged["c1"], Vec::<String>::new());
        assert_eq!(merged["c2"], vec!["x".to_string()], "other caches keep their granular keys");
    }

    /// Granular-only windows keep the original dedup'd merge semantics.
    #[test]
    fn debounce_merge_granular_dedups_and_broad_only_stays_broad() {
        let merged = merge_debounce_batch(&[
            msg("c1", &["a|0"]),
            msg("c1", &["a|0", "b|1"]),
        ]);
        assert_eq!(merged["c1"], vec!["a|0".to_string(), "b|1".to_string()]);

        let merged = merge_debounce_batch(&[msg("c3", &[]), msg("c3", &[])]);
        assert_eq!(merged["c3"], Vec::<String>::new());
    }

    /// Phase 3: cascade members merge into dependent_keys without duplicates
    /// (a payload found via pdeps AND via a hyd_deps set must be DELed and
    /// broadcast exactly once).
    #[test]
    fn merge_unique_dedups_cascade_members() {
        let mut deps = vec![
            "rs:gql:dashboard:en:h1:q1".to_string(),
            "rs:gql:dashboard:en:h1:q2".to_string(),
        ];
        merge_unique(
            &mut deps,
            vec![
                "rs:gql:dashboard:en:h1:q2".to_string(), // dup with existing
                "rs:gql:dashboard:en:h2:q3".to_string(),
                "rs:gql:dashboard:en:h2:q3".to_string(), // dup within cascade
            ],
        );
        assert_eq!(
            deps,
            vec![
                "rs:gql:dashboard:en:h1:q1".to_string(),
                "rs:gql:dashboard:en:h1:q2".to_string(),
                "rs:gql:dashboard:en:h2:q3".to_string(),
            ]
        );
    }
}
