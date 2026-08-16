//! Reverb (Pusher-compatible) HTTP broadcaster for RSC invalidation events.
//!
//! Implements the Pusher HTTP API batch_events endpoint with HMAC-SHA256 signing.
//! Used by both Tier 1 (query-level) and Tier 2 (row-level) invalidation notifiers
//! to broadcast cache invalidation events directly to Reverb, eliminating the need
//! for a PHP listener intermediary.
//!
//! Signing algorithm (verified against pusher-php-server and Laravel Reverb source):
//!   1. params = { auth_key, auth_timestamp, auth_version: "1.0" }
//!   2. body_md5 = md5(json_body)
//!   3. ksort(params + body_md5)
//!   4. string = "POST\n/apps/{app_id}/batch_events\n{sorted_query_string}"
//!   5. auth_signature = hmac_sha256(string, app_secret)

use std::collections::HashMap;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use tracing::{debug, warn};

type HmacSha256 = Hmac<Sha256>;

/// Configuration for connecting to a Reverb (Pusher-compatible) server.
pub struct ReverbConfig {
    /// Pusher app ID (e.g., "505168")
    pub app_id: String,
    /// Pusher app key for authentication
    pub app_key: String,
    /// Pusher app secret for HMAC signing
    pub app_secret: String,
    /// Reverb server hostname (default: "127.0.0.1")
    pub host: String,
    /// Reverb server port (default: 8896)
    pub port: u16,
}

impl ReverbConfig {
    /// Read Reverb configuration from environment variables.
    /// Returns None if required vars are missing (Reverb not configured).
    pub fn from_env() -> Option<Self> {
        let app_id = env::var("READYSET_REVERB_APP_ID").ok()?;
        let app_key = env::var("READYSET_REVERB_APP_KEY").ok()?;
        let app_secret = env::var("READYSET_REVERB_APP_SECRET").ok()?;
        let host = env::var("READYSET_REVERB_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        let port: u16 = env::var("READYSET_REVERB_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8896);

        Some(Self {
            app_id,
            app_key,
            app_secret,
            host,
            port,
        })
    }
}

/// Whether a key segment looks like an RSC authHash: a 32-char lowercase hex
/// md5 (PHP: md5("{userId}:{shopId}:{scope}")) or the literal "anon" for
/// guests. Lowercase-only, matching the PHP/JS parsers — PHP md5() output is
/// always lowercase, so an uppercase segment is never a producer hash.
fn is_auth_hash_segment(segment: &str) -> bool {
    segment == "anon"
        || (segment.len() == 32
            && segment.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')))
}

/// Extract authHash from an RSC cache key.
///
/// Supported key formats:
///   - GraphQL (current): rs:gql:{schema}:{locale}:{authHash}:{queryHash}
///   - GraphQL (legacy):  rs:gql:{schema}:{authHash}:{queryHash}
///   - Livewire:          rs:lw:{authHash}:{Component}_{name}:{paramHash}
///   - withDeps:          rs:{cacheName}:{paramHash} — no auth segment
///
/// For gql keys the authHash position depends on whether a locale segment is
/// present, so we find the first segment after the schema that *looks like*
/// an authHash (32-hex or "anon"), excluding the trailing queryHash — which
/// is also 32-hex, hence the exclusive upper bound.
///
/// withDeps keys carry no auth context: those broadcast on the shared "anon"
/// channel, which every RSC client (guest or authenticated) is authorized to
/// join — see Broadcast::channel('rsc.{authHash}') in core-channels.php and
/// the echo.js adapter's anon-channel subscription.
///
/// Returns None only for keys that are not RSC cache keys at all.
pub fn extract_auth_hash(key: &str) -> Option<String> {
    let parts: Vec<&str> = key.split(':').collect();
    if parts.len() < 3 || parts[0] != "rs" {
        return None;
    }
    match parts[1] {
        "gql" if parts.len() >= 5 => parts[3..parts.len() - 1]
            .iter()
            .find(|p| is_auth_hash_segment(p))
            .map(|p| (*p).to_string()),
        "gql" => None,
        "lw" if parts.len() >= 4 => Some(parts[2].to_string()),
        "lw" => None,
        // Internal bookkeeping keys are never broadcast targets. MUST stay in
        // sync with ReactiveSetCacheService::BOOKKEEPING_PREFIXES (PHP) and
        // rsc/auth-hash.js (JS) — locked by fixtures/rsc-contract.json.
        "deps" | "pdeps" | "row_deps" | "key_deps" | "key_pdeps" | "key_row_deps"
        | "cache_row_deps" | "tag" | "registered" | "reval" | "dep_refresh"
        | "lock" | "meta" | "cache_name" | "cache_names" | "cache_params"
        | "kill" | "listener" | "lw_throttle" | "t1_unsupported"
        // Predicate deps (INSERT-phantom narrowing): rs:pred_deps:{schema}.
        // {table}:{column}:{value} -> cache keys, rs:pred_cols:{schema}.{table}
        // -> advertised columns, plus the PHP-side reverse mappings.
        | "pred_deps" | "pred_cols" | "key_pred_deps" | "cache_pred_deps"
        // Twin/engine plumbing: hydration twins + reverse indexes, twin-mode
        // flags, Tier-1 liveness/admission registries, access-version
        // counters, engine WAL position, epoch counters, the app_settings
        // ingredient map, and daily metric counters.
        | "hyd" | "hyd_idx" | "hyd_deps" | "twin_on" | "twin_off" | "t1_live"
        | "t1_echo" | "t1_suspect" | "t1_seen" | "accessver" | "lsn" | "epoch"
        | "ing" | "metric" => None,
        // withDeps value keys (rs:{cacheName}:{paramHash}) — shared channel
        _ => Some("anon".to_string()),
    }
}

/// Expand dependent cache keys into the full list to DEL on invalidation:
/// for each key, the prefixed value key PLUS its stale-while-revalidate twin
/// `{key}:stale`.
///
/// The PHP layer keeps an SWR copy of every RSC value at `{key}:stale` with
/// ~3x the primary TTL. SWR exists to smooth TTL expiry, not to serve
/// known-stale data: if a data-change invalidation only DELs the value key,
/// concurrent clients can be served pre-change data from `:stale` for up to
/// the extended TTL when the rebuild fails. Shared by all three notifier
/// tiers (Tier 1 query-level, Tier 2 row-level, Tier 3 table-level).
pub fn expand_keys_for_del(prefix: &str, dependent_keys: &[String]) -> Vec<String> {
    let mut out = Vec::with_capacity(dependent_keys.len() * 2);
    for k in dependent_keys {
        out.push(format!("{}{}", prefix, k));
        out.push(format!("{}{}:stale", prefix, k));
    }
    out
}

/// Group dependent cache keys by authHash for per-user channel broadcasting.
///
/// Keys that don't match known formats (rs:gql:* or rs:lw:*) are silently skipped.
pub fn group_by_auth_hash(dependent_keys: &[String]) -> HashMap<String, Vec<String>> {
    let mut grouped: HashMap<String, Vec<String>> = HashMap::new();
    for key in dependent_keys {
        if let Some(auth_hash) = extract_auth_hash(key) {
            grouped.entry(auth_hash).or_default().push(key.clone());
        }
    }
    grouped
}

/// Wire channel name for an authHash: `private-rsc.{authHash}`.
///
/// Must match the PHP side exactly — RscKeyInvalidated::broadcastOn() returns
/// PrivateChannel("rsc.{authHash}") which Laravel prefixes to
/// "private-rsc.{authHash}", and Echo.private('rsc.{hash}') subscribes to the
/// same wire name. Locked by fixtures/rsc-contract.json expected_channel.
pub fn channel_for_auth_hash(auth_hash: &str) -> String {
    format!("private-rsc.{}", auth_hash)
}

/// Wire event name for RSC invalidation broadcasts. Locked by
/// fixtures/rsc-contract.json `invalidation_events[].expected_event_name`.
pub const EVENT_NAME: &str = "rsc.invalidated";

/// Unprefixed per-channel broadcast-epoch counter key: `rs:epoch:{suffix}`,
/// where suffix is the channel's authHash (the literal `anon` for the shared
/// channel). The counter is INCRed once per channel per DELIVERED event batch
/// (not per key) — it counts deliveries, and the client only ever compares
/// equality (epoch reconnect-skip groundwork). Locked by
/// fixtures/rsc-contract.json `invalidation_events[].expected_epoch_key`.
pub fn epoch_key(auth_hash: &str) -> String {
    format!("rs:epoch:{auth_hash}")
}

/// Sliding TTL for `rs:epoch:*` counters: 7 days, refreshed on every INCR.
/// Expiry resets the counter to 1 on the next INCR — safe, because clients
/// compare equality only (any change, including a reset, means "refetch").
pub const EPOCH_TTL_SECS: u64 = 7 * 86400;

/// Queue `INCR {prefix}rs:epoch:{auth_hash}` + sliding `EXPIRE` on `pipe`.
/// The INCR result is NOT ignored — callers read the post-INCR value out of
/// the pipeline reply and embed it as the event's `"epoch"` field. The EXPIRE
/// reply is ignored, so a pipeline built purely from this helper yields
/// exactly one integer per channel, in queue order.
pub fn add_epoch_incr(pipe: &mut redis::Pipeline, prefix: &str, auth_hash: &str) {
    let key = format!("{}{}", prefix, epoch_key(auth_hash));
    pipe.cmd("INCR").arg(&key);
    pipe.cmd("EXPIRE").arg(&key).arg(EPOCH_TTL_SECS).ignore();
}

/// INCR the per-channel epoch counters for every channel in `grouped`, in one
/// dedicated Redis pipeline, and return the post-INCR values by authHash.
///
/// ORDERING CONTRACT: this must be executed AND awaited BEFORE the HTTP
/// broadcast that carries the returned epochs — the counter must be visible
/// to any Redis reader before any WebSocket client can possibly receive the
/// event carrying that epoch value. All callers (Tier 1/2/3 notifiers and the
/// startup twin flush) call this after their DEL write pipeline completes and
/// immediately before `broadcast_to_reverb`.
///
/// Fail-open: on a Redis error the map is empty, the events ship WITHOUT the
/// `"epoch"` field (legacy payload shape), and clients must treat the missing
/// field as uncomparable — never as 0. Locked by the fixture's epoch-null case.
pub async fn incr_epochs(
    conn: &mut (impl redis::aio::ConnectionLike + Send),
    prefix: &str,
    grouped: &HashMap<String, Vec<String>>,
) -> HashMap<String, u64> {
    if grouped.is_empty() {
        return HashMap::new();
    }
    // Snapshot iteration order so replies pair with the right channel.
    let hashes: Vec<&String> = grouped.keys().collect();
    let mut pipe = redis::pipe();
    for h in &hashes {
        add_epoch_incr(&mut pipe, prefix, h);
    }
    match pipe.query_async::<Vec<i64>>(conn).await {
        Ok(counts) => hashes
            .into_iter()
            .zip(counts)
            .map(|(h, c)| (h.clone(), c.max(0) as u64))
            .collect(),
        Err(e) => {
            warn!(%e, "rs:epoch INCR pipeline failed — events ship without epoch (fail-open)");
            HashMap::new()
        }
    }
}

/// Inner (to-be-double-encoded) event data JSON for an `rsc.invalidated`
/// event: `{"epoch":N,"keys":[...]}` — or `{"keys":[...]}` when no epoch is
/// available (INCR failure fail-open). Field order is ALPHABETICAL (serde_json
/// without preserve_order uses a BTreeMap), so `epoch` precedes `keys`; byte
/// shape locked by fixtures/rsc-contract.json `invalidation_events`.
pub fn event_data_json(keys: &[String], epoch: Option<u64>) -> String {
    let mut obj = serde_json::Map::new();
    if let Some(e) = epoch {
        obj.insert("epoch".to_string(), serde_json::Value::from(e));
    }
    obj.insert("keys".to_string(), serde_json::json!(keys));
    serde_json::Value::Object(obj).to_string()
}

/// Broadcast invalidation events to Reverb via the Pusher batch_events HTTP API.
///
/// Groups events by authHash into a single batch request. Each authHash maps to
/// a private channel `private-rsc.{authHash}` with event name `rsc.invalidated`.
///
/// `epochs` carries the post-INCR per-channel counter values from
/// `incr_epochs` (which the caller MUST have executed and awaited before
/// calling this — see the ordering contract there). A channel missing from
/// the map ships the legacy payload without the `"epoch"` field.
///
/// Fire-and-forget: logs errors but never blocks the invalidation pipeline.
pub async fn broadcast_to_reverb(
    config: &ReverbConfig,
    client: &reqwest::Client,
    grouped: &HashMap<String, Vec<String>>,
    epochs: &HashMap<String, u64>,
) {
    if grouped.is_empty() {
        return;
    }

    // Build batch payload — one event per authHash
    let mut batch = Vec::new();
    for (auth_hash, keys) in grouped {
        let channel = channel_for_auth_hash(auth_hash);
        // data must be a JSON string (double-encoded per Pusher protocol)
        let data = event_data_json(keys, epochs.get(auth_hash).copied());

        batch.push(serde_json::json!({
            "name": EVENT_NAME,
            "channel": channel,
            "data": data,
        }));
    }

    let body = serde_json::json!({ "batch": batch }).to_string();
    let path = format!("/apps/{}/batch_events", config.app_id);

    // Pusher signing
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .to_string();

    let body_md5 = format!("{:x}", md5::compute(&body));

    // Build sorted query params (auth_key, auth_timestamp, auth_version, body_md5)
    let mut params = vec![
        ("auth_key", config.app_key.as_str()),
        ("auth_timestamp", timestamp.as_str()),
        ("auth_version", "1.0"),
        ("body_md5", body_md5.as_str()),
    ];
    params.sort_by_key(|&(k, _)| k);

    let query_string: String = params
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join("&");

    // HMAC-SHA256 signature
    let string_to_sign = format!("POST\n{}\n{}", path, query_string);
    let signature = match HmacSha256::new_from_slice(config.app_secret.as_bytes()) {
        Ok(mut mac) => {
            mac.update(string_to_sign.as_bytes());
            hex::encode(mac.finalize().into_bytes())
        }
        Err(e) => {
            warn!(%e, "Reverb: HMAC key error");
            return;
        }
    };

    let url = format!(
        "http://{}:{}{}?{}&auth_signature={}",
        config.host, config.port, path, query_string, signature
    );

    debug!(
        channels = grouped.len(),
        total_keys = grouped.values().map(|v| v.len()).sum::<usize>(),
        "Reverb: broadcasting batch"
    );

    match client.post(&url)
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await
    {
        Ok(resp) => {
            let status = resp.status();
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                warn!(%status, %body, "Reverb: batch_events returned non-2xx");
            }
        }
        Err(e) => {
            warn!(%e, "Reverb: HTTP request failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HASH_A: &str = "0c2259fa3fd90640b5b2a10e8075544e";
    const HASH_B: &str = "a3f1c2d4e5b6978811223344556677aa";
    const QHASH: &str = "54dd078ed77c1e68b6129838a3a398fc";

    #[test]
    fn test_extract_auth_hash_gql_legacy_no_locale() {
        // rs:gql:{schema}:{authHash}:{queryHash}
        assert_eq!(
            extract_auth_hash(&format!("rs:gql:dashboard:{HASH_A}:{QHASH}")),
            Some(HASH_A.to_string())
        );
    }

    #[test]
    fn test_extract_auth_hash_gql_with_locale() {
        // rs:gql:{schema}:{locale}:{authHash}:{queryHash} — current core format
        assert_eq!(
            extract_auth_hash(&format!("rs:gql:dashboard:en:{HASH_A}:{QHASH}")),
            Some(HASH_A.to_string())
        );
        // longer locale tags must not be mistaken for the hash either
        assert_eq!(
            extract_auth_hash(&format!("rs:gql:storefront:en-US:{HASH_A}:{QHASH}")),
            Some(HASH_A.to_string())
        );
    }

    #[test]
    fn test_extract_auth_hash_gql_anon() {
        assert_eq!(
            extract_auth_hash(&format!("rs:gql:storefront:en:anon:{QHASH}")),
            Some("anon".to_string())
        );
        assert_eq!(
            extract_auth_hash(&format!("rs:gql:storefront:anon:{QHASH}")),
            Some("anon".to_string())
        );
    }

    #[test]
    fn test_extract_auth_hash_lw() {
        assert_eq!(
            extract_auth_hash(&format!("rs:lw:{HASH_A}:OrderTable_orders:def456")),
            Some(HASH_A.to_string())
        );
    }

    #[test]
    fn test_extract_auth_hash_unknown() {
        assert_eq!(extract_auth_hash("not_rs:gql:x:y:z"), None);
        assert_eq!(extract_auth_hash("rs:gql:short"), None);
        // gql key whose middle segments contain no hash-shaped part
        assert_eq!(extract_auth_hash(&format!("rs:gql:dashboard:en:{QHASH}")), None);
    }

    #[test]
    fn test_extract_auth_hash_withdeps_anon_fallback() {
        // withDeps value keys have no auth segment — shared "anon" channel
        assert_eq!(
            extract_auth_hash("rs:demo:top-products:40cd750bba9870f18aada2478b24840a"),
            Some("anon".to_string())
        );
        // Internal bookkeeping keys must never become broadcast targets
        assert_eq!(extract_auth_hash("rs:deps:demo_top_products_mv"), None);
        assert_eq!(extract_auth_hash("rs:row_deps:public.bp_orders:abc"), None);
        assert_eq!(extract_auth_hash("rs:tag:user:some-uuid"), None);
        assert_eq!(extract_auth_hash("rs:registered:gql_dashboard_x"), None);
    }

    /// Predicate-deps bookkeeping keys (RSC "pred deps": PHP registers the FK
    /// predicates each cached envelope scanned, plus the per-table advertised
    /// column list the replicator polls) are internal mappings, never
    /// broadcast targets — without these arms the `_` fallback would classify
    /// them as anon-broadcastable value keys, and `rs:key_pred_deps:*` even
    /// embeds a full gql key that the `gql` arm would happily mine a hash from.
    ///
    /// The sample keys are copied verbatim from the canonical PHP contract
    /// fixture's `keys` rows (all four carry `expected_auth_hash: null`), so
    /// this assertion holds independently of when the fixture is re-vendored.
    #[test]
    fn test_extract_auth_hash_pred_deps_are_bookkeeping() {
        assert_eq!(
            extract_auth_hash(
                "rs:pred_deps:public.bp_term_relationships:subject_id:0195aaaa-0000-7000-8000-000000000001"
            ),
            None
        );
        assert_eq!(
            extract_auth_hash("rs:pred_cols:public.bp_term_relationships"),
            None
        );
        assert_eq!(
            extract_auth_hash("rs:key_pred_deps:rs:gql:dashboard:abc:v0:def"),
            None
        );
        assert_eq!(
            extract_auth_hash("rs:cache_pred_deps:rs:gql:dashboard:abc:v0:def"),
            None
        );
    }

    #[test]
    fn test_expand_keys_for_del_adds_stale_twins() {
        let keys = vec![
            format!("rs:gql:dashboard:en:{HASH_A}:{QHASH}"),
            "rs:lw:anon:Comp_x:p1".to_string(),
        ];
        assert_eq!(
            expand_keys_for_del("gz:", &keys),
            vec![
                format!("gz:rs:gql:dashboard:en:{HASH_A}:{QHASH}"),
                format!("gz:rs:gql:dashboard:en:{HASH_A}:{QHASH}:stale"),
                "gz:rs:lw:anon:Comp_x:p1".to_string(),
                "gz:rs:lw:anon:Comp_x:p1:stale".to_string(),
            ]
        );
    }

    #[test]
    fn test_expand_keys_for_del_empty_prefix_and_keys() {
        let keys = vec!["rs:demo:k:h".to_string()];
        assert_eq!(
            expand_keys_for_del("", &keys),
            vec!["rs:demo:k:h".to_string(), "rs:demo:k:h:stale".to_string()]
        );
        assert!(expand_keys_for_del("gz:", &[]).is_empty());
    }

    /// Path to the vendored copy of the cross-layer contract fixture.
    /// Canonical source lives in the PHP core package:
    ///   packages/businesspress/core/tests/Fixtures/rsc-contract.json
    fn vendored_fixture_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../fixtures/rsc-contract.json")
    }

    fn load_fixture() -> serde_json::Value {
        let raw = std::fs::read_to_string(vendored_fixture_path())
            .expect("vendored fixtures/rsc-contract.json missing — re-vendor from PHP core");
        serde_json::from_str(&raw).expect("rsc-contract.json is not valid JSON")
    }

    /// Contract: identity formula. The Rust bridge never *computes* authHashes
    /// (PHP is the producer), but the fixture's expected hashes must be exactly
    /// what md5("{user}:{shop|none}:{scope|customer}") yields, and every
    /// producible hash must be recognized by the shape-based parser.
    #[test]
    fn test_contract_hash_inputs() {
        let fixture = load_fixture();
        for row in fixture["hash_inputs"].as_array().expect("hash_inputs") {
            let expected = row["expected_auth_hash"].as_str().unwrap();
            match row["user_id"].as_str() {
                None => assert_eq!(expected, "anon", "anonymous must be literal 'anon'"),
                Some(user_id) => {
                    let shop = row["shop_id"].as_str().unwrap_or("none");
                    let scope = row["scope"].as_str().unwrap_or("customer");
                    let input = format!("{}:{}:{}", user_id, shop, scope);
                    let computed = format!("{:x}", md5::compute(input.as_bytes()));
                    assert_eq!(
                        computed, expected,
                        "fixture hash mismatch for input '{input}'"
                    );
                }
            }
            // Round-trip: any producer hash must parse back out of a gql key
            let key = format!("rs:gql:dashboard:en:{expected}:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
            assert_eq!(
                extract_auth_hash(&key),
                Some(expected.to_string()),
                "producer hash not recognized by extract_auth_hash: {expected}"
            );
        }
    }

    /// Contract: key-shape parsing + channel naming for every fixture key.
    #[test]
    fn test_contract_keys() {
        let fixture = load_fixture();
        for row in fixture["keys"].as_array().expect("keys") {
            let key = row["key"].as_str().unwrap();
            let expected_hash = row["expected_auth_hash"].as_str().map(str::to_string);
            let got = extract_auth_hash(key);
            assert_eq!(got, expected_hash, "extract_auth_hash drifted for key: {key:?}");

            let expected_channel = row["expected_channel"].as_str().map(str::to_string);
            let channel = got.as_deref().map(channel_for_auth_hash);
            assert_eq!(channel, expected_channel, "channel naming drifted for key: {key:?}");
        }
    }

    /// Contract: DEL expansion — value key plus its `:stale` SWR twin.
    #[test]
    fn test_contract_del_expansion() {
        let fixture = load_fixture();
        for row in fixture["del_expansion"].as_array().expect("del_expansion") {
            let key = row["key"].as_str().unwrap().to_string();
            let expected: Vec<String> = row["expected"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap().to_string())
                .collect();
            assert_eq!(
                expand_keys_for_del("", &[key.clone()]),
                expected,
                "expand_keys_for_del drifted for key: {key:?}"
            );
            // Prefixing must distribute over both twins
            let prefixed = expand_keys_for_del("gz:", &[key.clone()]);
            let want: Vec<String> = expected.iter().map(|k| format!("gz:{k}")).collect();
            assert_eq!(prefixed, want, "prefixed expansion drifted for key: {key:?}");
        }
    }

    /// Sync guard: the vendored fixture must be byte-identical to the
    /// canonical one in the PHP core package. Skips silently when the
    /// canonical path does not exist (CI checkout without the PHP repo).
    /// Override the canonical location with RSC_CONTRACT_CANONICAL.
    #[test]
    fn test_contract_fixture_in_sync() {
        let canonical = std::env::var("RSC_CONTRACT_CANONICAL").unwrap_or_else(|_| {
            "/var/www/reactive-1.businesspress.dev/packages/businesspress/core/tests/Fixtures/rsc-contract.json"
                .to_string()
        });
        let canonical = std::path::Path::new(&canonical);
        if !canonical.exists() {
            return; // canonical repo not present on this machine — skip
        }
        let canonical_bytes = std::fs::read(canonical).expect("read canonical fixture");
        let vendored_bytes = std::fs::read(vendored_fixture_path()).expect("read vendored fixture");
        assert_eq!(
            canonical_bytes, vendored_bytes,
            "fixtures/rsc-contract.json drifted from the canonical PHP copy — \
             re-vendor: cp {} {}",
            canonical.display(),
            vendored_fixture_path().display()
        );
    }

    /// Contract: `rsc.invalidated` event wire shape — event name, channel,
    /// epoch counter key and the exact (inner, to-be-double-encoded) data
    /// bytes, including the epoch-null fail-open case where the field is
    /// absent entirely.
    #[test]
    fn test_contract_invalidation_events() {
        let fixture = load_fixture();
        for row in fixture["invalidation_events"]
            .as_array()
            .expect("invalidation_events")
        {
            let auth_hash = row["auth_hash"].as_str().unwrap();
            let keys: Vec<String> = row["keys"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap().to_string())
                .collect();
            let epoch = row["epoch"].as_u64(); // null -> None (fail-open case)

            assert_eq!(
                EVENT_NAME,
                row["expected_event_name"].as_str().unwrap(),
                "event name drifted"
            );
            assert_eq!(
                channel_for_auth_hash(auth_hash),
                row["expected_channel"].as_str().unwrap(),
                "channel naming drifted for authHash {auth_hash:?}"
            );
            assert_eq!(
                epoch_key(auth_hash),
                row["expected_epoch_key"].as_str().unwrap(),
                "epoch key drifted for authHash {auth_hash:?}"
            );
            assert_eq!(
                event_data_json(&keys, epoch),
                row["expected_data"].as_str().unwrap(),
                "event data bytes drifted for authHash {auth_hash:?} (epoch {epoch:?})"
            );
        }
    }

    /// The epoch pipeline helper: INCR (result kept) then sliding EXPIRE
    /// (result ignored) on the SAME prefixed key, INCR queued first — the
    /// caller pairs one integer reply per channel in queue order.
    #[test]
    fn test_add_epoch_incr_pipeline_shape() {
        let mut pipe = redis::pipe();
        add_epoch_incr(&mut pipe, "pfx-", HASH_A);
        add_epoch_incr(&mut pipe, "pfx-", "anon");
        let packed = String::from_utf8_lossy(&pipe.get_packed_pipeline()).into_owned();
        assert!(packed.contains(&format!("pfx-rs:epoch:{HASH_A}")));
        assert!(packed.contains("pfx-rs:epoch:anon"));
        assert!(packed.contains("INCR"));
        assert!(packed.contains("EXPIRE"));
        assert!(
            packed.find("INCR").unwrap() < packed.find("EXPIRE").unwrap(),
            "INCR must be queued before its sliding EXPIRE"
        );
        assert!(
            packed.contains(&EPOCH_TTL_SECS.to_string()),
            "7d sliding TTL must be emitted"
        );
    }

    /// event_data_json: epoch present -> alphabetical field order (epoch
    /// before keys); epoch absent -> legacy `{"keys":[...]}` byte shape.
    #[test]
    fn test_event_data_json_shapes() {
        let keys = vec!["rs:lw:anon:C_x:p1".to_string()];
        assert_eq!(
            event_data_json(&keys, Some(3)),
            r#"{"epoch":3,"keys":["rs:lw:anon:C_x:p1"]}"#
        );
        assert_eq!(event_data_json(&keys, None), r#"{"keys":["rs:lw:anon:C_x:p1"]}"#);
        assert_eq!(event_data_json(&[], Some(1)), r#"{"epoch":1,"keys":[]}"#);
    }

    #[test]
    fn test_group_by_auth_hash_mixed() {
        let keys = vec![
            format!("rs:gql:dashboard:en:{HASH_A}:q1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            format!("rs:gql:dashboard:{HASH_A}:q2bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            format!("rs:lw:{HASH_A}:OrderTable_orders:p1"),
            format!("rs:gql:dashboard:sr:{HASH_B}:q3cccccccccccccccccccccccccccccc"),
            format!("rs:lw:{HASH_B}:StatsTable_stats:p2"),
            "rs:demo:top-products:40cd750bba9870f18aada2478b24840a".to_string(),
            "rs:deps:demo_top_products_mv".to_string(), // bookkeeping — skipped
        ];
        let grouped = group_by_auth_hash(&keys);
        assert_eq!(grouped.len(), 3);
        assert_eq!(grouped[HASH_A].len(), 3); // 2 gql (both formats) + 1 lw
        assert_eq!(grouped[HASH_B].len(), 2); // 1 gql + 1 lw
        assert_eq!(grouped["anon"].len(), 1); // withDeps key → shared channel
    }
}
