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

use hmac::{Hmac, Mac};
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

/// Whether a key segment looks like an RSC authHash: a 32-char hex md5
/// (PHP: md5("{userId}:{shopId}:{scope}")) or the literal "anon" for guests.
fn is_auth_hash_segment(segment: &str) -> bool {
    segment == "anon"
        || (segment.len() == 32 && segment.bytes().all(|b| b.is_ascii_hexdigit()))
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
        // Internal bookkeeping keys are never broadcast targets
        "deps" | "pdeps" | "row_deps" | "key_deps" | "key_pdeps" | "key_row_deps"
        | "tag" | "registered" | "lock" | "meta" | "cache_name" | "cache_names"
        | "cache_params" | "kill" | "listener" => None,
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

/// Broadcast invalidation events to Reverb via the Pusher batch_events HTTP API.
///
/// Groups events by authHash into a single batch request. Each authHash maps to
/// a private channel `private-rsc.{authHash}` with event name `rsc.invalidated`.
///
/// Fire-and-forget: logs errors but never blocks the invalidation pipeline.
pub async fn broadcast_to_reverb(
    config: &ReverbConfig,
    client: &reqwest::Client,
    grouped: &HashMap<String, Vec<String>>,
) {
    if grouped.is_empty() {
        return;
    }

    // Build batch payload — one event per authHash
    let mut batch = Vec::new();
    for (auth_hash, keys) in grouped {
        let channel = format!("private-rsc.{}", auth_hash);
        // data must be a JSON string (double-encoded per Pusher protocol)
        let data = serde_json::json!({ "keys": keys }).to_string();

        batch.push(serde_json::json!({
            "name": "rsc.invalidated",
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
