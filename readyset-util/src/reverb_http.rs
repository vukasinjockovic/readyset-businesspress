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

/// Extract authHash from an RSC cache key.
///
/// Supports two key formats:
///   - GraphQL:  rs:gql:{schema}:{authHash}:{queryHash}  → authHash at parts[3]
///   - Livewire: rs:lw:{authHash}:{Component}_{name}:{paramHash} → authHash at parts[2]
///
/// Returns None for keys that don't match either format.
pub fn extract_auth_hash(key: &str) -> Option<String> {
    let parts: Vec<&str> = key.split(':').collect();
    if parts.len() < 3 || parts[0] != "rs" {
        return None;
    }
    match parts[1] {
        "gql" if parts.len() >= 5 => Some(parts[3].to_string()),
        "lw" if parts.len() >= 4 => Some(parts[2].to_string()),
        _ => None,
    }
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

    #[test]
    fn test_extract_auth_hash_gql() {
        assert_eq!(
            extract_auth_hash("rs:gql:dashboard:abc123:q1"),
            Some("abc123".to_string())
        );
    }

    #[test]
    fn test_extract_auth_hash_lw() {
        assert_eq!(
            extract_auth_hash("rs:lw:abc123:OrderTable_orders:def456"),
            Some("abc123".to_string())
        );
    }

    #[test]
    fn test_extract_auth_hash_unknown() {
        assert_eq!(extract_auth_hash("rs:unknown:foo:bar"), None);
        assert_eq!(extract_auth_hash("not_rs:gql:x:y:z"), None);
        assert_eq!(extract_auth_hash("rs:gql:short"), None);
    }

    #[test]
    fn test_group_by_auth_hash_mixed() {
        let keys = vec![
            "rs:gql:dashboard:user1:q1".to_string(),
            "rs:gql:dashboard:user1:q2".to_string(),
            "rs:lw:user1:OrderTable_orders:p1".to_string(),
            "rs:gql:dashboard:user2:q3".to_string(),
            "rs:lw:user2:StatsTable_stats:p2".to_string(),
            "rs:unknown:ignored".to_string(),
        ];
        let grouped = group_by_auth_hash(&keys);
        assert_eq!(grouped.len(), 2);
        assert_eq!(grouped["user1"].len(), 3); // 2 gql + 1 lw
        assert_eq!(grouped["user2"].len(), 2); // 1 gql + 1 lw
    }
}
