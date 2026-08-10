use std::sync::Arc;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::{tool, tool_router};

use crate::connection::ReadysetConnection;

/// MCP server that exposes Readyset operations as tools.
///
/// Each tool maps to a Readyset SQL command, executed over the MySQL/PostgreSQL
/// wire protocol.
#[derive(Clone)]
pub struct ReadysetMcpServer {
    conn: Arc<ReadysetConnection>,
}

#[tool_router(server_handler)]
impl ReadysetMcpServer {
    /// Show Readyset system status including replication state, snapshot
    /// progress, and overall health.
    #[tool(description = "Show Readyset system status including replication \
        state, snapshot progress, and overall health. Returns key-value pairs \
        with metrics like replication lag, snapshot status, and connection \
        counts.")]
    async fn readyset_status(&self) -> String {
        match self.conn.query("SHOW READYSET STATUS").await {
            Ok(rows) => format_rows(&rows),
            Err(e) => format!("Error: {e}"),
        }
    }

    /// List queries currently being proxied to the upstream database. Shows
    /// query ID, SQL text, whether Readyset supports caching it, and query
    /// count. Use this to identify which queries could benefit from caching.
    #[tool(description = "List queries currently proxied to the upstream \
        database. Shows query ID, SQL text, whether Readyset supports caching \
        it, and query count. Use this to find cache candidates.")]
    async fn show_proxied_queries(&self) -> String {
        match self.conn.query("SHOW PROXIED QUERIES").await {
            Ok(rows) => format_rows(&rows),
            Err(e) => format!("Error: {e}"),
        }
    }

    /// List all active caches in Readyset. Shows cache name, query ID,
    /// the cached query text, and whether it is a deep or shallow cache.
    #[tool(description = "List all active caches in Readyset. Shows cache \
        name, query ID, the cached SQL, and cache type (deep or shallow). \
        Use this to see what is currently cached.")]
    async fn show_caches(&self) -> String {
        match self.conn.query("SHOW CACHES").await {
            Ok(rows) => format_rows(&rows),
            Err(e) => format!("Error: {e}"),
        }
    }

    /// Show the Readyset version running on the connected instance.
    #[tool(description = "Show the Readyset server version, build profile, \
        and related build metadata.")]
    async fn readyset_version(&self) -> String {
        match self.conn.query("SHOW READYSET VERSION").await {
            Ok(rows) => format_rows(&rows),
            Err(e) => format!("Error: {e}"),
        }
    }

    /// List proxied queries that Readyset supports caching but has not yet
    /// cached. These are the strongest candidates for CREATE CACHE.
    #[tool(description = "List proxied queries that Readyset *could* cache \
        but currently isn't. These are the best candidates for CREATE CACHE: \
        the support is already verified.")]
    async fn show_proxied_supported(&self) -> String {
        match self.conn.query("SHOW PROXIED SUPPORTED QUERIES").await {
            Ok(rows) => format_rows(&rows),
            Err(e) => format!("Error: {e}"),
        }
    }

    /// Create a cache for a SQL query. Provide either a full SELECT query or
    /// a query_id from show_proxied_queries.
    #[tool(description = "Create a cache for a SQL query. Provide either a \
        full SELECT query OR a query_id (from show_proxied_queries / \
        show_proxied_supported). An optional name can be provided to refer to \
        the cache later. Returns an error if the query is not cacheable.")]
    async fn create_cache(&self, Parameters(params): Parameters<CreateCacheParams>) -> String {
        let from_clause = match (params.query.as_deref(), params.query_id.as_deref()) {
            (Some(q), None) => {
                let q = q.trim();
                if q.is_empty() {
                    return "Error: query is empty".to_string();
                }
                q.to_string()
            }
            (None, Some(id)) => {
                let id = id.trim();
                if !is_safe_identifier(id) {
                    return format!("Error: query_id '{id}' has invalid characters");
                }
                id.to_string()
            }
            (Some(_), Some(_)) => {
                return "Error: provide either query or query_id, not both".to_string();
            }
            (None, None) => {
                return "Error: must provide either query or query_id".to_string();
            }
        };

        let name_clause = match params.name.as_deref() {
            None => String::new(),
            Some(n) => {
                if !is_safe_identifier(n) {
                    return format!("Error: name '{n}' has invalid characters");
                }
                format!(" `{n}`")
            }
        };

        let sql = format!("CREATE CACHE{name_clause} FROM {from_clause}");
        match self.conn.exec(&sql).await {
            Ok(()) => format!("Created cache from {from_clause}"),
            Err(e) => format!("Error: {e}"),
        }
    }

    /// Check whether a SQL query can be cached by Readyset, and why or why not.
    #[tool(description = "Check whether a SQL query can be cached by \
        Readyset. Returns the query's migration state: 'yes' if cacheable, \
        'no: <reason>' if not, 'cached' if already cached, or 'pending'.")]
    async fn explain_cache_support(
        &self,
        Parameters(params): Parameters<ExplainCacheSupportParams>,
    ) -> String {
        let query = params.query.trim();
        if query.is_empty() {
            return "Error: query is empty".to_string();
        }
        let sql = format!("EXPLAIN CREATE CACHE FROM {query}");
        match self.conn.query(&sql).await {
            Ok(rows) => format_rows(&rows),
            Err(e) => format!("Error: {e}"),
        }
    }

    /// Drop an existing cache by name.
    #[tool(description = "Remove an existing cache by name. Use show_caches \
        to find cache names. Fails if the cache does not exist.")]
    async fn drop_cache(&self, Parameters(params): Parameters<DropCacheParams>) -> String {
        if !is_safe_identifier(&params.name) {
            return format!(
                "Error: cache name '{}' contains invalid characters. Only letters, digits, underscore, and period are allowed.",
                params.name
            );
        }
        let sql = format!("DROP CACHE `{}`", params.name);
        match self.conn.exec(&sql).await {
            Ok(()) => format!("Dropped cache {}", params.name),
            Err(e) => format!("Error: {e}"),
        }
    }
}

/// Parameters for the `drop_cache` tool.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct DropCacheParams {
    /// The name of the cache to drop (as reported by show_caches).
    pub name: String,
}

/// Parameters for the `explain_cache_support` tool.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ExplainCacheSupportParams {
    /// The SQL SELECT query to check for cache support.
    pub query: String,
}

/// Parameters for the `create_cache` tool. Exactly one of `query` or
/// `query_id` must be set.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct CreateCacheParams {
    /// The full SELECT statement to cache. Mutually exclusive with `query_id`.
    #[serde(default)]
    pub query: Option<String>,
    /// A query_id from show_proxied_queries. Mutually exclusive with `query`.
    #[serde(default)]
    pub query_id: Option<String>,
    /// Optional name to assign to the cache.
    #[serde(default)]
    pub name: Option<String>,
}

/// True if `name` contains only characters safe to splice into a SQL identifier
/// (letters, digits, underscore, period). Used to reject injection attempts
/// when the user-supplied cache name is interpolated into DROP CACHE.
fn is_safe_identifier(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
}

impl ReadysetMcpServer {
    pub fn new(conn: ReadysetConnection) -> Self {
        Self {
            conn: Arc::new(conn),
        }
    }

    /// Construct a server that shares an existing connection pool. Used by the
    /// embedded HTTP transport, where each MCP session gets its own handler
    /// but all handlers reuse the same pool.
    pub fn from_shared(conn: Arc<ReadysetConnection>) -> Self {
        Self { conn }
    }
}

/// Format query result rows into a readable string for the LLM.
///
/// Each row is formatted as a block of `column: value` lines, separated by
/// blank lines between rows. Returns "(no results)" for empty result sets.
fn format_rows(rows: &[Vec<(String, String)>]) -> String {
    if rows.is_empty() {
        return "(no results)".to_string();
    }

    rows.iter()
        .map(|row| {
            row.iter()
                .map(|(col, val)| format!("{col}: {val}"))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_identifier_accepts_normal_names() {
        assert!(is_safe_identifier("my_cache"));
        assert!(is_safe_identifier("schema.table"));
        assert!(is_safe_identifier("q_abc123"));
    }

    #[test]
    fn safe_identifier_rejects_injection_chars() {
        assert!(!is_safe_identifier(""));
        assert!(!is_safe_identifier("'; DROP TABLE users; --"));
        assert!(!is_safe_identifier("foo`bar"));
        assert!(!is_safe_identifier("foo bar"));
        assert!(!is_safe_identifier(&"a".repeat(129)));
    }
}
