use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use mysql_async::prelude::Queryable;
use mysql_async::{Opts, OptsBuilder, Pool, SslOpts};
use native_tls::{Certificate, TlsConnector};
use tokio_postgres::{Client, NoTls};

/// A single row from a SQL result set, represented as column name -> value pairs.
pub type Row = Vec<(String, String)>;

/// Which wire protocol the Readyset instance speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbType {
    Mysql,
    Postgres,
}

/// Whether to negotiate TLS when connecting to Readyset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsMode {
    /// Connect over plain TCP.
    Disable,
    /// Negotiate TLS. Verification is governed by the rest of [`ConnectionConfig`].
    Require,
}

/// A connection to a Readyset instance. Dispatches to either the MySQL or
/// PostgreSQL client based on the configured dialect, but exposes a single
/// `query()` API that returns string-keyed rows.
pub enum ReadysetConnection {
    Mysql(Pool),
    Postgres(Client),
}

impl ReadysetConnection {
    /// Create a new connection from the given configuration.
    ///
    /// MySQL: returns immediately with a lazy pool.
    /// PostgreSQL: performs the handshake synchronously and spawns the
    /// background I/O task.
    pub async fn new(config: &ConnectionConfig) -> Result<Self> {
        match config.db_type {
            DbType::Mysql => {
                let mut builder = OptsBuilder::default()
                    .ip_or_hostname(&config.host)
                    .tcp_port(config.port)
                    .user(Some(&config.user))
                    .pass(Some(&config.password))
                    .db_name(config.database.as_deref());
                if config.tls_mode == TlsMode::Require {
                    builder = builder.ssl_opts(build_mysql_ssl_opts(config)?);
                }
                let opts: Opts = builder.into();
                Ok(Self::Mysql(Pool::new(opts)))
            }
            DbType::Postgres => {
                // Build a connection string. tokio_postgres handles escaping.
                let mut conn_str = format!(
                    "host={} port={} user={} password={}",
                    config.host, config.port, config.user, config.password
                );
                if let Some(db) = &config.database {
                    conn_str.push_str(&format!(" dbname={db}"));
                }

                let client = match config.tls_mode {
                    TlsMode::Disable => {
                        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
                            .await
                            .context("failed to connect to PostgreSQL")?;
                        // The connection future must be polled on the runtime to drive the protocol.
                        tokio::spawn(async move {
                            if let Err(e) = connection.await {
                                tracing::error!(error = %e, "postgres connection task exited");
                            }
                        });
                        client
                    }
                    TlsMode::Require => {
                        // sslmode=require encrypts but skips host verification;
                        // sslmode=verify-full enforces full validation against the loaded roots.
                        if config.tls_disable_verification {
                            conn_str.push_str(" sslmode=require");
                        } else {
                            conn_str.push_str(" sslmode=verify-full");
                        }
                        let connector = build_tls_connector(
                            config.tls_root_cert.as_deref(),
                            config.tls_disable_verification,
                        )?;
                        let tls = postgres_native_tls::MakeTlsConnector::new(connector);
                        let (client, connection) = tokio_postgres::connect(&conn_str, tls)
                            .await
                            .context("failed to connect to PostgreSQL")?;
                        tokio::spawn(async move {
                            if let Err(e) = connection.await {
                                tracing::error!(error = %e, "postgres connection task exited");
                            }
                        });
                        client
                    }
                };
                Ok(Self::Postgres(client))
            }
        }
    }

    /// Execute a SQL command and return all rows as (column, value) string pairs.
    pub async fn query(&self, sql: &str) -> Result<Vec<Row>> {
        match self {
            Self::Mysql(pool) => {
                let mut conn = pool
                    .get_conn()
                    .await
                    .context("failed to get MySQL connection from pool")?;
                let rows: Vec<mysql_async::Row> = conn
                    .query(sql)
                    .await
                    .with_context(|| format!("failed to execute: {sql}"))?;
                rows.into_iter().map(mysql_row_to_strings).collect()
            }
            Self::Postgres(client) => {
                let rows = client
                    .simple_query(sql)
                    .await
                    .with_context(|| format!("failed to execute: {sql}"))?;
                Ok(postgres_simple_query_to_rows(rows))
            }
        }
    }

    /// Execute a SQL command that returns no rows (used by cache management tools).
    pub async fn exec(&self, sql: &str) -> Result<()> {
        match self {
            Self::Mysql(pool) => {
                let mut conn = pool
                    .get_conn()
                    .await
                    .context("failed to get MySQL connection from pool")?;
                conn.query_drop(sql)
                    .await
                    .with_context(|| format!("failed to execute: {sql}"))?;
                Ok(())
            }
            Self::Postgres(client) => {
                client
                    .simple_query(sql)
                    .await
                    .with_context(|| format!("failed to execute: {sql}"))?;
                Ok(())
            }
        }
    }
}

/// Configuration for connecting to a Readyset instance.
pub struct ConnectionConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: Option<String>,
    pub db_type: DbType,
    pub tls_mode: TlsMode,
    /// PEM file containing one or more root certificates to use when verifying
    /// the server's certificate. Ignored when `tls_disable_verification` is set.
    pub tls_root_cert: Option<PathBuf>,
    /// Skip server certificate verification entirely. Encryption is still
    /// negotiated but the server's identity is not checked.
    pub tls_disable_verification: bool,
}

impl ConnectionConfig {
    /// Read connection configuration from environment variables.
    ///
    /// Required: READYSET_HOST, READYSET_PORT, READYSET_USER, READYSET_PASSWORD
    /// Optional:
    ///   READYSET_DATABASE                 — default database
    ///   READYSET_DB_TYPE                  — "mysql" (default) or "postgres" / "postgresql"
    ///   READYSET_SSLMODE                  — "disable" (default) or "require"
    ///   READYSET_TLS_ROOT_CERT            — path to a PEM file with root certs
    ///   READYSET_TLS_DISABLE_VERIFICATION — "true" to skip server cert verification
    pub fn from_env() -> Result<Self> {
        let host = std::env::var("READYSET_HOST")
            .context("READYSET_HOST environment variable is required")?;
        let port = std::env::var("READYSET_PORT")
            .context("READYSET_PORT environment variable is required")?
            .parse::<u16>()
            .context("READYSET_PORT must be a valid port number")?;
        let user = std::env::var("READYSET_USER")
            .context("READYSET_USER environment variable is required")?;
        let password = std::env::var("READYSET_PASSWORD")
            .context("READYSET_PASSWORD environment variable is required")?;
        let database = std::env::var("READYSET_DATABASE").ok();
        let db_type = match std::env::var("READYSET_DB_TYPE")
            .unwrap_or_else(|_| "mysql".into())
            .to_ascii_lowercase()
            .as_str()
        {
            "mysql" => DbType::Mysql,
            "postgres" | "postgresql" => DbType::Postgres,
            other => anyhow::bail!(
                "READYSET_DB_TYPE must be 'mysql' or 'postgres'/'postgresql', got '{other}'"
            ),
        };
        let tls_mode = match std::env::var("READYSET_SSLMODE")
            .unwrap_or_else(|_| "disable".into())
            .to_ascii_lowercase()
            .as_str()
        {
            "disable" | "disabled" | "off" | "false" => TlsMode::Disable,
            "require" | "required" | "on" | "true" => TlsMode::Require,
            other => {
                anyhow::bail!("READYSET_SSLMODE must be 'disable' or 'require', got '{other}'")
            }
        };
        let tls_root_cert = std::env::var("READYSET_TLS_ROOT_CERT")
            .ok()
            .map(PathBuf::from);
        let tls_disable_verification = std::env::var("READYSET_TLS_DISABLE_VERIFICATION")
            .ok()
            .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false);

        Ok(Self {
            host,
            port,
            user,
            password,
            database,
            db_type,
            tls_mode,
            tls_root_cert,
            tls_disable_verification,
        })
    }
}

/// Build a `native_tls::TlsConnector` honoring the configured verification mode.
///
/// Precedence: `disable_verification` wins over `root_cert`. When neither is
/// set, the system root store is used.
fn build_tls_connector(
    root_cert: Option<&Path>,
    disable_verification: bool,
) -> Result<TlsConnector> {
    let mut builder = TlsConnector::builder();
    if disable_verification {
        builder.danger_accept_invalid_certs(true);
    } else if let Some(path) = root_cert {
        let bytes = std::fs::read(path)
            .with_context(|| format!("failed to read TLS root cert from {}", path.display()))?;
        let pems = pem::parse_many(&bytes)
            .with_context(|| format!("failed to parse PEM from {}", path.display()))?;
        for pem in pems {
            builder.add_root_certificate(
                Certificate::from_der(pem.contents()).context("invalid root certificate")?,
            );
        }
    }
    builder.build().context("failed to build TLS connector")
}

/// Build `SslOpts` for `mysql_async` mirroring the configured verification mode.
fn build_mysql_ssl_opts(config: &ConnectionConfig) -> Result<SslOpts> {
    let mut opts = SslOpts::default();
    if config.tls_disable_verification {
        opts = opts.with_danger_accept_invalid_certs(true);
    } else if let Some(path) = &config.tls_root_cert {
        let bytes = std::fs::read(path)
            .with_context(|| format!("failed to read TLS root cert from {}", path.display()))?;
        let pems = pem::parse_many(&bytes)
            .with_context(|| format!("failed to parse PEM from {}", path.display()))?;
        let certs = pems
            .iter()
            .map(pem::encode)
            .map(String::into_bytes)
            .map(Into::into)
            .collect();
        opts = opts.with_root_certs(certs);
    }
    Ok(opts)
}

/// Convert a mysql_async::Row into (column, value) string pairs.
fn mysql_row_to_strings(row: mysql_async::Row) -> Result<Row> {
    let columns: Vec<String> = row
        .columns_ref()
        .iter()
        .map(|c| c.name_str().to_string())
        .collect();

    let mut pairs = Vec::with_capacity(columns.len());
    for (i, col_name) in columns.into_iter().enumerate() {
        let value: Option<String> = row.get(i);
        pairs.push((col_name, value.unwrap_or_else(|| "NULL".to_string())));
    }
    Ok(pairs)
}

/// Convert the output of `Client::simple_query` into our Row type.
///
/// `simple_query` returns all values as text (no binary type decoding), which
/// works for every Readyset custom command since they return human-readable
/// output. It also lets us handle non-standard types without special cases.
fn postgres_simple_query_to_rows(messages: Vec<tokio_postgres::SimpleQueryMessage>) -> Vec<Row> {
    let mut out = Vec::new();
    let mut column_names: Vec<String> = Vec::new();
    for msg in messages {
        match msg {
            tokio_postgres::SimpleQueryMessage::RowDescription(cols) => {
                column_names = cols.iter().map(|c| c.name().to_string()).collect();
            }
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                let mut pairs = Vec::with_capacity(column_names.len());
                for (i, name) in column_names.iter().enumerate() {
                    let value = row.get(i).unwrap_or("NULL").to_string();
                    pairs.push((name.clone(), value));
                }
                out.push(pairs);
            }
            // CommandComplete, etc.
            _ => {}
        }
    }
    out
}
