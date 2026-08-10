use anyhow::{Context, Result};
use readyset_mcp::connection::{ConnectionConfig, ReadysetConnection};
use readyset_mcp::server::ReadysetMcpServer;
use rmcp::ServiceExt;
use rmcp::transport::stdio;

#[tokio::main]
async fn main() -> Result<()> {
    // Log to stderr — stdout is the MCP transport
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::WARN.into()),
        )
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();

    let config = ConnectionConfig::from_env().context("failed to read connection configuration")?;

    let conn = ReadysetConnection::new(&config)
        .await
        .context("failed to create Readyset connection")?;

    tracing::info!(
        host = %config.host,
        port = %config.port,
        db_type = ?config.db_type,
        "starting Readyset MCP server"
    );

    let server = ReadysetMcpServer::new(conn);
    let service = server
        .serve(stdio())
        .await
        .context("failed to start MCP server")?;

    service.waiting().await?;

    Ok(())
}
