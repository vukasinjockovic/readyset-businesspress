//! Shared library for the Readyset MCP server.
//!
//! Exposes the MCP tool implementations and connection helpers so they can be
//! reused by both the standalone `readyset-mcp` binary (stdio transport) and
//! the Readyset adapter (embedded HTTP transport).

pub mod connection;
pub mod server;
