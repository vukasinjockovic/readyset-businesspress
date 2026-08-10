//! Tests that the MCP token SQL commands parse identically with both nom-sql
//! and sqlparser-rs, so they behave correctly regardless of which parser the
//! running Readyset instance prefers.

use readyset_sql::Dialect;
use readyset_sql::ast::{
    AlterMcpTokenStatement, CreateMcpTokenStatement, DropMcpTokenStatement, McpTokenExpiresChange,
    McpTokenScope, ShowStatement, SqlQuery,
};
use readyset_sql_parsing::{ParsingPreset, parse_query_with_config};

#[test]
fn create_mcp_token_basic() {
    let q = check_parse_mysql!("CREATE MCP TOKEN 'claude'");
    assert_eq!(
        q,
        SqlQuery::CreateMcpToken(CreateMcpTokenStatement {
            name: "claude".into(),
            scope: None,
            expires: None,
        })
    );
}

#[test]
fn create_mcp_token_with_scope() {
    let q = check_parse_mysql!("CREATE MCP TOKEN 'claude' WITH SCOPE read_only");
    assert_eq!(
        q,
        SqlQuery::CreateMcpToken(CreateMcpTokenStatement {
            name: "claude".into(),
            scope: Some(McpTokenScope::ReadOnly),
            expires: None,
        })
    );
}

#[test]
fn create_mcp_token_cache_admin_scope() {
    let q = check_parse_mysql!("CREATE MCP TOKEN 'claude' WITH SCOPE cache_admin");
    assert_eq!(
        q,
        SqlQuery::CreateMcpToken(CreateMcpTokenStatement {
            name: "claude".into(),
            scope: Some(McpTokenScope::CacheAdmin),
            expires: None,
        })
    );
}

#[test]
fn create_mcp_token_with_expires() {
    let q = check_parse_mysql!("CREATE MCP TOKEN 'claude' EXPIRES '2026-12-31T23:59:59Z'");
    assert_eq!(
        q,
        SqlQuery::CreateMcpToken(CreateMcpTokenStatement {
            name: "claude".into(),
            scope: None,
            expires: Some("2026-12-31T23:59:59Z".into()),
        })
    );
}

#[test]
fn create_mcp_token_scope_and_expires() {
    let q = check_parse_mysql!(
        "CREATE MCP TOKEN 'claude' WITH SCOPE full EXPIRES '2026-12-31T23:59:59Z'"
    );
    assert_eq!(
        q,
        SqlQuery::CreateMcpToken(CreateMcpTokenStatement {
            name: "claude".into(),
            scope: Some(McpTokenScope::Full),
            expires: Some("2026-12-31T23:59:59Z".into()),
        })
    );
}

#[test]
fn drop_mcp_token() {
    let q = check_parse_mysql!("DROP MCP TOKEN 'claude'");
    assert_eq!(
        q,
        SqlQuery::DropMcpToken(DropMcpTokenStatement {
            name: "claude".into(),
        })
    );
}

#[test]
fn show_mcp_tokens() {
    let q = check_parse_mysql!("SHOW MCP TOKENS");
    assert_eq!(q, SqlQuery::Show(ShowStatement::McpTokens));
}

#[test]
fn alter_mcp_token_set_expires() {
    let q = check_parse_mysql!("ALTER MCP TOKEN 'claude' SET EXPIRES '2026-12-31T23:59:59Z'");
    assert_eq!(
        q,
        SqlQuery::AlterMcpToken(AlterMcpTokenStatement {
            name: "claude".into(),
            expires: McpTokenExpiresChange::At("2026-12-31T23:59:59Z".into()),
        })
    );
}

#[test]
fn alter_mcp_token_never_expires() {
    let q = check_parse_mysql!("ALTER MCP TOKEN 'claude' SET NEVER EXPIRES");
    assert_eq!(
        q,
        SqlQuery::AlterMcpToken(AlterMcpTokenStatement {
            name: "claude".into(),
            expires: McpTokenExpiresChange::Never,
        })
    );
}

#[test]
fn postgresql_parses_too() {
    check_parse_postgres!("CREATE MCP TOKEN 'claude' WITH SCOPE read_only");
    check_parse_postgres!("DROP MCP TOKEN 'claude'");
    check_parse_postgres!("SHOW MCP TOKENS");
    check_parse_postgres!("ALTER MCP TOKEN 'claude' SET EXPIRES '2026-12-31T23:59:59Z'");
    check_parse_postgres!("ALTER MCP TOKEN 'claude' SET NEVER EXPIRES");
}
