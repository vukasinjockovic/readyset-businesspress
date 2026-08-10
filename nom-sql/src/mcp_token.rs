use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::{map, opt, value};
use nom::sequence::{preceded, tuple};
use nom_locate::LocatedSpan;
use readyset_sql::ast::{
    AlterMcpTokenStatement, CreateMcpTokenStatement, DropMcpTokenStatement, McpTokenExpiresChange,
    McpTokenScope,
};
use readyset_sql::Dialect;

use crate::dialect::DialectParser;
use crate::whitespace::whitespace1;
use crate::NomSqlResult;

/// Parse the scope identifier: read_only | cache_admin | full
fn scope_keyword(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], McpTokenScope> {
    alt((
        value(McpTokenScope::ReadOnly, tag_no_case("read_only")),
        value(McpTokenScope::CacheAdmin, tag_no_case("cache_admin")),
        value(McpTokenScope::Full, tag_no_case("full")),
    ))(i)
}

/// Parse: `CREATE MCP TOKEN '<name>' [WITH SCOPE <scope>] [EXPIRES '<datetime>']`
pub fn create_mcp_token(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], CreateMcpTokenStatement> {
    move |i| {
        let (i, _) = tag_no_case("create")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("mcp")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("token")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, name_bytes) = dialect.string_literal()(i)?;
        let name = String::from_utf8(name_bytes).unwrap_or_default();

        let (i, scope) = opt(preceded(
            tuple((
                whitespace1,
                tag_no_case("with"),
                whitespace1,
                tag_no_case("scope"),
                whitespace1,
            )),
            scope_keyword,
        ))(i)?;

        let (i, expires_bytes) = opt(preceded(
            tuple((whitespace1, tag_no_case("expires"), whitespace1)),
            dialect.string_literal(),
        ))(i)?;
        let expires = expires_bytes.and_then(|b| String::from_utf8(b).ok());

        Ok((
            i,
            CreateMcpTokenStatement {
                name,
                scope,
                expires,
            },
        ))
    }
}

/// Parse: `DROP MCP TOKEN '<name>'`
pub fn drop_mcp_token(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], DropMcpTokenStatement> {
    move |i| {
        let (i, _) = tag_no_case("drop")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("mcp")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("token")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, name_bytes) = dialect.string_literal()(i)?;
        let name = String::from_utf8(name_bytes).unwrap_or_default();

        Ok((i, DropMcpTokenStatement { name }))
    }
}

/// Parse: `MCP TOKENS` (follows the SHOW keyword).
pub fn mcp_tokens_show(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], ()> {
    map(
        tuple((tag_no_case("mcp"), whitespace1, tag_no_case("tokens"))),
        |_| (),
    )(i)
}

/// Parse: `ALTER MCP TOKEN '<name>' SET (EXPIRES '<datetime>' | NEVER EXPIRES)`.
pub fn alter_mcp_token(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], AlterMcpTokenStatement> {
    move |i| {
        let (i, _) = tag_no_case("alter")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("mcp")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("token")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, name_bytes) = dialect.string_literal()(i)?;
        let name = String::from_utf8(name_bytes).unwrap_or_default();
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("set")(i)?;
        let (i, _) = whitespace1(i)?;

        let (i, expires) = alt((
            // NEVER EXPIRES
            value(
                McpTokenExpiresChange::Never,
                tuple((tag_no_case("never"), whitespace1, tag_no_case("expires"))),
            ),
            // EXPIRES '<datetime>'
            map(
                preceded(
                    tuple((tag_no_case("expires"), whitespace1)),
                    dialect.string_literal(),
                ),
                |bytes| McpTokenExpiresChange::At(String::from_utf8(bytes).unwrap_or_default()),
            ),
        ))(i)?;

        Ok((i, AlterMcpTokenStatement { name, expires }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_mcp_token_basic() {
        let query = b"CREATE MCP TOKEN 'my-token'";
        let (_, stmt) = create_mcp_token(Dialect::MySQL)(LocatedSpan::new(query)).unwrap();
        assert_eq!(stmt.name, "my-token");
        assert_eq!(stmt.scope, None);
    }

    #[test]
    fn test_create_mcp_token_with_scope() {
        let query = b"CREATE MCP TOKEN 'my-token' WITH SCOPE read_only";
        let (_, stmt) = create_mcp_token(Dialect::MySQL)(LocatedSpan::new(query)).unwrap();
        assert_eq!(stmt.name, "my-token");
        assert_eq!(stmt.scope, Some(McpTokenScope::ReadOnly));
    }

    #[test]
    fn test_create_mcp_token_cache_admin() {
        let query = b"CREATE MCP TOKEN 'claude' WITH SCOPE cache_admin";
        let (_, stmt) = create_mcp_token(Dialect::MySQL)(LocatedSpan::new(query)).unwrap();
        assert_eq!(stmt.scope, Some(McpTokenScope::CacheAdmin));
    }

    #[test]
    fn test_create_mcp_token_with_expires() {
        let query = b"CREATE MCP TOKEN 'claude' EXPIRES '2026-12-31T23:59:59Z'";
        let (_, stmt) = create_mcp_token(Dialect::MySQL)(LocatedSpan::new(query)).unwrap();
        assert_eq!(stmt.expires, Some("2026-12-31T23:59:59Z".to_string()));
        assert_eq!(stmt.scope, None);
    }

    #[test]
    fn test_create_mcp_token_scope_and_expires() {
        let query = b"CREATE MCP TOKEN 'claude' WITH SCOPE full EXPIRES '2026-12-31T23:59:59Z'";
        let (_, stmt) = create_mcp_token(Dialect::MySQL)(LocatedSpan::new(query)).unwrap();
        assert_eq!(stmt.scope, Some(McpTokenScope::Full));
        assert_eq!(stmt.expires, Some("2026-12-31T23:59:59Z".to_string()));
    }

    #[test]
    fn test_drop_mcp_token() {
        let query = b"DROP MCP TOKEN 'my-token'";
        let (_, stmt) = drop_mcp_token(Dialect::MySQL)(LocatedSpan::new(query)).unwrap();
        assert_eq!(stmt.name, "my-token");
    }

    #[test]
    fn test_alter_mcp_token_set_expires() {
        let query = b"ALTER MCP TOKEN 'claude' SET EXPIRES '2026-12-31T23:59:59Z'";
        let (_, stmt) = alter_mcp_token(Dialect::MySQL)(LocatedSpan::new(query)).unwrap();
        assert_eq!(stmt.name, "claude");
        assert_eq!(
            stmt.expires,
            McpTokenExpiresChange::At("2026-12-31T23:59:59Z".to_string())
        );
    }

    #[test]
    fn test_alter_mcp_token_never_expires() {
        let query = b"ALTER MCP TOKEN 'claude' SET NEVER EXPIRES";
        let (_, stmt) = alter_mcp_token(Dialect::MySQL)(LocatedSpan::new(query)).unwrap();
        assert_eq!(stmt.name, "claude");
        assert_eq!(stmt.expires, McpTokenExpiresChange::Never);
    }
}
