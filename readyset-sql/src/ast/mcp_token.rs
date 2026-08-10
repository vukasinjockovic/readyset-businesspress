use std::fmt;

use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{Dialect, DialectDisplay};

/// The scope of permissions granted by an MCP token.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum McpTokenScope {
    /// SHOW commands, EXPLAIN, status queries only.
    ReadOnly,
    /// ReadOnly + CREATE/DROP CACHE.
    CacheAdmin,
    /// All operations including ALTER READYSET.
    Full,
}

impl fmt::Display for McpTokenScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadOnly => write!(f, "read_only"),
            Self::CacheAdmin => write!(f, "cache_admin"),
            Self::Full => write!(f, "full"),
        }
    }
}

/// `CREATE MCP TOKEN '<name>' [WITH SCOPE <scope>] [EXPIRES '<rfc3339-datetime>']`
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct CreateMcpTokenStatement {
    pub name: String,
    pub scope: Option<McpTokenScope>,
    /// Expiration timestamp as an RFC 3339 string. `None` means the token never expires.
    pub expires: Option<String>,
}

impl DialectDisplay for CreateMcpTokenStatement {
    fn display(&self, _dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(f, "CREATE MCP TOKEN '{}'", self.name)?;
            if let Some(scope) = &self.scope {
                write!(f, " WITH SCOPE {scope}")?;
            }
            if let Some(expires) = &self.expires {
                write!(f, " EXPIRES '{expires}'")?;
            }
            Ok(())
        })
    }
}

/// `DROP MCP TOKEN '<name>'`
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct DropMcpTokenStatement {
    pub name: String,
}

impl DialectDisplay for DropMcpTokenStatement {
    fn display(&self, _dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| write!(f, "DROP MCP TOKEN '{}'", self.name))
    }
}

/// Expiration change for `ALTER MCP TOKEN`.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum McpTokenExpiresChange {
    /// `EXPIRES '<rfc3339>'` — set a new expiration timestamp.
    At(String),
    /// `NEVER EXPIRES` — remove the expiration, token never expires.
    Never,
}

/// `ALTER MCP TOKEN '<name>' SET (EXPIRES '<datetime>' | NEVER EXPIRES)`
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct AlterMcpTokenStatement {
    pub name: String,
    pub expires: McpTokenExpiresChange,
}

impl DialectDisplay for AlterMcpTokenStatement {
    fn display(&self, _dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(f, "ALTER MCP TOKEN '{}' SET ", self.name)?;
            match &self.expires {
                McpTokenExpiresChange::At(s) => write!(f, "EXPIRES '{s}'"),
                McpTokenExpiresChange::Never => write!(f, "NEVER EXPIRES"),
            }
        })
    }
}
