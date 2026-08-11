//! RSC admission gate (twin-cache plan, Phase 1b): make CREATE CACHE honest.
//!
//! ReadySet admits some query shapes whose dataflow serves reads but never
//! delivers Reader deltas — "zombie caches": the Tier-1 Redis invalidation
//! bridge (see `readyset-dataflow/src/redis_notifier.rs`) never fires for
//! them, so downstream caches keyed off those deltas silently go stale.
//!
//! Shape classes known to produce zombies, detected here on the real
//! `readyset-sql` AST at cache-creation time:
//! - correlated scalar subquery in the SELECT projection list
//! - correlated subquery (incl. EXISTS / NOT EXISTS / IN) in WHERE
//!
//! Behavior is controlled by `READYSET_ADMISSION_MODE`:
//! - `off`     — no checks
//! - `warn`    — (DEFAULT) loud warning log line plus
//!   `SETEX rs:t1_suspect:{cache_name} 604800 {class}` so the PHP side can
//!   see the verdict; creation proceeds
//! - `enforce` — refuse creation with a clear error
//!
//! The ground-truth complement is the delta-liveness registry
//! (`rs:t1_live:{cache_name}`, written by the dataflow notifier on every
//! delivered delta): admission flags *suspected* zombies at creation,
//! liveness *proves* which caches actually receive deltas.
//!
//! # Which statement gets classified (and why it matters)
//!
//! The classifier must see the statement **as submitted**. The adapter's
//! `CREATE CACHE` path runs `adapter_rewrites::rewrite_query` →
//! `rewrite_equivalent_deep` before it builds the `ViewCreateRequest`, and that
//! pipeline includes `unnest_subqueries`, which *decorrelates* correlated
//! subqueries (an `EXISTS`/`IN`/scalar subquery becomes a join against a
//! grouped derived table). Classifying the post-rewrite form therefore finds
//! nothing to flag: the correlation the class is named after has been rewritten
//! away. Decorrelation makes those shapes *serve reads*; it does not make them
//! *deliver Reader deltas*, so they remain zombies while looking healthy to an
//! AST walk.
//!
//! [`classify_for_admission`] is the entry point the backend uses: it inspects
//! the submitted statement first and falls back to the rewritten one (both for
//! `CREATE CACHE FROM <query_id>`, where no submitted statement exists, and as
//! a belt so the pre-existing post-rewrite verdicts remain a subset of the
//! verdicts reached today).

use std::env;
use std::sync::OnceLock;

use readyset_sql::ast::{
    CacheInner, Expr, FieldDefinitionExpr, InValue, JoinRightSide, SelectStatement, TableExpr,
    TableExprInner,
};
use readyset_sql_passes::is_correlated;
use tracing::warn;

/// Shape class: correlated scalar subquery in the SELECT projection list.
pub const CLASS_CORRELATED_PROJECTION: &str = "correlated-scalar-subquery-in-projection";
/// Shape class: correlated subquery (incl. EXISTS/NOT EXISTS/IN) in WHERE.
pub const CLASS_CORRELATED_WHERE: &str = "correlated-subquery-in-where";

/// Not a shape class from the AST walk: upstream's shallow caches have no
/// dataflow and therefore no Reader deltas — they are permanently excluded
/// from RSC twin/HYD eligibility (upstream-sync ruling, 2026-08-10). Every
/// shallow cache is marked suspect with this class so the PHP side sees the
/// verdict; `rs:t1_live` can never fire for a shallow cache.
pub const CLASS_SHALLOW_CACHE: &str = "shallow-cache";

/// Suspect-key TTL: 7 days, matching the rs:t1_live liveness registry.
const SUSPECT_TTL_SECS: u64 = 604_800;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionMode {
    Off,
    Warn,
    Enforce,
}

/// Read `READYSET_ADMISSION_MODE` once (process lifetime). Default: warn.
pub fn admission_mode() -> AdmissionMode {
    static MODE: OnceLock<AdmissionMode> = OnceLock::new();
    *MODE.get_or_init(|| match env::var("READYSET_ADMISSION_MODE").as_deref() {
        Ok("off") => AdmissionMode::Off,
        Ok("enforce") => AdmissionMode::Enforce,
        Ok("warn") | Err(_) => AdmissionMode::Warn,
        Ok(other) => {
            warn!(
                mode = %other,
                "Unknown READYSET_ADMISSION_MODE value; defaulting to 'warn'"
            );
            AdmissionMode::Warn
        }
    })
}

/// The statement as the client submitted it, when this `CacheInner` carries one.
///
/// `CREATE CACHE FROM <query_id>` (`CacheInner::Id`) has no submitted statement:
/// the only form available is the one already stored in the query status cache,
/// which is post-rewrite. Same for a `CREATE CACHE` whose inner SELECT failed to
/// parse — there is nothing to classify and cache creation fails anyway.
pub fn submitted_statement(inner: &CacheInner) -> Option<&SelectStatement> {
    match inner {
        CacheInner::Statement { deep: Ok(stmt), .. } => Some(stmt.as_ref()),
        CacheInner::Statement { deep: Err(_), .. } | CacheInner::Id(_) => None,
    }
}

/// Admission verdict for a `CREATE CACHE`.
///
/// `submitted` is the statement as written by the client (pre-rewrite), when one
/// is available; `rewritten` is the statement the `ViewCreateRequest` was built
/// from, i.e. after `adapter_rewrites::rewrite_query`.
///
/// The submitted form is authoritative because the pipeline's
/// `unnest_subqueries` pass decorrelates correlated subqueries before the
/// `ViewCreateRequest` exists (see the module docs). The rewritten form is still
/// checked as a fallback so that:
///
/// - `CREATE CACHE FROM <query_id>`, which has no submitted statement, keeps the
///   only classification it can get, and
/// - every verdict the pre-fix code reached is still reached (the new verdict set
///   is a strict superset — this is a plumbing fix, not a policy change).
pub fn classify_for_admission(
    submitted: Option<&SelectStatement>,
    rewritten: &SelectStatement,
) -> Option<&'static str> {
    submitted
        .and_then(classify_zombie_shape)
        .or_else(|| classify_zombie_shape(rewritten))
}

/// Classify a SELECT against the known zombie shape classes.
///
/// Returns the first matching class, or `None` for shapes with reliable delta
/// maintenance (plain selects, joins — including LEFT JOIN + GROUP BY derived
/// tables — and *uncorrelated* subqueries).
///
/// Callers on the `CREATE CACHE` path want [`classify_for_admission`]: this
/// function is a pure AST walk and will not recognise a shape whose correlation
/// the rewrite pipeline has already removed.
pub fn classify_zombie_shape(stmt: &SelectStatement) -> Option<&'static str> {
    // 1. Correlated scalar subquery in the projection list.
    for field in &stmt.fields {
        if let FieldDefinitionExpr::Expr { expr, .. } = field {
            if let Some(class) = walk_expr(expr, CLASS_CORRELATED_PROJECTION) {
                return Some(class);
            }
        }
    }

    // 2. Correlated subquery in WHERE (EXISTS / NOT EXISTS / IN / scalar).
    if let Some(where_clause) = &stmt.where_clause {
        if let Some(class) = walk_expr(where_clause, CLASS_CORRELATED_WHERE) {
            return Some(class);
        }
    }

    // 3. Recurse into derived tables / CTEs: a zombie shape nested inside a
    // derived table starves the outer dataflow just the same.
    for sub in nested_statements(stmt) {
        if let Some(class) = classify_zombie_shape(sub) {
            return Some(class);
        }
    }

    None
}

/// Iterate the sub-SELECTs that appear as relations of `stmt` (CTEs, derived
/// tables in FROM, derived tables on the right side of JOINs).
fn nested_statements(stmt: &SelectStatement) -> impl Iterator<Item = &SelectStatement> {
    let ctes = stmt.ctes.iter().map(|cte| &cte.statement);
    let from_tables = stmt.tables.iter().filter_map(table_expr_subquery);
    let join_tables = stmt
        .join
        .iter()
        .flat_map(|join| match &join.right {
            JoinRightSide::Table(te) => std::slice::from_ref(te).iter(),
            JoinRightSide::Tables(tes) => tes.iter(),
        })
        .filter_map(table_expr_subquery);
    ctes.chain(from_tables).chain(join_tables)
}

fn table_expr_subquery(te: &TableExpr) -> Option<&SelectStatement> {
    match &te.inner {
        TableExprInner::Subquery(sq) => Some(sq.as_ref()),
        TableExprInner::Table(_) => None,
        // VALUES lists (upstream addition, stable-2604xx): literal row
        // constructors — no nested SELECT to descend into, cannot correlate.
        TableExprInner::Values { .. } => None,
    }
}

/// Check a subquery found inside an expression: correlated → the given class;
/// uncorrelated → not a zombie itself, but its own body may still contain one.
fn check_subquery(sq: &SelectStatement, class: &'static str) -> Option<&'static str> {
    if is_correlated(sq) {
        Some(class)
    } else {
        classify_zombie_shape(sq)
    }
}

/// Recursively search an expression tree for correlated subqueries.
/// `class` is the shape class to report for subqueries found in this context.
fn walk_expr(expr: &Expr, class: &'static str) -> Option<&'static str> {
    match expr {
        Expr::NestedSelect(sq) | Expr::Exists(sq) => check_subquery(sq, class),
        Expr::In { lhs, rhs, .. } => walk_expr(lhs, class).or_else(|| match rhs {
            InValue::Subquery(sq) => check_subquery(sq, class),
            InValue::List(exprs) => exprs.iter().find_map(|e| walk_expr(e, class)),
        }),
        Expr::BinaryOp { lhs, rhs, .. }
        | Expr::OpAny { lhs, rhs, .. }
        | Expr::OpSome { lhs, rhs, .. }
        | Expr::OpAll { lhs, rhs, .. } => {
            walk_expr(lhs, class).or_else(|| walk_expr(rhs, class))
        }
        Expr::UnaryOp { rhs, .. } => walk_expr(rhs, class),
        Expr::CaseWhen {
            branches,
            else_expr,
        } => branches
            .iter()
            .find_map(|b| walk_expr(&b.condition, class).or_else(|| walk_expr(&b.body, class)))
            .or_else(|| else_expr.as_deref().and_then(|e| walk_expr(e, class))),
        Expr::Between {
            operand, min, max, ..
        } => walk_expr(operand, class)
            .or_else(|| walk_expr(min, class))
            .or_else(|| walk_expr(max, class)),
        Expr::Cast { expr, .. }
        | Expr::ConvertUsing { expr, .. }
        | Expr::Collate { expr, .. } => walk_expr(expr, class),
        Expr::Call(func) => func.arguments().find_map(|e| walk_expr(e, class)),
        Expr::Row { exprs, .. } => exprs.iter().find_map(|e| walk_expr(e, class)),
        Expr::Array(args) => match args {
            readyset_sql::ast::ArrayArguments::List(exprs) => {
                exprs.iter().find_map(|e| walk_expr(e, class))
            }
            readyset_sql::ast::ArrayArguments::Subquery(sq) => check_subquery(sq, class),
        },
        Expr::WindowFunction { .. }
        | Expr::Literal(_)
        | Expr::Column(_)
        | Expr::Variable(_) => None,
    }
}

/// Fire-and-forget `SETEX rs:t1_suspect:{cache_name} 604800 {class}` so the
/// PHP side can see the admission verdict. Reuses the same env-based Redis
/// config as the notifiers (`READYSET_REDIS_URL` / `READYSET_REDIS_PREFIX`).
/// CREATE CACHE is rare, so a short-lived connection per call is fine.
pub fn mark_suspect(cache_name: &str, class: &'static str) {
    let cache_name = cache_name.to_string();
    tokio::spawn(async move {
        let redis_url =
            env::var("READYSET_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
        let prefix = env::var("READYSET_REDIS_PREFIX").unwrap_or_default();
        let client = match redis::Client::open(redis_url.as_str()) {
            Ok(c) => c,
            Err(e) => {
                warn!(%e, cache_name, "RSC admission: failed to create Redis client for rs:t1_suspect");
                return;
            }
        };
        let mut conn = match client.get_multiplexed_async_connection().await {
            Ok(c) => c,
            Err(e) => {
                warn!(%e, cache_name, "RSC admission: failed to connect to Redis for rs:t1_suspect");
                return;
            }
        };
        let key = format!("{}rs:t1_suspect:{}", prefix, cache_name);
        if let Err(e) = redis::cmd("SETEX")
            .arg(&key)
            .arg(SUSPECT_TTL_SECS)
            .arg(class)
            .query_async::<()>(&mut conn)
            .await
        {
            warn!(%e, cache_name, "RSC admission: failed to write rs:t1_suspect");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use readyset_sql::Dialect;
    use readyset_sql_parsing::parse_select;

    fn parse(q: &str) -> SelectStatement {
        parse_select(Dialect::PostgreSQL, q).unwrap()
    }

    #[test]
    fn flags_correlated_scalar_subquery_in_projection() {
        // The Eloquent withCount() shape — the demo order-board zombie.
        let stmt = parse(
            "SELECT o.id, o.email, \
             (SELECT count(*) FROM bp_invoices i WHERE i.order_id = o.id AND i.deleted_at IS NULL) AS total_invoices_count \
             FROM bp_orders o WHERE o.shipping_status = 'sent'",
        );
        assert_eq!(
            classify_zombie_shape(&stmt),
            Some(CLASS_CORRELATED_PROJECTION)
        );
    }

    #[test]
    fn flags_correlated_where_exists() {
        let stmt = parse(
            "SELECT o.id FROM bp_orders o \
             WHERE EXISTS (SELECT 1 FROM bp_invoices i WHERE i.order_id = o.id)",
        );
        assert_eq!(classify_zombie_shape(&stmt), Some(CLASS_CORRELATED_WHERE));
    }

    #[test]
    fn flags_correlated_where_not_exists() {
        let stmt = parse(
            "SELECT o.id FROM bp_orders o \
             WHERE NOT EXISTS (SELECT 1 FROM bp_invoices i WHERE i.order_id = o.id)",
        );
        assert_eq!(classify_zombie_shape(&stmt), Some(CLASS_CORRELATED_WHERE));
    }

    #[test]
    fn flags_correlated_scalar_subquery_in_where() {
        let stmt = parse(
            "SELECT o.id FROM bp_orders o \
             WHERE o.total = (SELECT max(i.total) FROM bp_invoices i WHERE i.order_id = o.id)",
        );
        assert_eq!(classify_zombie_shape(&stmt), Some(CLASS_CORRELATED_WHERE));
    }

    #[test]
    fn plain_healthy_select_not_flagged() {
        let stmt = parse(
            "SELECT id, email, payment_status FROM bp_orders \
             WHERE shipping_status = 'sent' AND deleted_at IS NULL \
             ORDER BY created_at DESC",
        );
        assert_eq!(classify_zombie_shape(&stmt), None);
    }

    #[test]
    fn left_join_group_by_derived_table_not_flagged() {
        // The decorrelated order-board shape — the healthy replacement.
        let stmt = parse(
            "SELECT o.id, o.email, coalesce(t.cnt, 0) AS total_invoices_count \
             FROM bp_orders o \
             LEFT JOIN (SELECT order_id, count(*) AS cnt FROM bp_invoices \
                        WHERE deleted_at IS NULL GROUP BY order_id) t \
             ON o.id = t.order_id \
             WHERE o.shipping_status = 'sent'",
        );
        assert_eq!(classify_zombie_shape(&stmt), None);
    }

    #[test]
    fn uncorrelated_scalar_subquery_not_flagged() {
        let stmt = parse(
            "SELECT o.id, (SELECT count(*) FROM bp_shops) AS shop_count FROM bp_orders o",
        );
        assert_eq!(classify_zombie_shape(&stmt), None);
    }

    #[test]
    fn uncorrelated_where_in_subquery_not_flagged() {
        let stmt = parse(
            "SELECT o.id FROM bp_orders o \
             WHERE o.shop_id IN (SELECT id FROM bp_shops WHERE active = true)",
        );
        assert_eq!(classify_zombie_shape(&stmt), None);
    }

    #[test]
    fn correlated_where_in_subquery_flagged() {
        let stmt = parse(
            "SELECT o.id FROM bp_orders o \
             WHERE o.id IN (SELECT i.order_id FROM bp_invoices i WHERE i.user_id = o.user_id)",
        );
        assert_eq!(classify_zombie_shape(&stmt), Some(CLASS_CORRELATED_WHERE));
    }

    #[test]
    fn zombie_nested_inside_derived_table_flagged() {
        let stmt = parse(
            "SELECT x.id FROM (SELECT o.id, \
             (SELECT count(*) FROM bp_invoices i WHERE i.order_id = o.id) AS c \
             FROM bp_orders o) x",
        );
        assert_eq!(
            classify_zombie_shape(&stmt),
            Some(CLASS_CORRELATED_PROJECTION)
        );
    }

    /// End-to-end: statement → the **real** adapter rewrite pipeline
    /// (`adapter_rewrites::rewrite_query`, the same entry point
    /// `Backend::query_from_cache_inner` calls) → admission classification.
    ///
    /// The tests above parse their own SQL and hand it straight to
    /// `classify_zombie_shape`. That is precisely why they stayed green while the
    /// live gate went blind: the 2026-08-10 upstream merge added
    /// `unnest_subqueries` to `rewrite_equivalent_deep`, which decorrelates the
    /// zombie shapes *before* the `ViewCreateRequest` — and hence before the gate
    /// — ever sees them. Anything that guards this behaviour has to traverse the
    /// pipeline.
    mod through_rewrite_pipeline {
        use std::collections::{HashMap, HashSet};
        use std::sync::Arc;

        use readyset_sql::ast::{CacheInner, SqlIdentifier};
        use readyset_sql_parsing::{parse_create_table, parse_select};
        use readyset_sql_passes::adapter_rewrites::{self, AdapterRewriteParams};
        use schema_catalog::{RewriteContext, SchemaCatalog, SchemaGeneration};

        use super::*;

        const ORDERS_DDL: &str = "CREATE TABLE public.bp_orders (\
             id int NOT NULL PRIMARY KEY, user_id int NOT NULL, shop_id int NOT NULL, \
             total int NOT NULL, email text NOT NULL, shipping_status text NOT NULL)";
        const INVOICES_DDL: &str = "CREATE TABLE public.bp_invoices (\
             id int NOT NULL PRIMARY KEY, order_id int NOT NULL, user_id int NOT NULL, \
             total int NOT NULL, deleted_at timestamp)";

        /// A real [`RewriteContext`] (the production context type) over a
        /// two-table catalog, so the pipeline's schema-dependent passes —
        /// `unnest_subqueries` among them — actually run.
        fn rewrite_context() -> RewriteContext {
            let mut base_schemas = HashMap::new();
            let mut view_schemas = HashMap::new();
            for ddl in [ORDERS_DDL, INVOICES_DDL] {
                let stmt = parse_create_table(Dialect::PostgreSQL, ddl).unwrap();
                let body = stmt.body.clone().unwrap();
                let cols: Vec<SqlIdentifier> =
                    body.fields.iter().map(|f| f.column.name.clone()).collect();
                view_schemas.insert(stmt.table.clone(), cols);
                base_schemas.insert(stmt.table.clone(), body);
            }
            let catalog = SchemaCatalog {
                generation: SchemaGeneration::INITIAL,
                base_schemas,
                uncompiled_views: HashMap::new(),
                custom_types: HashMap::new(),
                view_schemas,
                non_replicated_relations: HashSet::new(),
            };
            RewriteContext::new(
                readyset_data::Dialect::DEFAULT_POSTGRESQL,
                Arc::new(catalog),
                vec![SqlIdentifier::from("public")],
            )
        }

        fn rewrite_params() -> AdapterRewriteParams {
            AdapterRewriteParams {
                dialect: Dialect::PostgreSQL,
                server_supports_topk: true,
                server_supports_pagination: true,
                server_supports_mixed_comparisons: true,
                autoparameterize: true,
            }
        }

        /// `(submitted, rewritten)` — exactly the pair `create_deep_cache` sees.
        fn submitted_and_rewritten(sql: &str) -> (SelectStatement, SelectStatement) {
            let submitted = parse_select(Dialect::PostgreSQL, sql).unwrap();
            let mut rewritten = submitted.clone();
            adapter_rewrites::rewrite_query(&mut rewritten, rewrite_params(), &rewrite_context())
                .expect("rewrite pipeline succeeds");
            (submitted, rewritten)
        }

        /// The blinding itself, asserted rather than assumed: after the real
        /// pipeline runs, a correlated `EXISTS` in WHERE is gone from the AST, so
        /// a raw walk of the rewritten statement finds nothing.
        ///
        /// This test also protects the two below from becoming vacuous — if the
        /// pipeline ever stops decorrelating (or the harness stops reaching the
        /// unnest pass), this assertion fails and says so, instead of letting the
        /// gate tests pass for the wrong reason.
        #[test]
        fn pipeline_decorrelates_and_blinds_the_raw_classifier() {
            let (submitted, rewritten) = submitted_and_rewritten(
                "SELECT o.id FROM bp_orders o \
                 WHERE EXISTS (SELECT 1 FROM bp_invoices i WHERE i.order_id = o.id)",
            );
            assert_eq!(
                classify_zombie_shape(&submitted),
                Some(CLASS_CORRELATED_WHERE),
                "the submitted statement is a correlated-WHERE zombie"
            );
            assert_eq!(
                classify_zombie_shape(&rewritten),
                None,
                "the rewrite pipeline decorrelated the subquery, so a raw walk of the \
                 rewritten statement is blind to it — this is the regression that let \
                 zombie shapes through the gate"
            );
        }

        /// The gate's contract: a correlated subquery in WHERE is still flagged
        /// after the pipeline has run.
        #[test]
        fn admission_flags_correlated_where_after_rewrite() {
            let (submitted, rewritten) = submitted_and_rewritten(
                "SELECT o.id FROM bp_orders o \
                 WHERE EXISTS (SELECT 1 FROM bp_invoices i WHERE i.order_id = o.id)",
            );
            assert_eq!(
                classify_for_admission(Some(&submitted), &rewritten),
                Some(CLASS_CORRELATED_WHERE)
            );
        }

        /// Same contract for the `withCount()` shape — a correlated scalar
        /// subquery in the projection.
        #[test]
        fn admission_flags_correlated_projection_after_rewrite() {
            let (submitted, rewritten) = submitted_and_rewritten(
                "SELECT o.id, o.email, \
                 (SELECT count(*) FROM bp_invoices i \
                  WHERE i.order_id = o.id AND i.deleted_at IS NULL) AS total_invoices_count \
                 FROM bp_orders o WHERE o.shipping_status = 'sent'",
            );
            assert_eq!(
                classify_for_admission(Some(&submitted), &rewritten),
                Some(CLASS_CORRELATED_PROJECTION)
            );
        }

        /// Correlated `IN`, the third live class, through the pipeline.
        #[test]
        fn admission_flags_correlated_in_after_rewrite() {
            let (submitted, rewritten) = submitted_and_rewritten(
                "SELECT o.id FROM bp_orders o \
                 WHERE o.id IN (SELECT i.order_id FROM bp_invoices i WHERE i.user_id = o.user_id)",
            );
            assert_eq!(
                classify_for_admission(Some(&submitted), &rewritten),
                Some(CLASS_CORRELATED_WHERE)
            );
        }

        /// No false positives: healthy shapes stay admitted through the pipeline.
        /// `gb_ctrl_flat` is the flat control used on the live box; the
        /// LEFT JOIN + GROUP BY derived table is the decorrelated replacement the
        /// gate's error message tells operators to write.
        #[test]
        fn admission_admits_healthy_shapes_after_rewrite() {
            for sql in [
                "SELECT id, email, shipping_status FROM bp_orders \
                 WHERE shipping_status = 'sent' ORDER BY id DESC",
                "SELECT o.id, o.email, coalesce(t.cnt, 0) AS total_invoices_count \
                 FROM bp_orders o \
                 LEFT JOIN (SELECT order_id, count(*) AS cnt FROM bp_invoices \
                            WHERE deleted_at IS NULL GROUP BY order_id) t \
                 ON o.id = t.order_id \
                 WHERE o.shipping_status = 'sent'",
                "SELECT i.order_id, count(*) AS cnt FROM bp_invoices i \
                 WHERE i.deleted_at IS NULL GROUP BY i.order_id",
                "SELECT o.id FROM bp_orders o \
                 WHERE o.shop_id IN (SELECT id FROM bp_invoices WHERE total > 0)",
            ] {
                let (submitted, rewritten) = submitted_and_rewritten(sql);
                assert_eq!(
                    classify_for_admission(Some(&submitted), &rewritten),
                    None,
                    "healthy shape must not be flagged: {sql}"
                );
            }
        }

        /// `CREATE CACHE <name> FROM SELECT …` carries the submitted statement;
        /// `CREATE CACHE FROM <query_id>` cannot, and falls back to the rewritten
        /// form (pre-fix behaviour) rather than skipping classification.
        #[test]
        fn submitted_statement_extraction() {
            let stmt = parse("SELECT id FROM bp_orders");
            let inner = CacheInner::Statement {
                deep: Ok(Box::new(stmt.clone())),
                shallow: Err("no shallow".to_string()),
            };
            assert_eq!(submitted_statement(&inner), Some(&stmt));

            let by_id = CacheInner::Id(SqlIdentifier::from("q_0000000000000000"));
            assert!(submitted_statement(&by_id).is_none());

            let unparseable = CacheInner::Statement {
                deep: Err("nope".to_string()),
                shallow: Err("nope".to_string()),
            };
            assert!(submitted_statement(&unparseable).is_none());
        }

        /// With no submitted statement the rewritten form is still classified —
        /// the fallback must not silently admit everything.
        #[test]
        fn falls_back_to_rewritten_when_no_submitted_statement() {
            let zombie = parse(
                "SELECT o.id FROM bp_orders o \
                 WHERE EXISTS (SELECT 1 FROM bp_invoices i WHERE i.order_id = o.id)",
            );
            assert_eq!(
                classify_for_admission(None, &zombie),
                Some(CLASS_CORRELATED_WHERE)
            );
            let healthy = parse("SELECT id FROM bp_orders WHERE shipping_status = 'sent'");
            assert_eq!(classify_for_admission(None, &healthy), None);
        }
    }
}
