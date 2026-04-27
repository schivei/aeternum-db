//! Internal Abstract Syntax Tree (AST) for AeternumDB SQL.
//!
//! This module defines a clean, type-safe AST that is independent of the
//! [`sqlparser`] crate's AST.  All external SQL is first parsed by
//! [`sqlparser`] and then lowered into this internal representation via
//! [`Statement::try_from`].
//!
//! The internal AST is intentionally simpler than the full sqlparser AST:
//! it retains only the constructs that the query planner and executor need
//! and discards syntactic sugar that has already been normalized.
//!
//! # Supported statements
//!
//! | Category | Statement |
//! |----------|-----------|
//! | DML | [`Statement::Select`], [`Statement::Insert`], [`Statement::Update`], [`Statement::Delete`] |
//! | DDL | [`Statement::CreateTable`], [`Statement::DropTable`], [`Statement::AlterTable`] |
//! | DCL | [`Statement::Grant`], [`Statement::Revoke`] (scaffolding only) |

use sqlparser::ast as sp;

// ── Error type ────────────────────────────────────────────────────────────────

/// Errors that can occur while converting a sqlparser AST node to the internal
/// AST.
#[derive(Debug, Clone, PartialEq)]
pub enum AstError {
    /// A SQL construct is not yet supported by the internal AST.
    Unsupported(String),
    /// A required field was absent or could not be converted.
    Invalid(String),
}

impl std::fmt::Display for AstError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AstError::Unsupported(s) => write!(f, "unsupported SQL construct: {s}"),
            AstError::Invalid(s) => write!(f, "invalid AST node: {s}"),
        }
    }
}

impl std::error::Error for AstError {}

// ── Value ─────────────────────────────────────────────────────────────────────

/// A literal SQL value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// Integer literal (`42`).
    Integer(i64),
    /// Floating-point literal (`3.14`).
    Float(f64),
    /// String literal (`'hello'`).
    String(String),
    /// Boolean literal (`TRUE` / `FALSE`).
    Boolean(bool),
    /// The SQL `NULL` literal.
    Null,
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Integer(n) => write!(f, "{n}"),
            Value::Float(n) => write!(f, "{n}"),
            Value::String(s) => write!(f, "'{s}'"),
            Value::Boolean(b) => write!(f, "{}", if *b { "TRUE" } else { "FALSE" }),
            Value::Null => write!(f, "NULL"),
        }
    }
}

// ── Data types ────────────────────────────────────────────────────────────────

/// SQL column data type.
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    /// `INTEGER` / `INT` / `BIGINT` etc.
    Integer,
    /// `FLOAT` / `REAL` / `DOUBLE`.
    Float,
    /// `VARCHAR(n)` / `TEXT` / `CHAR(n)`.
    Varchar(Option<u64>),
    /// `BOOLEAN` / `BOOL`.
    Boolean,
    /// `DATE`.
    Date,
    /// `TIMESTAMP`.
    Timestamp,
    /// `DECIMAL(p, s)` / `NUMERIC(p, s)`.
    Decimal(Option<u64>, Option<u64>),
    /// Reference to a single row in another table: `table_name`.
    /// Used for foreign key relationships and OO-style references.
    Reference(String),
    /// Array of references to multiple rows: `[table_name]`.
    /// Used for one-to-many relationships.
    ReferenceArray(String),
    /// Virtual reverse reference (computed): `~table_name(column)`.
    /// Provides inverse navigation without storing data.
    VirtualReference {
        table: String,
        column: String,
    },
    /// Virtual reverse reference array: `~[table_name](column)`.
    VirtualReferenceArray {
        table: String,
        column: String,
    },
    /// Any other type forwarded as a string (for forward-compatibility).
    Other(String),
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Integer => write!(f, "INTEGER"),
            DataType::Float => write!(f, "FLOAT"),
            DataType::Varchar(Some(n)) => write!(f, "VARCHAR({n})"),
            DataType::Varchar(None) => write!(f, "TEXT"),
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::Date => write!(f, "DATE"),
            DataType::Timestamp => write!(f, "TIMESTAMP"),
            DataType::Decimal(Some(p), Some(s)) => write!(f, "DECIMAL({p},{s})"),
            DataType::Decimal(Some(p), None) => write!(f, "DECIMAL({p})"),
            DataType::Decimal(None, _) => write!(f, "DECIMAL"),
            DataType::Reference(table) => write!(f, "{table}"),
            DataType::ReferenceArray(table) => write!(f, "[{table}]"),
            DataType::VirtualReference { table, column } => write!(f, "~{table}({column})"),
            DataType::VirtualReferenceArray { table, column } => {
                write!(f, "~[{table}]({column})")
            }
            DataType::Other(s) => write!(f, "{s}"),
        }
    }
}

// ── Expressions ───────────────────────────────────────────────────────────────

/// Binary operators used in expressions.
#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOperator {
    // Arithmetic
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    // Comparison
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    // Logical
    And,
    Or,
    // String
    Like,
    NotLike,
}

impl std::fmt::Display for BinaryOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            BinaryOperator::Plus => "+",
            BinaryOperator::Minus => "-",
            BinaryOperator::Multiply => "*",
            BinaryOperator::Divide => "/",
            BinaryOperator::Modulo => "%",
            BinaryOperator::Eq => "=",
            BinaryOperator::NotEq => "!=",
            BinaryOperator::Lt => "<",
            BinaryOperator::LtEq => "<=",
            BinaryOperator::Gt => ">",
            BinaryOperator::GtEq => ">=",
            BinaryOperator::And => "AND",
            BinaryOperator::Or => "OR",
            BinaryOperator::Like => "LIKE",
            BinaryOperator::NotLike => "NOT LIKE",
        };
        write!(f, "{s}")
    }
}

/// Unary operators.
#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOperator {
    /// Negation (`-expr`).
    Minus,
    /// Logical NOT (`NOT expr`).
    Not,
}

/// A SQL expression node.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// A literal value (`42`, `'hello'`, `TRUE`, `NULL`).
    Literal(Value),
    /// A column reference, optionally qualified (`table.column`).
    Column {
        /// Optional table qualifier.
        table: Option<String>,
        /// Column name.
        name: String,
    },
    /// `*` – select all columns (used inside [`SelectItem::Wildcard`]).
    Wildcard,
    /// A binary operation (`a + b`, `x = y`).
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    /// A unary operation (`-x`, `NOT b`).
    UnaryOp { op: UnaryOperator, expr: Box<Expr> },
    /// A function call (`COUNT(*)`, `SUM(price)`).
    Function {
        /// Function name (normalized to uppercase).
        name: String,
        /// Arguments.
        args: Vec<Expr>,
        /// `COUNT(DISTINCT …)` flag.
        distinct: bool,
    },
    /// `expr IS NULL` / `expr IS NOT NULL`.
    IsNull { expr: Box<Expr>, negated: bool },
    /// `expr BETWEEN low AND high`.
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    /// `expr IN (list)`.
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// `expr IN (subquery)`.
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<SelectStatement>,
        negated: bool,
    },
    /// A scalar subquery used as an expression: `(SELECT …)`.
    Subquery(Box<SelectStatement>),
    /// A `CAST(expr AS type)` expression.
    Cast {
        expr: Box<Expr>,
        data_type: DataType,
    },
    /// A conditional `CASE WHEN … THEN … ELSE … END` expression.
    Case {
        operand: Option<Box<Expr>>,
        conditions: Vec<(Expr, Expr)>,
        else_result: Option<Box<Expr>>,
    },
}

// ── SELECT helpers ─────────────────────────────────────────────────────────────

/// A single item in the SELECT list.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    /// `*`
    Wildcard,
    /// `table.*`
    QualifiedWildcard(String),
    /// An expression, optionally aliased.
    Expr { expr: Expr, alias: Option<String> },
}

/// A table reference in the FROM clause.
#[derive(Debug, Clone, PartialEq)]
pub enum TableReference {
    /// A plain table name, optionally aliased.
    Named { name: String, alias: Option<String> },
    /// A subquery in the FROM clause, with a mandatory alias.
    Subquery {
        query: Box<SelectStatement>,
        alias: String,
    },
    /// A JOIN between two table references.
    Join {
        left: Box<TableReference>,
        right: Box<TableReference>,
        join_type: JoinType,
        condition: Option<Expr>,
    },
}

/// JOIN type variants.
#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

/// An ORDER BY clause item.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub ascending: bool,
}

// ── DML statement bodies ───────────────────────────────────────────────────────

/// A `SELECT` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    /// Common Table Expressions (CTEs) defined with WITH clause.
    pub with: Vec<CommonTableExpr>,
    /// The SELECT list.
    pub columns: Vec<SelectItem>,
    /// The FROM clause.
    pub from: Option<TableReference>,
    /// The WHERE clause.
    pub where_clause: Option<Expr>,
    /// GROUP BY expressions.
    pub group_by: Vec<Expr>,
    /// HAVING clause (requires GROUP BY).
    pub having: Option<Expr>,
    /// ORDER BY list.
    pub order_by: Vec<OrderByExpr>,
    /// LIMIT count.
    pub limit: Option<u64>,
    /// OFFSET count.
    pub offset: Option<u64>,
    /// Whether DISTINCT was specified.
    pub distinct: bool,
}

/// A Common Table Expression (CTE) in a WITH clause.
#[derive(Debug, Clone, PartialEq)]
pub struct CommonTableExpr {
    /// CTE name (alias).
    pub name: String,
    /// Column names (optional).
    pub columns: Vec<String>,
    /// The query that defines this CTE.
    pub query: Box<SelectStatement>,
}

/// An `INSERT` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStatement {
    /// Target table name.
    pub table: String,
    /// Explicit column list, or empty if omitted.
    pub columns: Vec<String>,
    /// Row values to insert.  Each inner `Vec` is one row.
    pub values: Vec<Vec<Expr>>,
}

/// An `UPDATE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStatement {
    /// Target table name.
    pub table: String,
    /// Assignments (`column = expr`).
    pub assignments: Vec<(String, Expr)>,
    /// Optional WHERE clause.
    pub where_clause: Option<Expr>,
}

/// A `DELETE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStatement {
    /// Target table name.
    pub table: String,
    /// Optional WHERE clause.
    pub where_clause: Option<Expr>,
}

// ── DDL statement bodies ───────────────────────────────────────────────────────

/// A column definition inside `CREATE TABLE`.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub default: Option<Expr>,
    /// AUTO_INCREMENT flag (for integer primary keys).
    pub auto_increment: bool,
    /// Minimum array length for ReferenceArray types.
    pub min_length: Option<u64>,
    /// Maximum array length for ReferenceArray types.
    pub max_length: Option<u64>,
    /// UNIQUES constraint for ReferenceArray (all references must be distinct).
    pub uniques: bool,
}

/// A `CREATE TABLE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStatement {
    pub table: String,
    pub columns: Vec<ColumnDef>,
    /// `IF NOT EXISTS` flag.
    pub if_not_exists: bool,
}

/// A `DROP TABLE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DropTableStatement {
    pub tables: Vec<String>,
    /// `IF EXISTS` flag.
    pub if_exists: bool,
}

/// An `ALTER TABLE` operation.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableOperation {
    AddColumn(ColumnDef),
    DropColumn { name: String, if_exists: bool },
    RenameColumn { old_name: String, new_name: String },
    RenameTable { new_name: String },
}

/// An `ALTER TABLE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableStatement {
    pub table: String,
    pub operations: Vec<AlterTableOperation>,
}

// ── DCL scaffolding ─────────────────────────────────────────────────────────

/// A `GRANT` statement (scaffolding; not yet executed).
#[derive(Debug, Clone, PartialEq)]
pub struct GrantStatement {
    pub privileges: Vec<String>,
    pub on: String,
    pub to: Vec<String>,
}

/// A `REVOKE` statement (scaffolding; not yet executed).
#[derive(Debug, Clone, PartialEq)]
pub struct RevokeStatement {
    pub privileges: Vec<String>,
    pub on: String,
    pub from: Vec<String>,
}

// ── Transaction control ────────────────────────────────────────────────────────

/// Transaction isolation level.
#[derive(Debug, Clone, PartialEq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

/// `BEGIN TRANSACTION` / `START TRANSACTION` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct BeginTransactionStatement {
    pub isolation_level: Option<IsolationLevel>,
    pub read_only: bool,
}

/// `COMMIT` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CommitStatement;

/// `ROLLBACK` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct RollbackStatement {
    pub savepoint: Option<String>,
}

/// `SAVEPOINT` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct SavepointStatement {
    pub name: String,
}

/// `RELEASE SAVEPOINT` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct ReleaseSavepointStatement {
    pub name: String,
}

// ── Top-level statement ────────────────────────────────────────────────────────

/// A fully lowered SQL statement ready for the query planner.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateTable(CreateTableStatement),
    DropTable(DropTableStatement),
    AlterTable(AlterTableStatement),
    /// DCL scaffolding — recognized but not yet executed.
    Grant(GrantStatement),
    /// DCL scaffolding — recognized but not yet executed.
    Revoke(RevokeStatement),
    /// Transaction control statements.
    BeginTransaction(BeginTransactionStatement),
    Commit(CommitStatement),
    Rollback(RollbackStatement),
    Savepoint(SavepointStatement),
    ReleaseSavepoint(ReleaseSavepointStatement),
}

// ── Conversion from sqlparser AST ─────────────────────────────────────────────

impl TryFrom<sp::Statement> for Statement {
    type Error = AstError;

    fn try_from(stmt: sp::Statement) -> Result<Self, Self::Error> {
        match stmt {
            sp::Statement::Query(q) => Ok(Statement::Select(convert_query(*q)?)),
            sp::Statement::Insert(insert) => convert_insert(insert),
            sp::Statement::Update(update) => convert_update(update),
            sp::Statement::Delete(delete) => convert_delete(delete),
            sp::Statement::CreateTable(ct) => convert_create_table(ct),
            sp::Statement::Drop {
                object_type,
                if_exists,
                names,
                ..
            } => convert_drop(object_type, if_exists, names),
            sp::Statement::AlterTable(alt) => convert_alter_table(alt),
            sp::Statement::Grant(grant) => convert_grant(grant),
            sp::Statement::Revoke(revoke) => convert_revoke(revoke),
            other => Err(AstError::Unsupported(format!(
                "statement type not supported: {other}"
            ))),
        }
    }
}

// ── Internal conversion helpers ────────────────────────────────────────────────

fn ident_to_string(ident: &sp::Ident) -> String {
    ident.value.clone()
}

fn object_name_to_string(name: &sp::ObjectName) -> String {
    name.0
        .iter()
        .filter_map(|part| match part {
            sp::ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
            sp::ObjectNamePart::Function(_) => None,
        })
        .collect::<Vec<_>>()
        .join(".")
}

// ── SELECT conversion ──────────────────────────────────────────────────────────

fn convert_query(query: sp::Query) -> Result<SelectStatement, AstError> {
    // Convert WITH clause (CTEs)
    let with_ctes = if let Some(with) = query.with {
        with.cte_tables
            .into_iter()
            .map(convert_cte)
            .collect::<Result<Vec<_>, _>>()?
    } else {
        vec![]
    };

    let mut stmt = match *query.body {
        sp::SetExpr::Select(select) => {
            convert_select(*select, query.order_by, query.limit_clause)?
        }
        other => {
            return Err(AstError::Unsupported(format!(
                "query body not supported: {other}"
            )))
        }
    };

    stmt.with = with_ctes;
    Ok(stmt)
}

fn convert_cte(cte: sp::Cte) -> Result<CommonTableExpr, AstError> {
    let name = ident_to_string(&cte.alias.name);
    let columns = cte
        .alias
        .columns
        .into_iter()
        .map(|col_def| col_def.name.value.clone())
        .collect();
    let query = Box::new(convert_query(*cte.query)?);
    Ok(CommonTableExpr {
        name,
        columns,
        query,
    })
}

fn convert_select(
    select: sp::Select,
    order_by: Option<sp::OrderBy>,
    limit_clause: Option<sp::LimitClause>,
) -> Result<SelectStatement, AstError> {
    let distinct = matches!(select.distinct, Some(sp::Distinct::Distinct));

    let columns = select
        .projection
        .into_iter()
        .map(convert_select_item)
        .collect::<Result<Vec<_>, _>>()?;

    let from = if select.from.is_empty() {
        None
    } else if select.from.len() == 1 {
        Some(convert_table_with_joins(
            select.from.into_iter().next().unwrap(),
        )?)
    } else {
        // Multiple FROM tables → implicit cross join
        let mut refs: Vec<TableReference> = select
            .from
            .into_iter()
            .map(convert_table_with_joins)
            .collect::<Result<Vec<_>, _>>()?;
        let mut acc = refs.remove(0);
        for r in refs {
            acc = TableReference::Join {
                left: Box::new(acc),
                right: Box::new(r),
                join_type: JoinType::Cross,
                condition: None,
            };
        }
        Some(acc)
    };

    let where_clause = select.selection.map(convert_expr).transpose()?;

    let group_by = match select.group_by {
        sp::GroupByExpr::All(_) => return Err(AstError::Unsupported("GROUP BY ALL".to_string())),
        sp::GroupByExpr::Expressions(exprs, _) => exprs
            .into_iter()
            .map(convert_expr)
            .collect::<Result<Vec<_>, _>>()?,
    };

    let having = select.having.map(convert_expr).transpose()?;

    let order_by_exprs = if let Some(ob) = order_by {
        match ob.kind {
            sp::OrderByKind::Expressions(exprs) => exprs
                .into_iter()
                .map(convert_order_by_expr)
                .collect::<Result<Vec<_>, _>>()?,
            sp::OrderByKind::All(_) => {
                return Err(AstError::Unsupported("ORDER BY ALL".to_string()))
            }
        }
    } else {
        vec![]
    };

    let (limit_val, offset_val) = match limit_clause {
        Some(sp::LimitClause::LimitOffset { limit, offset, .. }) => {
            let l = limit.map(|e| expr_to_u64(&e)).transpose()?;
            let o = offset.map(|o| expr_to_u64(&o.value)).transpose()?;
            (l, o)
        }
        Some(sp::LimitClause::OffsetCommaLimit { offset, limit }) => {
            let l = Some(expr_to_u64(&limit)?);
            let o = Some(expr_to_u64(&offset)?);
            (l, o)
        }
        None => (None, None),
    };

    Ok(SelectStatement {
        with: vec![], // Will be populated by convert_query
        columns,
        from,
        where_clause,
        group_by,
        having,
        order_by: order_by_exprs,
        limit: limit_val,
        offset: offset_val,
        distinct,
    })
}

fn expr_to_u64(expr: &sp::Expr) -> Result<u64, AstError> {
    match expr {
        sp::Expr::Value(sp::ValueWithSpan {
            value: sp::Value::Number(n, _),
            ..
        }) => n
            .parse::<u64>()
            .map_err(|_| AstError::Invalid(format!("expected non-negative integer, got {n}"))),
        other => Err(AstError::Invalid(format!(
            "expected integer literal for LIMIT/OFFSET, got {other}"
        ))),
    }
}

fn convert_select_item(item: sp::SelectItem) -> Result<SelectItem, AstError> {
    match item {
        sp::SelectItem::Wildcard(_) => Ok(SelectItem::Wildcard),
        sp::SelectItem::QualifiedWildcard(kind, _) => {
            let name = match kind {
                sp::SelectItemQualifiedWildcardKind::ObjectName(n) => object_name_to_string(&n),
                sp::SelectItemQualifiedWildcardKind::Expr(e) => format!("{e}"),
            };
            Ok(SelectItem::QualifiedWildcard(name))
        }
        sp::SelectItem::UnnamedExpr(e) => Ok(SelectItem::Expr {
            expr: convert_expr(e)?,
            alias: None,
        }),
        sp::SelectItem::ExprWithAlias { expr, alias } => Ok(SelectItem::Expr {
            expr: convert_expr(expr)?,
            alias: Some(ident_to_string(&alias)),
        }),
    }
}

fn convert_table_with_joins(twj: sp::TableWithJoins) -> Result<TableReference, AstError> {
    let mut result = convert_table_factor(twj.relation)?;
    for join in twj.joins {
        let right = convert_table_factor(join.relation)?;
        let (join_type, condition) = convert_join_operator(join.join_operator)?;
        result = TableReference::Join {
            left: Box::new(result),
            right: Box::new(right),
            join_type,
            condition,
        };
    }
    Ok(result)
}

fn convert_table_factor(factor: sp::TableFactor) -> Result<TableReference, AstError> {
    match factor {
        sp::TableFactor::Table { name, alias, .. } => Ok(TableReference::Named {
            name: object_name_to_string(&name),
            alias: alias.map(|a| ident_to_string(&a.name)),
        }),
        sp::TableFactor::Derived {
            subquery, alias, ..
        } => {
            let alias_name = alias.map(|a| ident_to_string(&a.name)).ok_or_else(|| {
                AstError::Invalid("subquery in FROM must have an alias".to_string())
            })?;
            Ok(TableReference::Subquery {
                query: Box::new(convert_query(*subquery)?),
                alias: alias_name,
            })
        }
        other => Err(AstError::Unsupported(format!(
            "table factor not supported: {other}"
        ))),
    }
}

fn convert_join_operator(op: sp::JoinOperator) -> Result<(JoinType, Option<Expr>), AstError> {
    match op {
        sp::JoinOperator::Join(c) | sp::JoinOperator::Inner(c) => {
            Ok((JoinType::Inner, convert_join_constraint(c)?))
        }
        sp::JoinOperator::Left(c) | sp::JoinOperator::LeftOuter(c) => {
            Ok((JoinType::Left, convert_join_constraint(c)?))
        }
        sp::JoinOperator::Right(c) | sp::JoinOperator::RightOuter(c) => {
            Ok((JoinType::Right, convert_join_constraint(c)?))
        }
        sp::JoinOperator::FullOuter(c) => Ok((JoinType::Full, convert_join_constraint(c)?)),
        sp::JoinOperator::CrossJoin(_) => Ok((JoinType::Cross, None)),
        other => Err(AstError::Unsupported(format!(
            "join type not supported: {other:?}"
        ))),
    }
}

fn convert_join_constraint(constraint: sp::JoinConstraint) -> Result<Option<Expr>, AstError> {
    match constraint {
        sp::JoinConstraint::On(e) => Ok(Some(convert_expr(e)?)),
        sp::JoinConstraint::None => Ok(None),
        sp::JoinConstraint::Natural | sp::JoinConstraint::Using(_) => {
            // Natural / USING joins are not yet supported in the internal AST.
            Ok(None)
        }
    }
}

fn convert_order_by_expr(expr: sp::OrderByExpr) -> Result<OrderByExpr, AstError> {
    Ok(OrderByExpr {
        expr: convert_expr(expr.expr)?,
        ascending: expr.options.asc.unwrap_or(true),
    })
}

// ── Expression conversion ──────────────────────────────────────────────────────

/// Convert a sqlparser expression to the internal [`Expr`].
pub fn convert_expr(expr: sp::Expr) -> Result<Expr, AstError> {
    match expr {
        sp::Expr::Value(v) => Ok(Expr::Literal(convert_value(v.value)?)),
        sp::Expr::Identifier(ident) => Ok(Expr::Column {
            table: None,
            name: ident_to_string(&ident),
        }),
        sp::Expr::CompoundIdentifier(parts) => {
            let names: Vec<String> = parts.iter().map(ident_to_string).collect();
            if names.len() == 2 {
                Ok(Expr::Column {
                    table: Some(names[0].clone()),
                    name: names[1].clone(),
                })
            } else {
                Ok(Expr::Column {
                    table: None,
                    name: names.join("."),
                })
            }
        }
        sp::Expr::Wildcard(_) => Ok(Expr::Wildcard),
        sp::Expr::QualifiedWildcard(name, _) => Ok(Expr::Column {
            table: Some(object_name_to_string(&name)),
            name: "*".to_string(),
        }),
        sp::Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: Box::new(convert_expr(*left)?),
            op: convert_binary_op(op)?,
            right: Box::new(convert_expr(*right)?),
        }),
        sp::Expr::UnaryOp { op, expr } => Ok(Expr::UnaryOp {
            op: convert_unary_op(op)?,
            expr: Box::new(convert_expr(*expr)?),
        }),
        sp::Expr::Function(f) => convert_function(f),
        sp::Expr::IsNull(e) => Ok(Expr::IsNull {
            expr: Box::new(convert_expr(*e)?),
            negated: false,
        }),
        sp::Expr::IsNotNull(e) => Ok(Expr::IsNull {
            expr: Box::new(convert_expr(*e)?),
            negated: true,
        }),
        sp::Expr::Between {
            expr,
            negated,
            low,
            high,
        } => Ok(Expr::Between {
            expr: Box::new(convert_expr(*expr)?),
            low: Box::new(convert_expr(*low)?),
            high: Box::new(convert_expr(*high)?),
            negated,
        }),
        sp::Expr::InList {
            expr,
            list,
            negated,
        } => Ok(Expr::InList {
            expr: Box::new(convert_expr(*expr)?),
            list: list
                .into_iter()
                .map(convert_expr)
                .collect::<Result<Vec<_>, _>>()?,
            negated,
        }),
        sp::Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => Ok(Expr::InSubquery {
            expr: Box::new(convert_expr(*expr)?),
            subquery: Box::new(convert_query(*subquery)?),
            negated,
        }),
        sp::Expr::Subquery(q) => Ok(Expr::Subquery(Box::new(convert_query(*q)?))),
        sp::Expr::Cast {
            kind: sp::CastKind::Cast,
            expr,
            data_type,
            ..
        } => Ok(Expr::Cast {
            expr: Box::new(convert_expr(*expr)?),
            data_type: convert_data_type(data_type)?,
        }),
        sp::Expr::Nested(e) => convert_expr(*e),
        sp::Expr::Like {
            expr,
            negated,
            pattern,
            ..
        } => Ok(Expr::BinaryOp {
            left: Box::new(convert_expr(*expr)?),
            op: if negated {
                BinaryOperator::NotLike
            } else {
                BinaryOperator::Like
            },
            right: Box::new(convert_expr(*pattern)?),
        }),
        sp::Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            let ops = operand.map(|o| convert_expr(*o)).transpose()?;
            let conds = conditions
                .into_iter()
                .map(|c| {
                    let cond = convert_expr(c.condition)?;
                    let result = convert_expr(c.result)?;
                    Ok((cond, result))
                })
                .collect::<Result<Vec<_>, AstError>>()?;
            let els = else_result.map(|e| convert_expr(*e)).transpose()?;
            Ok(Expr::Case {
                operand: ops.map(Box::new),
                conditions: conds,
                else_result: els.map(Box::new),
            })
        }
        other => Err(AstError::Unsupported(format!(
            "expression not supported: {other}"
        ))),
    }
}

fn convert_value(val: sp::Value) -> Result<Value, AstError> {
    match val {
        sp::Value::Number(n, _) => {
            if n.contains('.') {
                n.parse::<f64>()
                    .map(Value::Float)
                    .map_err(|_| AstError::Invalid(format!("invalid float: {n}")))
            } else {
                n.parse::<i64>()
                    .map(Value::Integer)
                    .map_err(|_| AstError::Invalid(format!("invalid integer: {n}")))
            }
        }
        sp::Value::SingleQuotedString(s) | sp::Value::DoubleQuotedString(s) => Ok(Value::String(s)),
        sp::Value::Boolean(b) => Ok(Value::Boolean(b)),
        sp::Value::Null => Ok(Value::Null),
        other => Err(AstError::Unsupported(format!(
            "literal value not supported: {other}"
        ))),
    }
}

fn convert_binary_op(op: sp::BinaryOperator) -> Result<BinaryOperator, AstError> {
    match op {
        sp::BinaryOperator::Plus => Ok(BinaryOperator::Plus),
        sp::BinaryOperator::Minus => Ok(BinaryOperator::Minus),
        sp::BinaryOperator::Multiply => Ok(BinaryOperator::Multiply),
        sp::BinaryOperator::Divide => Ok(BinaryOperator::Divide),
        sp::BinaryOperator::Modulo => Ok(BinaryOperator::Modulo),
        sp::BinaryOperator::Eq => Ok(BinaryOperator::Eq),
        sp::BinaryOperator::NotEq => Ok(BinaryOperator::NotEq),
        sp::BinaryOperator::Lt => Ok(BinaryOperator::Lt),
        sp::BinaryOperator::LtEq => Ok(BinaryOperator::LtEq),
        sp::BinaryOperator::Gt => Ok(BinaryOperator::Gt),
        sp::BinaryOperator::GtEq => Ok(BinaryOperator::GtEq),
        sp::BinaryOperator::And => Ok(BinaryOperator::And),
        sp::BinaryOperator::Or => Ok(BinaryOperator::Or),
        other => Err(AstError::Unsupported(format!(
            "binary operator not supported: {other}"
        ))),
    }
}

fn convert_unary_op(op: sp::UnaryOperator) -> Result<UnaryOperator, AstError> {
    match op {
        sp::UnaryOperator::Minus => Ok(UnaryOperator::Minus),
        sp::UnaryOperator::Not => Ok(UnaryOperator::Not),
        other => Err(AstError::Unsupported(format!(
            "unary operator not supported: {other}"
        ))),
    }
}

fn convert_function(f: sp::Function) -> Result<Expr, AstError> {
    let name = object_name_to_string(&f.name).to_uppercase();
    let (distinct, args) = match f.args {
        sp::FunctionArguments::None => (false, vec![]),
        sp::FunctionArguments::List(list) => {
            let distinct = matches!(
                list.duplicate_treatment,
                Some(sp::DuplicateTreatment::Distinct)
            );
            let args = list
                .args
                .into_iter()
                .map(|a| match a {
                    sp::FunctionArg::Unnamed(sp::FunctionArgExpr::Expr(e)) => convert_expr(e),
                    sp::FunctionArg::Unnamed(sp::FunctionArgExpr::Wildcard) => Ok(Expr::Wildcard),
                    sp::FunctionArg::Named {
                        arg: sp::FunctionArgExpr::Expr(e),
                        ..
                    } => convert_expr(e),
                    other => Err(AstError::Unsupported(format!(
                        "function argument not supported: {other}"
                    ))),
                })
                .collect::<Result<Vec<_>, _>>()?;
            (distinct, args)
        }
        other => {
            return Err(AstError::Unsupported(format!(
                "function arguments style not supported: {other}"
            )))
        }
    };
    Ok(Expr::Function {
        name,
        args,
        distinct,
    })
}

// ── Data type conversion ───────────────────────────────────────────────────────

fn convert_data_type(dt: sp::DataType) -> Result<DataType, AstError> {
    match dt {
        sp::DataType::Int(_)
        | sp::DataType::Integer(_)
        | sp::DataType::BigInt(_)
        | sp::DataType::SmallInt(_)
        | sp::DataType::TinyInt(_) => Ok(DataType::Integer),
        sp::DataType::Float(_)
        | sp::DataType::Real
        | sp::DataType::Double(_)
        | sp::DataType::DoublePrecision => Ok(DataType::Float),
        sp::DataType::Varchar(n) => Ok(DataType::Varchar(char_length_to_u64(n))),
        sp::DataType::Char(n) => Ok(DataType::Varchar(char_length_to_u64(n))),
        sp::DataType::Text => Ok(DataType::Varchar(None)),
        sp::DataType::Boolean => Ok(DataType::Boolean),
        sp::DataType::Bool => Ok(DataType::Boolean),
        sp::DataType::Date => Ok(DataType::Date),
        sp::DataType::Timestamp(_, _) => Ok(DataType::Timestamp),
        sp::DataType::Decimal(info) | sp::DataType::Numeric(info) => {
            let (p, s) = match info {
                sp::ExactNumberInfo::None => (None, None),
                sp::ExactNumberInfo::Precision(p) => (Some(p), None),
                sp::ExactNumberInfo::PrecisionAndScale(p, s) => (Some(p), Some(s as u64)),
            };
            Ok(DataType::Decimal(p, s))
        }
        other => Ok(DataType::Other(format!("{other}"))),
    }
}

fn char_length_to_u64(cl: Option<sp::CharacterLength>) -> Option<u64> {
    match cl {
        Some(sp::CharacterLength::IntegerLength { length, .. }) => Some(length),
        _ => None,
    }
}

// ── INSERT conversion ─────────────────────────────────────────────────────────

fn convert_insert(insert: sp::Insert) -> Result<Statement, AstError> {
    let table = match insert.table {
        sp::TableObject::TableName(name) => object_name_to_string(&name),
        sp::TableObject::TableFunction(_) => {
            return Err(AstError::Unsupported(
                "INSERT INTO TABLE FUNCTION not supported".to_string(),
            ))
        }
    };

    let columns: Vec<String> = insert.columns.iter().map(ident_to_string).collect();

    let values = match insert.source {
        Some(source) => match *source.body {
            sp::SetExpr::Values(sp::Values { rows, .. }) => rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(convert_expr)
                        .collect::<Result<Vec<_>, _>>()
                })
                .collect::<Result<Vec<_>, _>>()?,
            other => {
                return Err(AstError::Unsupported(format!(
                    "INSERT source not supported: {other}"
                )))
            }
        },
        None => vec![],
    };

    Ok(Statement::Insert(InsertStatement {
        table,
        columns,
        values,
    }))
}

// ── UPDATE conversion ─────────────────────────────────────────────────────────

fn convert_update(update: sp::Update) -> Result<Statement, AstError> {
    let table_name = match &update.table.relation {
        sp::TableFactor::Table { name, .. } => object_name_to_string(name),
        other => {
            return Err(AstError::Unsupported(format!(
                "UPDATE table factor not supported: {other}"
            )))
        }
    };

    let assignments: Vec<(String, Expr)> = update
        .assignments
        .into_iter()
        .map(|a| {
            let col = match a.target {
                sp::AssignmentTarget::ColumnName(name) => object_name_to_string(&name),
                sp::AssignmentTarget::Tuple(names) => names
                    .iter()
                    .map(object_name_to_string)
                    .collect::<Vec<_>>()
                    .join(", "),
            };
            let val = convert_expr(a.value)?;
            Ok((col, val))
        })
        .collect::<Result<Vec<_>, AstError>>()?;

    let where_clause = update.selection.map(convert_expr).transpose()?;

    Ok(Statement::Update(UpdateStatement {
        table: table_name,
        assignments,
        where_clause,
    }))
}

// ── DELETE conversion ─────────────────────────────────────────────────────────

fn convert_delete(delete: sp::Delete) -> Result<Statement, AstError> {
    let table = if !delete.tables.is_empty() {
        object_name_to_string(&delete.tables[0])
    } else {
        let from_tables = match &delete.from {
            sp::FromTable::WithFromKeyword(tables) | sp::FromTable::WithoutKeyword(tables) => {
                tables
            }
        };
        if !from_tables.is_empty() {
            match &from_tables[0].relation {
                sp::TableFactor::Table { name, .. } => object_name_to_string(name),
                other => {
                    return Err(AstError::Unsupported(format!(
                        "DELETE FROM factor not supported: {other}"
                    )))
                }
            }
        } else {
            return Err(AstError::Invalid("DELETE has no target table".to_string()));
        }
    };

    let where_clause = delete.selection.map(convert_expr).transpose()?;

    Ok(Statement::Delete(DeleteStatement {
        table,
        where_clause,
    }))
}

// ── CREATE TABLE conversion ────────────────────────────────────────────────────

fn convert_create_table(ct: sp::CreateTable) -> Result<Statement, AstError> {
    let table = object_name_to_string(&ct.name);
    let columns = ct
        .columns
        .into_iter()
        .map(convert_column_def)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Statement::CreateTable(CreateTableStatement {
        table,
        columns,
        if_not_exists: ct.if_not_exists,
    }))
}

fn convert_column_def(col: sp::ColumnDef) -> Result<ColumnDef, AstError> {
    let name = ident_to_string(&col.name);
    let data_type = convert_data_type(col.data_type)?;

    let mut nullable = true;
    let mut primary_key = false;
    let mut unique = false;
    let mut default = None;
    let auto_increment = false;
    let min_length = None;
    let max_length = None;
    let uniques = false;

    for option in col.options {
        match option.option {
            sp::ColumnOption::Null => nullable = true,
            sp::ColumnOption::NotNull => nullable = false,
            sp::ColumnOption::PrimaryKey(_) => {
                primary_key = true;
                nullable = false;
            }
            sp::ColumnOption::Unique(_) => {
                unique = true;
            }
            sp::ColumnOption::Default(e) => {
                default = Some(convert_expr(e)?);
            }
            _ => {}
        }
    }

    Ok(ColumnDef {
        name,
        data_type,
        nullable,
        primary_key,
        unique,
        default,
        auto_increment,
        min_length,
        max_length,
        uniques,
    })
}

// ── DROP TABLE conversion ──────────────────────────────────────────────────────

fn convert_drop(
    object_type: sp::ObjectType,
    if_exists: bool,
    names: Vec<sp::ObjectName>,
) -> Result<Statement, AstError> {
    match object_type {
        sp::ObjectType::Table => {
            let tables = names.iter().map(object_name_to_string).collect();
            Ok(Statement::DropTable(DropTableStatement {
                tables,
                if_exists,
            }))
        }
        other => Err(AstError::Unsupported(format!("DROP {other} not supported"))),
    }
}

// ── ALTER TABLE conversion ─────────────────────────────────────────────────────

fn convert_alter_table(alt: sp::AlterTable) -> Result<Statement, AstError> {
    let table = object_name_to_string(&alt.name);
    let ops = alt
        .operations
        .into_iter()
        .map(convert_alter_operation)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Statement::AlterTable(AlterTableStatement {
        table,
        operations: ops,
    }))
}

fn convert_alter_operation(op: sp::AlterTableOperation) -> Result<AlterTableOperation, AstError> {
    match op {
        sp::AlterTableOperation::AddColumn { column_def, .. } => Ok(
            AlterTableOperation::AddColumn(convert_column_def(column_def)?),
        ),
        sp::AlterTableOperation::DropColumn {
            column_names,
            if_exists,
            ..
        } => {
            // sqlparser 0.61 allows dropping multiple columns in one operation;
            // we represent each as a separate internal operation.
            let name = column_names
                .into_iter()
                .map(|i| i.value)
                .collect::<Vec<_>>()
                .join(", ");
            Ok(AlterTableOperation::DropColumn { name, if_exists })
        }
        sp::AlterTableOperation::RenameColumn {
            old_column_name,
            new_column_name,
        } => Ok(AlterTableOperation::RenameColumn {
            old_name: ident_to_string(&old_column_name),
            new_name: ident_to_string(&new_column_name),
        }),
        sp::AlterTableOperation::RenameTable { table_name } => {
            let new_name = match table_name {
                sp::RenameTableNameKind::As(n) | sp::RenameTableNameKind::To(n) => {
                    object_name_to_string(&n)
                }
            };
            Ok(AlterTableOperation::RenameTable { new_name })
        }
        other => Err(AstError::Unsupported(format!(
            "ALTER TABLE operation not supported: {other}"
        ))),
    }
}

// ── GRANT / REVOKE conversion (scaffolding) ───────────────────────────────────

fn convert_grant(grant: sp::Grant) -> Result<Statement, AstError> {
    let privs: Vec<String> = match grant.privileges {
        sp::Privileges::All { .. } => vec!["ALL".to_string()],
        sp::Privileges::Actions(actions) => actions.iter().map(|a| format!("{a}")).collect(),
    };
    let on = grant
        .objects
        .map(|o| format!("{o}"))
        .unwrap_or_else(|| "*".to_string());
    let to = grant.grantees.iter().map(|g| format!("{g}")).collect();
    Ok(Statement::Grant(GrantStatement {
        privileges: privs,
        on,
        to,
    }))
}

fn convert_revoke(revoke: sp::Revoke) -> Result<Statement, AstError> {
    let privs: Vec<String> = match revoke.privileges {
        sp::Privileges::All { .. } => vec!["ALL".to_string()],
        sp::Privileges::Actions(actions) => actions.iter().map(|a| format!("{a}")).collect(),
    };
    let on = revoke
        .objects
        .map(|o| format!("{o}"))
        .unwrap_or_else(|| "*".to_string());
    let from = revoke.grantees.iter().map(|g| format!("{g}")).collect();
    Ok(Statement::Revoke(RevokeStatement {
        privileges: privs,
        on,
        from,
    }))
}
