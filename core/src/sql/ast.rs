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
    /// `INTEGER` / `INT` — 32-bit signed integer.
    Integer,
    /// `INTEGER UNSIGNED` / `INT UNSIGNED` — 32-bit unsigned integer.
    UnsignedInt,
    /// `FLOAT` / `REAL` — 32-bit signed floating-point (IEEE 754).
    Float,
    /// `DOUBLE` / `DOUBLE PRECISION` — 64-bit signed floating-point (IEEE 754).
    Double,
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
    VirtualReference { table: String, column: String },
    /// Virtual reverse reference array: `~[table_name](column)`.
    VirtualReferenceArray { table: String, column: String },
    /// Any other type forwarded as a string (for forward-compatibility).
    Other(String),
    /// `TINYINT` (1 byte, signed).
    TinyInt,
    /// `TINYINT UNSIGNED`.
    UnsignedTinyInt,
    /// `SMALLINT` (2 bytes, signed).
    SmallInt,
    /// `SMALLINT UNSIGNED`.
    UnsignedSmallInt,
    /// `MEDIUMINT` (3 bytes, signed).
    MediumInt,
    /// `MEDIUMINT UNSIGNED`.
    UnsignedMediumInt,
    /// `BIGINT` (8 bytes, signed).
    BigInt,
    /// `BIGINT UNSIGNED`.
    UnsignedBigInt,
    /// `CHAR(n)` — fixed-length character string.
    Char(Option<u64>),
    /// MySQL `TINYTEXT`.
    TinyText,
    /// MySQL `MEDIUMTEXT`.
    MediumText,
    /// MySQL `LONGTEXT`.
    LongText,
    /// `TIME` — time of day without a date component.
    Time,
    /// `TIME WITH TIME ZONE`.
    TimeTz,
    /// `DATETIME` — date and time without timezone.
    DateTime,
    /// `TIMESTAMP WITH TIME ZONE`.
    TimestampTz,
    /// Anonymous inline `ENUM` with named variants (MySQL-compatible syntax).
    ///
    /// The system **automatically assigns** numeric values to variants —
    /// users supply only the names (and the optional `NONE` marker for
    /// FLAG enums).  Values are:
    /// - Regular enum: sequential integers `0, 1, 2, …` in declaration order.
    /// - FLAG enum: `NONE = 0`; other variants get powers of 2 (`1, 2, 4, …`).
    ///
    /// For a **named, reusable** enum type that can be shared across tables
    /// and is protected from deletion when in use, use
    /// `CREATE TYPE name AS ENUM [FLAG] (...)` and reference it via
    /// [`DataType::EnumRef`].
    ///
    /// **Storage**: the column always stores the numeric `u64` value.
    /// The engine auto-casts `'active'` → its assigned number, and
    /// for FLAG enums `'read' | 'write'` → bitwise OR of their numbers.
    Enum {
        variants: Vec<EnumVariant>,
        /// `true` for bitmask / flag enumerations (`[Flags]` in C# terms).
        flag: bool,
    },
    /// Reference to a **named user-defined type** created with
    /// `CREATE TYPE name AS ENUM [FLAG] (...)` or
    /// `CREATE TYPE name AS (field type, ...)`.
    ///
    /// The catalog resolves the type and its auto-assigned values.
    /// The type cannot be dropped while any column references it.
    EnumRef(String),
    /// `UUID` / `GUID`.
    Uuid,
    /// `BINARY(n)` — fixed-length binary string.
    Binary(Option<u64>),
    /// `VARBINARY(n)` — variable-length binary string.
    Varbinary(Option<u64>),
    /// `BLOB(n)` — binary large object.
    Blob(Option<u64>),
    /// MySQL `TINYBLOB`.
    TinyBlob,
    /// MySQL `MEDIUMBLOB`.
    MediumBlob,
    /// MySQL `LONGBLOB`.
    LongBlob,
    /// A vector of values of the given element type: `[DataType]`.
    /// Elements can be any base type, e.g. `[INTEGER]`, `[VARCHAR(100)]`.
    Vector(Box<DataType>),
}

// ── EnumVariant ───────────────────────────────────────────────────────────────

/// A single named variant in an [`DataType::Enum`] or a
/// [`TypeDefinition::Enum`] type body.
///
/// **Users supply only the name** (and the `NONE` marker for FLAG enums).
/// Numeric values are assigned automatically by the system and are **immutable**
/// once the type is created — they can never be edited to prevent data corruption.
#[derive(Debug, Clone, PartialEq)]
pub struct EnumVariant {
    /// Variant name (normalized to lowercase).
    pub name: String,
    /// Marks the zero/empty-state variant for FLAG enums.
    ///
    /// Write `NONE` (case-insensitive) as the variant name to create this
    /// marker.  The system assigns value `0` to the NONE variant and
    /// powers-of-2 to the remaining variants.  A column whose enum type has
    /// a NONE variant is implicitly non-nullable — the NONE state already
    /// represents *absence of a value*.
    pub is_none: bool,
}

// ── TypeDefinition ────────────────────────────────────────────────────────────

/// Body of a `CREATE TYPE … AS …` statement.
#[derive(Debug, Clone, PartialEq)]
pub enum TypeDefinition {
    /// `AS ENUM [FLAG] ('name1', 'name2', …)`
    ///
    /// Declares a C#-style enumeration.  The system assigns the numeric
    /// values automatically (sequential for regular; powers-of-2 for FLAG)
    /// and stores them permanently in the catalog.  Values cannot be changed
    /// after creation.
    Enum {
        /// Whether this is a FLAG (bitmask) enum.
        flag: bool,
        /// Variant names in declaration order.  The system assigns values
        /// in this order; the order is therefore significant and immutable.
        variants: Vec<EnumVariant>,
    },
    /// `AS (field_name data_type [NOT NULL], …)`
    ///
    /// A composite / row type whose fields each have a name and data type.
    Composite(Vec<CompositeField>),
}

/// A single field in a [`TypeDefinition::Composite`] type.
#[derive(Debug, Clone, PartialEq)]
pub struct CompositeField {
    /// Field name (normalized to lowercase).
    pub name: String,
    /// Field data type.
    pub data_type: DataType,
    /// Whether the field is non-nullable.
    pub not_null: bool,
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Integer => write!(f, "INTEGER"),
            DataType::UnsignedInt => write!(f, "INTEGER UNSIGNED"),
            DataType::Float => write!(f, "FLOAT"),
            DataType::Double => write!(f, "DOUBLE"),
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
            DataType::TinyInt => write!(f, "TINYINT"),
            DataType::UnsignedTinyInt => write!(f, "TINYINT UNSIGNED"),
            DataType::SmallInt => write!(f, "SMALLINT"),
            DataType::UnsignedSmallInt => write!(f, "SMALLINT UNSIGNED"),
            DataType::MediumInt => write!(f, "MEDIUMINT"),
            DataType::UnsignedMediumInt => write!(f, "MEDIUMINT UNSIGNED"),
            DataType::BigInt => write!(f, "BIGINT"),
            DataType::UnsignedBigInt => write!(f, "BIGINT UNSIGNED"),
            DataType::Char(Some(n)) => write!(f, "CHAR({n})"),
            DataType::Char(None) => write!(f, "CHAR"),
            DataType::TinyText => write!(f, "TINYTEXT"),
            DataType::MediumText => write!(f, "MEDIUMTEXT"),
            DataType::LongText => write!(f, "LONGTEXT"),
            DataType::Time => write!(f, "TIME"),
            DataType::TimeTz => write!(f, "TIME WITH TIME ZONE"),
            DataType::DateTime => write!(f, "DATETIME"),
            DataType::TimestampTz => write!(f, "TIMESTAMP WITH TIME ZONE"),
            DataType::Enum { variants, flag } => {
                let kw = if *flag { "ENUM FLAG" } else { "ENUM" };
                let list = variants
                    .iter()
                    .map(|v| {
                        if v.is_none {
                            "NONE".to_string()
                        } else {
                            format!("'{}'", v.name)
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{kw}({list})")
            }
            DataType::EnumRef(name) => write!(f, "{name}"),
            DataType::Uuid => write!(f, "UUID"),
            DataType::Binary(Some(n)) => write!(f, "BINARY({n})"),
            DataType::Binary(None) => write!(f, "BINARY"),
            DataType::Varbinary(Some(n)) => write!(f, "VARBINARY({n})"),
            DataType::Varbinary(None) => write!(f, "VARBINARY"),
            DataType::Blob(Some(n)) => write!(f, "BLOB({n})"),
            DataType::Blob(None) => write!(f, "BLOB"),
            DataType::TinyBlob => write!(f, "TINYBLOB"),
            DataType::MediumBlob => write!(f, "MEDIUMBLOB"),
            DataType::LongBlob => write!(f, "LONGBLOB"),
            DataType::Vector(elem) => write!(f, "[{elem}]"),
            DataType::Other(s) => write!(f, "{s}"),
        }
    }
}

impl DataType {
    /// Compute the resolved numeric value for every variant in one pass.
    ///
    /// Auto-assignment rules:
    /// - Regular enum: sequential integers `0, 1, 2, …` (explicit values
    ///   advance the counter past themselves).
    /// - FLAG enum: powers of 2 `1, 2, 4, 8, …`; a NONE variant is always
    ///   `0` regardless of position.
    pub fn enum_resolved_values(&self) -> Vec<u64> {
        let (variants, flag) = match self {
            DataType::Enum { variants, flag } => (variants, *flag),
            _ => return vec![],
        };
        let mut out = vec![0u64; variants.len()];
        let mut next: u64 = if flag { 1 } else { 0 };
        for (i, v) in variants.iter().enumerate() {
            if v.is_none {
                out[i] = 0;
            } else {
                out[i] = next;
                next = if flag { next << 1 } else { next + 1 };
            }
        }
        out
    }

    /// Look up the stored numeric value for an enum variant by name
    /// (case-insensitive).  Returns `None` if `self` is not an `Enum` or
    /// the name does not match any variant.
    pub fn enum_value_of(&self, name: &str) -> Option<u64> {
        let (variants, _) = match self {
            DataType::Enum { variants, flag } => (variants, *flag),
            _ => return None,
        };
        let lower = name.to_lowercase();
        let resolved = self.enum_resolved_values();
        variants
            .iter()
            .zip(resolved.iter())
            .find(|(v, _)| {
                (v.is_none && lower == "none") || (!v.is_none && v.name.to_lowercase() == lower)
            })
            .map(|(_, &val)| val)
    }

    /// Return the variant name for a stored numeric value (regular enums).
    /// Returns `None` if `self` is not an `Enum` or the value does not
    /// match any variant exactly.
    pub fn enum_name_of(&self, value: u64) -> Option<&str> {
        let variants = match self {
            DataType::Enum { variants, .. } => variants,
            _ => return None,
        };
        self.enum_resolved_values()
            .into_iter()
            .zip(variants.iter())
            .find(|(v, _)| *v == value)
            .map(|(_, var)| {
                if var.is_none {
                    "none"
                } else {
                    var.name.as_str()
                }
            })
    }

    /// For a FLAG enum, decompose a bitmask `value` into the list of
    /// matching variant names.  For a regular enum returns a single-element
    /// vec (or empty if invalid).  Returns `[]` for `self` not being `Enum`.
    pub fn enum_decompose_flags(&self, value: u64) -> Vec<String> {
        let (variants, flag) = match self {
            DataType::Enum { variants, flag } => (variants, *flag),
            _ => return vec![],
        };
        if value == 0 {
            return variants
                .iter()
                .find(|v| v.is_none)
                .map(|_| vec!["none".to_string()])
                .unwrap_or_default();
        }
        if !flag {
            return self
                .enum_name_of(value)
                .map(|n| vec![n.to_string()])
                .unwrap_or_default();
        }
        self.enum_resolved_values()
            .into_iter()
            .zip(variants.iter())
            .filter(|(v, var)| !var.is_none && *v != 0 && (value & v) == *v)
            .map(|(_, var)| var.name.clone())
            .collect()
    }

    /// Check whether a numeric value is valid for this enum.
    ///
    /// - Regular enum: `value` must equal exactly one variant's resolved value.
    /// - FLAG enum: `value` must be a valid combination of variant bits;
    ///   `0` is valid only when a NONE variant exists.
    pub fn enum_is_valid_value(&self, value: u64) -> bool {
        let (variants, flag) = match self {
            DataType::Enum { variants, flag } => (variants, *flag),
            _ => return false,
        };
        if value == 0 {
            return variants.iter().any(|v| v.is_none);
        }
        if !flag {
            return self.enum_name_of(value).is_some();
        }
        let all_bits: u64 = self
            .enum_resolved_values()
            .into_iter()
            .zip(variants.iter())
            .filter(|(_, v)| !v.is_none)
            .fold(0, |acc, (v, _)| acc | v);
        (value & !all_bits) == 0
    }

    /// Check whether a string name is a valid variant for this enum
    /// (case-insensitive).
    pub fn enum_is_valid_name(&self, name: &str) -> bool {
        self.enum_value_of(name).is_some()
    }
}

// ── Expressions ───────────────────────────────────────────────────────────────

/// Binary operators used in expressions.
#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOperator {
    // ── Arithmetic ────────────────────────────────────────────────────────
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    // ── Comparison ───────────────────────────────────────────────────────
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    // ── Logical ──────────────────────────────────────────────────────────
    And,
    Or,
    // ── Pattern matching ─────────────────────────────────────────────────
    /// `LIKE` — case-sensitive wildcard match (`%`, `_`).
    Like,
    /// `NOT LIKE`.
    NotLike,
    /// `ILIKE` — case-insensitive wildcard match (PostgreSQL / AeternumDB).
    ILike,
    /// `NOT ILIKE`.
    NotILike,
    /// `SIMILAR TO` — SQL-standard regex-like pattern match.
    SimilarTo,
    /// `NOT SIMILAR TO`.
    NotSimilarTo,
    // ── Regular expression ────────────────────────────────────────────────
    /// `REGEXP` / `RLIKE` — case-sensitive regex match (MySQL-style).
    Regexp,
    /// `NOT REGEXP` / `NOT RLIKE`.
    NotRegexp,
    /// `~` — case-sensitive POSIX regex match (PostgreSQL-style).
    RegexpMatch,
    /// `~*` — case-insensitive POSIX regex match.
    RegexpIMatch,
    /// `!~` — case-sensitive POSIX regex non-match.
    NotRegexpMatch,
    /// `!~*` — case-insensitive POSIX regex non-match.
    NotRegexpIMatch,
    // ── Bitwise ──────────────────────────────────────────────────────────
    /// `&` — bitwise AND (also used to test FLAG enum bits).
    BitwiseAnd,
    /// `|` — bitwise OR (also used to combine FLAG enum variants).
    BitwiseOr,
    /// `^` — bitwise XOR.
    BitwiseXor,
    /// `<<` — left shift.
    ShiftLeft,
    /// `>>` — right shift.
    ShiftRight,
    // ── String concatenation ─────────────────────────────────────────────
    /// `||` — string concatenation (SQL standard / PostgreSQL).
    StringConcat,
    // ── Reverse pattern matching ─────────────────────────────────────────
    /// `pattern REVLIKE string` — reverse LIKE: the **left** side is the
    /// pattern and the **right** side is the value being tested.
    ///
    /// Useful when a column *contains* patterns and you want to check which
    /// stored patterns match a given string:
    /// ```sql
    /// -- Do any patterns in `rules.pattern_col` match the string 'hello world'?
    /// 'hello world' REVLIKE ANY (SELECT pattern_col FROM rules)
    /// ```
    RevLike,
    /// `pattern NOT REVLIKE string`.
    NotRevLike,
    /// `pattern REVILIKE string` — case-insensitive reverse LIKE.
    RevILike,
    /// `pattern NOT REVILIKE string`.
    NotRevILike,
    /// `pattern REVREGEXP string` — reverse REGEXP: the left side is the
    /// regex pattern, the right side is the value being tested.
    RevRegexp,
    /// `pattern NOT REVREGEXP string`.
    NotRevRegexp,
    /// `pattern REVREGEXP* string` — case-insensitive reverse REGEXP.
    RevRegexpIMatch,
    /// `pattern NOT REVREGEXP* string`.
    NotRevRegexpIMatch,
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
            BinaryOperator::ILike => "ILIKE",
            BinaryOperator::NotILike => "NOT ILIKE",
            BinaryOperator::SimilarTo => "SIMILAR TO",
            BinaryOperator::NotSimilarTo => "NOT SIMILAR TO",
            BinaryOperator::Regexp => "REGEXP",
            BinaryOperator::NotRegexp => "NOT REGEXP",
            BinaryOperator::RegexpMatch => "~",
            BinaryOperator::RegexpIMatch => "~*",
            BinaryOperator::NotRegexpMatch => "!~",
            BinaryOperator::NotRegexpIMatch => "!~*",
            BinaryOperator::BitwiseAnd => "&",
            BinaryOperator::BitwiseOr => "|",
            BinaryOperator::BitwiseXor => "^",
            BinaryOperator::ShiftLeft => "<<",
            BinaryOperator::ShiftRight => ">>",
            BinaryOperator::StringConcat => "||",
            BinaryOperator::RevLike => "REVLIKE",
            BinaryOperator::NotRevLike => "NOT REVLIKE",
            BinaryOperator::RevILike => "REVILIKE",
            BinaryOperator::NotRevILike => "NOT REVILIKE",
            BinaryOperator::RevRegexp => "REVREGEXP",
            BinaryOperator::NotRevRegexp => "NOT REVREGEXP",
            BinaryOperator::RevRegexpIMatch => "REVREGEXP*",
            BinaryOperator::NotRevRegexpIMatch => "NOT REVREGEXP*",
        };
        write!(f, "{s}")
    }
}

/// Unary operators.
#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOperator {
    /// Arithmetic negation (`-expr`).
    Minus,
    /// Logical NOT (`NOT expr`).
    Not,
    /// `~expr` — bitwise NOT (ones-complement).  Used with integer columns
    /// and FLAG enum bitmasks.
    BitwiseNot,
}

impl std::fmt::Display for UnaryOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnaryOperator::Minus => write!(f, "-"),
            UnaryOperator::Not => write!(f, "NOT"),
            UnaryOperator::BitwiseNot => write!(f, "~"),
        }
    }
}

// ── Quantifier for array/list operators ──────────────────────────────────────

/// Controls which elements of an array/list must satisfy a predicate in an
/// [`Expr::ArrayOp`] expression.
#[derive(Debug, Clone, PartialEq)]
pub enum ArrayQuantifier {
    /// True if **at least one** element satisfies the condition (`ANY` /
    /// `SOME`).  Equivalent to `IN` when the operator is `=`.
    Any,
    /// True if **every** element satisfies the condition (`ALL`).
    All,
}

// ── Text-function helpers ─────────────────────────────────────────────────────

/// Direction for [`Expr::Trim`].
#[derive(Debug, Clone, PartialEq)]
pub enum TrimWhereField {
    /// `TRIM(LEADING … FROM …)` — strip from the left.
    Leading,
    /// `TRIM(TRAILING … FROM …)` — strip from the right.
    Trailing,
    /// `TRIM(BOTH … FROM …)` — strip from both ends (default).
    Both,
}

// ── Full-text search ──────────────────────────────────────────────────────────

/// Search modifier for [`Expr::MatchAgainst`].
#[derive(Debug, Clone, PartialEq)]
pub enum TextSearchModifier {
    /// `IN NATURAL LANGUAGE MODE` (default, MySQL-style).
    NaturalLanguage,
    /// `IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION`.
    NaturalLanguageWithExpansion,
    /// `IN BOOLEAN MODE` — supports `+`, `-`, `*`, `"…"` operators.
    Boolean,
    /// `WITH QUERY EXPANSION`.
    WithExpansion,
}

/// The kind of index to create.  Controls physical storage and search
/// algorithm.  Defaults to [`IndexType::BTree`].
#[derive(Debug, Clone, PartialEq)]
pub enum IndexType {
    /// B-Tree index — default; efficient for equality and range queries.
    BTree,
    /// Hash index — O(1) equality lookups; no range queries.
    Hash,
    /// Generalized Inverted Index — full-text and array containment.
    Gin,
    /// Generalized Search Tree — geometric / range / full-text types.
    Gist,
    /// Space-partitioned GiST.
    SpGist,
    /// Block Range Index — very compact; for append-only / monotone data.
    Brin,
    /// Bloom filter — probabilistic; low false-negative rate.
    Bloom,
    /// MySQL-style `FULLTEXT` index (uses inverted word lists).
    FullText,
    /// Trigram index (`pg_trgm`-style) — fast `LIKE`/`REGEXP` substring
    /// search by storing all 3-character substrings of text values.
    Trigram,
    /// Any other index type forwarded as a string.
    Other(String),
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
    /// A binary operation (`a + b`, `x = y`, `name LIKE '%foo%'`).
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    /// A unary operation (`-x`, `NOT b`, `~flags`).
    UnaryOp { op: UnaryOperator, expr: Box<Expr> },
    /// A function call (`COUNT(*)`, `SUM(price)`, `UPPER(name)`).
    ///
    /// Standard text functions (`REPLACE`, `SUBSTRING`, `POSITION`, `TRIM`,
    /// `UPPER`, `LOWER`, `LENGTH`, `CONCAT`, `LEFT`, `RIGHT`, `LPAD`,
    /// `RPAD`, `CHAR_LENGTH`) and their regex variants (`REGEXP_REPLACE`,
    /// `REGEXP_SUBSTR`, `REGEXP_INSTR`, `REGEXP_LIKE`, `REGEXP_COUNT`) are
    /// captured here with the function name normalized to uppercase.
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
    /// `expr [NOT] IN (list)`.
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// `expr [NOT] IN (subquery)`.
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<SelectStatement>,
        negated: bool,
    },
    /// `expr <op> ANY|ALL (list | subquery | array_column)`
    ///
    /// Applies `op` between `expr` and **each element** of a list or the
    /// result of a subquery; the overall result depends on the quantifier:
    ///
    /// - `ANY` — true if at least one element satisfies the condition.
    /// - `ALL` — true if every element satisfies the condition.
    ///
    /// AeternumDB extends this beyond equality to any [`BinaryOperator`],
    /// including `LIKE`, `REGEXP`, bitwise operators, etc.:
    ///
    /// ```sql
    /// -- Standard SQL
    /// price > ALL (SELECT max_price FROM limits)
    ///
    /// -- AeternumDB extension (Phase 4 custom grammar)
    /// tag LIKE ANY ['%rust%', '%python%']
    /// score REGEXP ANY ['^A', '^B']
    /// ```
    ArrayOp {
        /// The left-hand expression tested against each element.
        expr: Box<Expr>,
        /// The operator applied between `expr` and each element.
        op: BinaryOperator,
        /// `ANY` or `ALL`.
        quantifier: ArrayQuantifier,
        /// The array elements or subquery.
        right: Box<Expr>,
    },
    /// A scalar subquery used as an expression: `(SELECT …)`.
    Subquery(Box<SelectStatement>),
    /// `CAST(expr AS type)`.
    Cast {
        expr: Box<Expr>,
        data_type: DataType,
    },
    /// `CASE [operand] WHEN … THEN … [ELSE …] END`.
    Case {
        operand: Option<Box<Expr>>,
        conditions: Vec<(Expr, Expr)>,
        else_result: Option<Box<Expr>>,
    },
    // ── SQL-standard text-function syntax ─────────────────────────────────
    /// `SUBSTRING(expr [FROM pos] [FOR len])` / `SUBSTR(expr, pos [, len])`.
    ///
    /// - `from_pos`: starting position (1-based).
    /// - `len`: maximum number of characters to return.
    Substring {
        expr: Box<Expr>,
        from_pos: Option<Box<Expr>>,
        len: Option<Box<Expr>>,
    },
    /// `POSITION(substr IN expr)` — 1-based index of first occurrence, or 0
    /// if not found.
    Position {
        substr: Box<Expr>,
        in_expr: Box<Expr>,
    },
    /// `TRIM([LEADING|TRAILING|BOTH] [trim_what] FROM expr)`.
    Trim {
        expr: Box<Expr>,
        /// Trim direction; defaults to `BOTH` when absent.
        trim_where: Option<TrimWhereField>,
        /// Characters to strip; defaults to space when `None`.
        trim_what: Option<Box<Expr>>,
    },
    // ── Full-text search ──────────────────────────────────────────────────
    /// `MATCH (col1, col2, …) AGAINST ('pattern' [modifier])`.
    ///
    /// MySQL-style full-text search.  Column names must reference indexed
    /// full-text or trigram columns.  Use [`TextSearchModifier::Boolean`]
    /// for boolean-mode queries (`+word -word "phrase" word*`).
    ///
    /// AeternumDB also supports the `@@` operator for PostgreSQL-compatible
    /// `tsquery` / `tsvector` style search; that is mapped to this node with
    /// `modifier: None` and the pattern in `match_value`.
    MatchAgainst {
        /// Columns to search (must have a FULLTEXT or TRIGRAM index).
        columns: Vec<String>,
        /// Search pattern / query string.
        match_value: Box<Expr>,
        /// Optional search modifier.
        modifier: Option<TextSearchModifier>,
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
    /// `EXPAND ref_col [AS alias]`
    ///
    /// Expands **all** columns from the target of a reference-typed column.
    /// When the column is a vector reference (multi-valued), the expansion also
    /// **unnests** the reference so each referenced row becomes its own result
    /// row.  Only valid in the `SELECT` list.
    ///
    /// The planner (PR 1.4) resolves `EXPAND` to the full column list of the
    /// referenced table and adds an implicit `UNNEST` step when the reference
    /// cardinality is > 1.
    ///
    /// When lowering from `sqlparser`, a function call named `EXPAND` (or
    /// `expand`) with a single argument is mapped to this variant.
    Expand {
        /// The reference-typed column to expand.
        expr: Box<Expr>,
        /// Optional alias prefix applied to all expanded columns
        /// (e.g. `EXPAND order_ref AS o` → `o.total`, `o.status`, …).
        alias: Option<String>,
    },
}

/// A single transformation item in a `VIEW AS` clause.
///
/// The `VIEW AS` clause is a post-result projection that applies primitive
/// expressions over the columns returned by the main `SELECT`.  Only primitive
/// expressions are allowed — **no aggregate functions and no sub-selects**.
/// The restriction is enforced by the semantic validator
/// ([`ValidationError::ViewAsAggregateNotAllowed`],
/// [`ValidationError::ViewAsSubqueryNotAllowed`]).
///
/// Syntax:
/// ```sql
/// SELECT id, score FROM users
/// VIEW AS (
///     score * 100 AS pct_score,
///     UPPER(name)  AS display_name
/// )
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct ViewAsItem {
    /// The transformation expression (primitive only — no aggregates, no
    /// sub-selects).
    pub expr: Expr,
    /// The name of the output column produced by this transformation.
    pub alias: String,
}

/// A table reference in the FROM clause.
#[derive(Debug, Clone, PartialEq)]
pub enum TableReference {
    /// A plain table name, optionally qualified with database and/or schema,
    /// and optionally aliased.
    ///
    /// Cross-database joins are not permitted; the `database` field is captured
    /// for routing but a [`ValidationError`] is raised if two tables in the
    /// same query reference different databases.
    Named {
        /// Database qualifier (e.g. `db` in `db.app.users`).  `None` means
        /// the active connection database.
        database: Option<String>,
        /// Schema qualifier (e.g. `app` in `app.users`).  `None` defaults to
        /// the `app` schema at execution time.
        schema: Option<String>,
        name: String,
        alias: Option<String>,
    },
    /// A subquery in the FROM clause, with a mandatory alias.
    Subquery {
        query: Box<SelectStatement>,
        alias: String,
    },
    /// A JOIN between two table references.
    ///
    /// AeternumDB joins are driven by **reference column types** — the join
    /// itself needs no `ON` clause because the relationship is encoded in the
    /// schema.  `filter_by` carries an *optional* additional predicate that
    /// further narrows the result set, expressed via the `FILTER BY` clause.
    ///
    /// When lowering from standard SQL, sqlparser's `ON` condition is mapped
    /// to `filter_by` so that existing SQL keeps working.
    Join {
        left: Box<TableReference>,
        right: Box<TableReference>,
        join_type: JoinType,
        /// Optional `FILTER BY` (or legacy `ON`) predicate.
        filter_by: Option<Expr>,
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
    /// Optional `VIEW AS (expr AS name, ...)` result-transformation clause.
    ///
    /// Applied as a final post-result projection after all filtering,
    /// grouping, ordering, and limiting.  Each item transforms or renames one
    /// output column using a **primitive** expression — aggregate functions and
    /// sub-selects are not permitted (validator enforces this).
    ///
    /// This is an AeternumDB-specific extension; parsing from raw SQL is a
    /// Phase 4 custom-grammar task.
    pub view_as: Option<Vec<ViewAsItem>>,
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
    /// `CHECK (expr)` constraint on this column.
    pub check: Option<Expr>,
    /// Multilingual text directive for this column (optional).
    /// When set the column stores a map of locale → value instead of a plain value.
    pub text_directive: Option<TextDirective>,
    /// Named typed metadata directives attached to each value of this column.
    pub terms_directives: Vec<TermsDirective>,
}

/// Directive for multilingual/localized text columns.
///
/// A text-directive column stores translations keyed by locale tag.
/// Clients access a specific locale with `column_name@'locale'` syntax.
/// When a requested locale is not present the `default_locale` is returned.
#[derive(Debug, Clone, PartialEq)]
pub struct TextDirective {
    /// The locale tag used when no directive is specified or the requested
    /// locale does not exist (e.g. `"en"`, `"pt-BR"`).
    pub default_locale: String,
}

/// Kind of value stored inside a terms-directive slot.
#[derive(Debug, Clone, PartialEq)]
pub enum TermsDirectiveKind {
    Text,
    Integer,
    Float,
    Boolean,
    Enum(Vec<String>),
}

/// A named terms-directive slot on a column.
///
/// Terms directives attach typed metadata to each cell value without
/// adding extra rows.  For example a price column could carry a
/// `currency` terms directive of kind `Text`.
#[derive(Debug, Clone, PartialEq)]
pub struct TermsDirective {
    /// Directive name (e.g. `"currency"`, `"unit"`).
    pub name: String,
    /// Type of value this directive accepts.
    pub kind: TermsDirectiveKind,
}

/// Behavior for a temporary table when the transaction that created it commits.
///
/// This mirrors the SQL-standard `ON COMMIT` clause for temporary tables:
/// - `PreserveRows` (default) — rows survive the commit; table is dropped when
///   the **session** ends.
/// - `DeleteRows` — rows are truncated on each commit, but the table structure
///   persists for the lifetime of the session.
/// - `Drop` — the table (and all its rows) is dropped when the transaction
///   commits.
///
/// If `CreateTableStatement::temporary` is `false` this field is always `None`.
#[derive(Debug, Clone, PartialEq)]
pub enum OnCommitBehavior {
    /// `ON COMMIT PRESERVE ROWS` — default if omitted.
    PreserveRows,
    /// `ON COMMIT DELETE ROWS` — truncate rows on each commit.
    DeleteRows,
    /// `ON COMMIT DROP` — drop the whole table on commit.
    Drop,
}

/// An index column: column name + optional ordering.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexColumn {
    pub name: String,
    pub ascending: Option<bool>,
}

/// A table-level constraint.
///
/// **Note**: `FOREIGN KEY` constraints are not supported in AeternumDB.
/// Use reference column types (`table_name`, `[table_name]`, `~table_name(col)`)
/// to express relationships.  Relationships are resolved via `objid` at
/// execution time rather than through declarative FK constraints.
#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraint {
    /// `PRIMARY KEY (col1, col2, ...)`.
    PrimaryKey {
        name: Option<String>,
        columns: Vec<String>,
    },
    /// `UNIQUE [name] (col1, col2, ...)`.
    Unique {
        name: Option<String>,
        columns: Vec<String>,
    },
    /// `CHECK (expr)`.
    Check { name: Option<String>, expr: Expr },
}

/// A `CREATE TABLE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStatement {
    /// Database qualifier (connection-level routing).  `None` = active database.
    pub database: Option<String>,
    /// Schema qualifier.  `None` defaults to the `app` schema.
    pub schema: Option<String>,
    pub table: String,
    pub columns: Vec<ColumnDef>,
    /// `IF NOT EXISTS` flag.
    pub if_not_exists: bool,
    /// `TEMPORARY` / `TEMP` flag.
    pub temporary: bool,
    /// Parent tables to inherit from (`INHERITS (parent, ...)`).
    pub inherits: Vec<String>,
    /// Lifecycle behavior for temporary tables (`ON COMMIT ...`).
    ///
    /// `None` means "default" (`PRESERVE ROWS` for temporary tables, ignored
    /// for permanent tables).  See [`OnCommitBehavior`].
    pub on_commit: Option<OnCommitBehavior>,
    /// Table-level constraints (composite primary keys, unique, checks).
    pub constraints: Vec<TableConstraint>,
    /// Whether this table uses system versioning (temporal/versioned data).
    /// When `true` each row is versioned and historical values are retained.
    /// Corresponds to SQL-standard `WITH SYSTEM VERSIONING`.
    pub versioned: bool,
    /// Whether this is a FLAT table.
    ///
    /// FLAT tables are optimised for fast sequential reads and do **not**
    /// support joins, reference column types, versioning, or inheritance.
    /// They are analogous to heap files — the simplest possible storage layout.
    pub flat: bool,
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
    AddColumn(Box<ColumnDef>),
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

/// A `CREATE INDEX` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndexStatement {
    /// Index name (optional).
    pub name: Option<String>,
    /// Table the index is on.
    pub table: String,
    /// Indexed columns.
    pub columns: Vec<IndexColumn>,
    /// Whether this is a UNIQUE index.
    pub unique: bool,
    /// `IF NOT EXISTS` flag.
    pub if_not_exists: bool,
    /// Physical index type.  Defaults to [`IndexType::BTree`].
    ///
    /// Use [`IndexType::FullText`] for `MATCH … AGAINST` queries,
    /// [`IndexType::Trigram`] for fast `LIKE`/`REGEXP` substring searches,
    /// [`IndexType::Gin`] for array-containment or JSONB queries.
    pub index_type: IndexType,
}

/// A `DROP INDEX` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DropIndexStatement {
    /// Index names to drop.
    pub names: Vec<String>,
    /// `IF EXISTS` flag.
    pub if_exists: bool,
}

// ── DCL scaffolding ─────────────────────────────────────────────────────────

/// A `GRANT` statement (scaffolding; not yet executed).
#[derive(Debug, Clone, PartialEq)]
pub struct GrantStatement {
    pub privileges: Vec<String>,
    /// Optional column list for column-level grants (e.g. `GRANT SELECT(col1) ON t TO u`).
    pub columns: Vec<String>,
    pub on: String,
    pub to: Vec<String>,
}

/// A `REVOKE` statement (scaffolding; not yet executed).
#[derive(Debug, Clone, PartialEq)]
pub struct RevokeStatement {
    pub privileges: Vec<String>,
    /// Optional column list for column-level revokes.
    pub columns: Vec<String>,
    pub on: String,
    pub from: Vec<String>,
}

/// A `CREATE MATERIALIZED VIEW` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateMaterializedViewStatement {
    /// View name.
    pub name: String,
    /// The query defining the view.
    pub query: Box<SelectStatement>,
    /// `IF NOT EXISTS` flag.
    pub if_not_exists: bool,
    /// `OR REPLACE` flag.
    pub or_replace: bool,
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

/// Scope for a `COMMIT` in a nested-transaction stack.
#[derive(Debug, Clone, PartialEq)]
pub enum CommitScope {
    /// Commit only the current (innermost) open transaction.
    ///
    /// This is the default when no explicit scope is given (`COMMIT`).
    /// If nested transactions exist the outermost transaction remains open;
    /// the inner nesting level is collapsed.
    Current,
    /// Commit a specific named transaction and every transaction nested
    /// inside it, up to and including that level.
    ///
    /// Syntax: `COMMIT TRANSACTION <name>` (AeternumDB extension).
    Named(String),
    /// Commit *all* open transactions in the nesting stack at once —
    /// equivalent to issuing `COMMIT` at every nesting level in sequence.
    ///
    /// Syntax: `COMMIT ALL` (AeternumDB extension).
    All,
}

/// Scope for a `ROLLBACK` in a nested-transaction stack.
#[derive(Debug, Clone, PartialEq)]
pub enum RollbackScope {
    /// Roll back only the current (innermost) open transaction.
    ///
    /// This is the default when no explicit scope is given (`ROLLBACK`).
    Current,
    /// Roll back to a named savepoint within the current transaction.
    ///
    /// Syntax: `ROLLBACK TO [SAVEPOINT] <name>`.
    ToSavepoint(String),
    /// Roll back a specific named nested transaction and everything inside
    /// it, then resume at the level just above that transaction.
    ///
    /// Syntax: `ROLLBACK TRANSACTION <name>` (AeternumDB extension).
    Named(String),
    /// Roll back *all* open transactions in the nesting stack at once —
    /// equivalent to issuing `ROLLBACK` at every nesting level in sequence.
    ///
    /// Syntax: `ROLLBACK ALL` (AeternumDB extension).
    All,
}

/// `BEGIN TRANSACTION` / `START TRANSACTION` statement.
///
/// Issuing `BEGIN` while a transaction is already open starts a **nested**
/// transaction. The execution layer assigns an auto-generated savepoint name
/// unless an explicit `name` is provided.
#[derive(Debug, Clone, PartialEq)]
pub struct BeginTransactionStatement {
    /// Optional name for this transaction level.
    ///
    /// Syntax: `BEGIN TRANSACTION <name>` (AeternumDB extension — requires
    /// Phase 4 custom SQL grammar; parsed programmatically for now).
    pub name: Option<String>,
    pub isolation_level: Option<IsolationLevel>,
    pub read_only: bool,
}

/// `COMMIT` / `COMMIT TRANSACTION` / `COMMIT ALL` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CommitStatement {
    /// Which nesting level(s) to commit.
    pub scope: CommitScope,
    /// `true` when `AND CHAIN` was present — immediately starts a new
    /// transaction with the same isolation level after the commit.
    pub chain: bool,
}

/// `ROLLBACK` / `ROLLBACK TO SAVEPOINT` / `ROLLBACK TRANSACTION` / `ROLLBACK ALL`.
#[derive(Debug, Clone, PartialEq)]
pub struct RollbackStatement {
    /// Which nesting level(s) to roll back.
    pub scope: RollbackScope,
    /// `true` when `AND CHAIN` was present — immediately starts a new
    /// transaction with the same isolation level after the rollback.
    pub chain: bool,
}

/// `SAVEPOINT <name>` statement.
///
/// Creates a named savepoint within the current transaction.
/// Savepoints work at the innermost open transaction level.
#[derive(Debug, Clone, PartialEq)]
pub struct SavepointStatement {
    pub name: String,
}

/// `RELEASE SAVEPOINT <name>` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct ReleaseSavepointStatement {
    pub name: String,
}

// ── User management scaffolding ────────────────────────────────────────────

/// A `CREATE USER` statement (scaffolding; not yet executed).
#[derive(Debug, Clone, PartialEq)]
pub struct CreateUserStatement {
    /// User name.
    pub name: String,
    /// Optional password hash / authentication string.
    pub password: Option<String>,
    /// Optional list of roles to grant immediately.
    pub roles: Vec<String>,
}

/// A `DROP USER` statement (scaffolding; not yet executed).
#[derive(Debug, Clone, PartialEq)]
pub struct DropUserStatement {
    pub names: Vec<String>,
    pub if_exists: bool,
}

/// A `CREATE TYPE` (composite/row user-defined type) statement.
///
/// Used for composite types only.  To create an enumeration, use
/// [`CreateEnumStatement`] via `CREATE ENUM`.
///
/// UTDs allow DBAs to define custom composite types with read/write/anonymization
/// restrictions that can be applied per user or group.  Like enums, composite
/// types cannot be dropped while any column references them.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTypeStatement {
    /// Type name (lowercased).
    pub name: String,
    /// Type body.
    pub definition: TypeDefinition,
}

/// A `DROP TYPE` statement for composite user-defined types.
///
/// Returns [`ValidationError::TypeInUse`] if any table column still
/// references this type.
#[derive(Debug, Clone, PartialEq)]
pub struct DropTypeStatement {
    /// Type name (lowercased).
    pub name: String,
    /// `IF EXISTS` flag — suppresses error if the type does not exist.
    pub if_exists: bool,
}

// ── Enum DDL ──────────────────────────────────────────────────────────────────

/// `CREATE ENUM [FLAG] name ('variant1', 'variant2', …)`
///
/// Defines a **named, reusable enumeration type** that can be referenced in
/// column definitions.  Users supply only the variant **names**; the system
/// auto-assigns the numeric values and stores them permanently:
///
/// - Regular enum: `0, 1, 2, …` in declaration order.
/// - FLAG enum: `NONE = 0` (if present), then `1, 2, 4, 8, …` for the rest.
///
/// Assigned values are **immutable** — they cannot be changed after creation
/// to prevent data loss.  The type cannot be dropped while any column
/// references it (see [`DropEnumStatement`]).
///
/// ```sql
/// -- Regular enum (system assigns 0, 1, 2)
/// CREATE ENUM status ('active', 'inactive', 'pending')
///
/// -- FLAG enum (system assigns none=0, read=1, write=2, admin=4)
/// CREATE ENUM FLAG permissions (NONE, 'read', 'write', 'admin')
///
/// -- Reference in a table
/// CREATE TABLE users (
///     id    INTEGER,
///     state status,
///     perms permissions
/// )
/// ```
///
/// Parsed from `CREATE TYPE name AS ENUM [FLAG] (...)` when the underlying
/// SQL parser is used; AeternumDB's native `CREATE ENUM` keyword is a
/// Phase 4 grammar extension.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateEnumStatement {
    /// Enum type name (lowercased).
    pub name: String,
    /// `true` for a FLAG (bitmask) enum.
    pub flag: bool,
    /// Variant names in declaration order.
    /// Users supply names only; numeric values are assigned by the system.
    pub variants: Vec<EnumVariant>,
    /// `IF NOT EXISTS` flag.
    pub if_not_exists: bool,
}

/// `DROP ENUM [IF EXISTS] name`
///
/// Removes a named enum type from the catalog.  Fails with
/// [`ValidationError::TypeInUse`] if any table column still references
/// this enum.
#[derive(Debug, Clone, PartialEq)]
pub struct DropEnumStatement {
    /// Enum type name (lowercased).
    pub name: String,
    /// `IF EXISTS` flag — suppresses error if the enum does not exist.
    pub if_exists: bool,
}

// ── Database / Schema DDL scaffolding ─────────────────────────────────────────

/// A `CREATE DATABASE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateDatabaseStatement {
    /// Database name (lowercased).
    pub name: String,
    /// `IF NOT EXISTS` flag.
    pub if_not_exists: bool,
}

/// A `DROP DATABASE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DropDatabaseStatement {
    /// Database name (lowercased).
    pub name: String,
    /// `IF EXISTS` flag.
    pub if_exists: bool,
}

/// A `USE [DATABASE] db_name` statement — switches the active database for the
/// current connection.  Cross-database JOINs are not supported; all tables in
/// a query must belong to the same database.
#[derive(Debug, Clone, PartialEq)]
pub struct UseDatabaseStatement {
    /// Target database name (lowercased).
    pub name: String,
}

/// A `CREATE SCHEMA` statement.
///
/// Schemas group tables within a database.  The default application schema is
/// `app`.  Several schemas are reserved for system use (see the SQL reference).
#[derive(Debug, Clone, PartialEq)]
pub struct CreateSchemaStatement {
    /// Optional database qualifier (lowercased).
    pub database: Option<String>,
    /// Schema name (lowercased).
    pub name: String,
    /// `IF NOT EXISTS` flag.
    pub if_not_exists: bool,
}

/// A `DROP SCHEMA` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DropSchemaStatement {
    /// Optional database qualifier (lowercased).
    pub database: Option<String>,
    /// Schema name (lowercased).
    pub name: String,
    /// `IF EXISTS` flag.
    pub if_exists: bool,
}

// ── Top-level statement ────────────────────────────────────────────────────────

/// A fully lowered SQL statement ready for the query planner.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(Box<SelectStatement>),
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
    /// A materialized view definition.
    CreateMaterializedView(CreateMaterializedViewStatement),
    /// Transaction control statements.
    BeginTransaction(BeginTransactionStatement),
    Commit(CommitStatement),
    Rollback(RollbackStatement),
    Savepoint(SavepointStatement),
    ReleaseSavepoint(ReleaseSavepointStatement),
    /// `CREATE INDEX` statement.
    CreateIndex(CreateIndexStatement),
    /// `DROP INDEX` statement.
    DropIndex(DropIndexStatement),
    /// `CREATE USER` scaffolding.
    CreateUser(CreateUserStatement),
    /// `DROP USER` scaffolding.
    DropUser(DropUserStatement),
    /// `CREATE ENUM [FLAG] name (...)` — define a named enumeration type.
    CreateEnum(CreateEnumStatement),
    /// `DROP ENUM [IF EXISTS] name` — remove a named enumeration type.
    DropEnum(DropEnumStatement),
    /// `CREATE TYPE name AS (field type, …)` — composite user-defined type.
    CreateType(CreateTypeStatement),
    /// `DROP TYPE [IF EXISTS] name` — remove a composite user-defined type.
    DropType(DropTypeStatement),
    /// `CREATE DATABASE` statement.
    CreateDatabase(CreateDatabaseStatement),
    /// `DROP DATABASE` statement.
    DropDatabase(DropDatabaseStatement),
    /// `USE [DATABASE] db` — switch active database.
    UseDatabase(UseDatabaseStatement),
    /// `CREATE SCHEMA` statement.
    CreateSchema(CreateSchemaStatement),
    /// `DROP SCHEMA` statement.
    DropSchema(DropSchemaStatement),
}

// ── Conversion from sqlparser AST ─────────────────────────────────────────────

impl TryFrom<sp::Statement> for Statement {
    type Error = AstError;

    fn try_from(stmt: sp::Statement) -> Result<Self, Self::Error> {
        match stmt {
            sp::Statement::Query(q) => Ok(Statement::Select(Box::new(convert_query(*q)?))),
            sp::Statement::Insert(insert) => convert_insert(insert),
            sp::Statement::Update(update) => convert_update(update),
            sp::Statement::Delete(delete) => convert_delete(delete),
            sp::Statement::CreateTable(ct) => convert_create_table(ct),
            sp::Statement::CreateView(cv) if cv.materialized => {
                convert_create_materialized_view(cv)
            }
            sp::Statement::Drop {
                object_type,
                if_exists,
                names,
                ..
            } => convert_drop(object_type, if_exists, names),
            sp::Statement::AlterTable(alt) => convert_alter_table(alt),
            sp::Statement::Grant(grant) => convert_grant(grant),
            sp::Statement::Revoke(revoke) => convert_revoke(revoke),
            sp::Statement::StartTransaction { modes, .. } => {
                let isolation_level = modes.iter().find_map(|m| {
                    if let sp::TransactionMode::IsolationLevel(lvl) = m {
                        Some(match lvl {
                            sp::TransactionIsolationLevel::ReadUncommitted => {
                                IsolationLevel::ReadUncommitted
                            }
                            sp::TransactionIsolationLevel::ReadCommitted => {
                                IsolationLevel::ReadCommitted
                            }
                            sp::TransactionIsolationLevel::RepeatableRead => {
                                IsolationLevel::RepeatableRead
                            }
                            sp::TransactionIsolationLevel::Serializable
                            | sp::TransactionIsolationLevel::Snapshot => {
                                IsolationLevel::Serializable
                            }
                        })
                    } else {
                        None
                    }
                });
                let read_only = modes.iter().any(|m| {
                    matches!(
                        m,
                        sp::TransactionMode::AccessMode(sp::TransactionAccessMode::ReadOnly)
                    )
                });
                Ok(Statement::BeginTransaction(BeginTransactionStatement {
                    name: None, // named BEGIN requires AeternumDB custom grammar (Phase 4)
                    isolation_level,
                    read_only,
                }))
            }
            sp::Statement::Commit { chain, .. } => Ok(Statement::Commit(CommitStatement {
                scope: CommitScope::Current,
                chain,
            })),
            sp::Statement::Rollback { chain, savepoint } => {
                let scope = match savepoint {
                    Some(sp) => RollbackScope::ToSavepoint(sp.value.to_lowercase()),
                    None => RollbackScope::Current,
                };
                Ok(Statement::Rollback(RollbackStatement { scope, chain }))
            }
            sp::Statement::Savepoint { name } => Ok(Statement::Savepoint(SavepointStatement {
                name: ident_to_string(&name),
            })),
            sp::Statement::ReleaseSavepoint { name } => {
                Ok(Statement::ReleaseSavepoint(ReleaseSavepointStatement {
                    name: ident_to_string(&name),
                }))
            }
            sp::Statement::CreateIndex(ci) => {
                let name = ci.name.as_ref().map(object_name_to_string);
                let table = object_name_to_string(&ci.table_name);
                let columns = ci
                    .columns
                    .into_iter()
                    .map(|c| IndexColumn {
                        name: match &c.column.expr {
                            sp::Expr::Identifier(ident) => ident.value.clone(),
                            other => other.to_string(),
                        },
                        ascending: c.column.options.asc,
                    })
                    .collect();
                Ok(Statement::CreateIndex(CreateIndexStatement {
                    name,
                    table,
                    columns,
                    unique: ci.unique,
                    if_not_exists: ci.if_not_exists,
                    index_type: match ci.using {
                        Some(sp::IndexType::BTree) => IndexType::BTree,
                        Some(sp::IndexType::Hash) => IndexType::Hash,
                        Some(sp::IndexType::GIN) => IndexType::Gin,
                        Some(sp::IndexType::GiST) => IndexType::Gist,
                        Some(sp::IndexType::SPGiST) => IndexType::SpGist,
                        Some(sp::IndexType::BRIN) => IndexType::Brin,
                        Some(sp::IndexType::Bloom) => IndexType::Bloom,
                        Some(sp::IndexType::Custom(ident)) => {
                            let name_lc = ident.value.to_lowercase();
                            match name_lc.as_str() {
                                "fulltext" | "full_text" => IndexType::FullText,
                                "trigram" | "gin_trgm" | "gist_trgm" => IndexType::Trigram,
                                _ => IndexType::Other(ident.value.clone()),
                            }
                        }
                        None => IndexType::BTree,
                    },
                }))
            }
            sp::Statement::CreateUser(cu) => Ok(Statement::CreateUser(CreateUserStatement {
                name: cu.name.value.clone(),
                password: None,
                roles: vec![],
            })),
            sp::Statement::CreateType {
                name,
                representation,
            } => {
                let type_name = object_name_to_string(&name);
                match representation {
                    // `CREATE TYPE name AS ENUM ('a', 'b', 'c')` →
                    // mapped to `CreateEnum` (the canonical AeternumDB form).
                    // Users specify names only; system assigns numeric values.
                    Some(sp::UserDefinedTypeRepresentation::Enum { labels }) => {
                        let variants = labels
                            .into_iter()
                            .map(|lbl| {
                                let n = lbl.value.to_lowercase();
                                EnumVariant {
                                    is_none: n == "none",
                                    name: n,
                                }
                            })
                            .collect();
                        Ok(Statement::CreateEnum(CreateEnumStatement {
                            name: type_name,
                            flag: false,
                            variants,
                            if_not_exists: false,
                        }))
                    }
                    // `CREATE TYPE name AS (field type, …)` → composite type.
                    Some(sp::UserDefinedTypeRepresentation::Composite { attributes }) => {
                        let fields = attributes
                            .into_iter()
                            .map(|attr| {
                                let dt = convert_data_type(attr.data_type)?;
                                Ok(CompositeField {
                                    name: attr.name.value.to_lowercase(),
                                    data_type: dt,
                                    not_null: false,
                                })
                            })
                            .collect::<Result<Vec<_>, AstError>>()?;
                        Ok(Statement::CreateType(CreateTypeStatement {
                            name: type_name,
                            definition: TypeDefinition::Composite(fields),
                        }))
                    }
                    // Other representations (Range, etc.) — scaffold as empty composite.
                    _ => Ok(Statement::CreateType(CreateTypeStatement {
                        name: type_name,
                        definition: TypeDefinition::Composite(vec![]),
                    })),
                }
            }
            sp::Statement::CreateDatabase {
                db_name,
                if_not_exists,
                ..
            } => Ok(Statement::CreateDatabase(CreateDatabaseStatement {
                name: object_name_to_string(&db_name),
                if_not_exists,
            })),
            sp::Statement::CreateSchema {
                schema_name,
                if_not_exists,
                ..
            } => {
                let (database, name) = match schema_name {
                    sp::SchemaName::Simple(n) => {
                        let full = object_name_to_string(&n);
                        let mut parts = full.splitn(2, '.').collect::<Vec<_>>();
                        if parts.len() == 2 {
                            (Some(parts[0].to_string()), parts[1].to_string())
                        } else {
                            (None, parts.remove(0).to_string())
                        }
                    }
                    sp::SchemaName::UnnamedAuthorization(_)
                    | sp::SchemaName::NamedAuthorization(_, _) => {
                        return Err(AstError::Unsupported(
                            "AUTHORIZATION form of CREATE SCHEMA is not supported".to_string(),
                        ))
                    }
                };
                Ok(Statement::CreateSchema(CreateSchemaStatement {
                    database,
                    name,
                    if_not_exists,
                }))
            }
            sp::Statement::Use(use_expr) => {
                let name = match use_expr {
                    sp::Use::Database(n)
                    | sp::Use::Schema(n)
                    | sp::Use::Catalog(n)
                    | sp::Use::Object(n) => object_name_to_string(&n),
                    sp::Use::Default => "default".to_string(),
                    other => {
                        return Err(AstError::Unsupported(format!(
                            "USE variant not supported: {other}"
                        )))
                    }
                };
                Ok(Statement::UseDatabase(UseDatabaseStatement { name }))
            }
            other => Err(AstError::Unsupported(format!(
                "statement type not supported: {other}"
            ))),
        }
    }
}

// ── Internal conversion helpers ────────────────────────────────────────────────

fn ident_to_string(ident: &sp::Ident) -> String {
    ident.value.to_lowercase()
}

fn object_name_to_string(name: &sp::ObjectName) -> String {
    name.0
        .iter()
        .filter_map(|part| match part {
            sp::ObjectNamePart::Identifier(ident) => Some(ident.value.to_lowercase()),
            sp::ObjectNamePart::Function(_) => None,
        })
        .collect::<Vec<_>>()
        .join(".")
}

/// Decompose a possibly-qualified object name into `(database, schema, table)`.
///
/// | Parts | Result |
/// |-------|--------|
/// | `table` | `(None, None, "table")` |
/// | `schema.table` | `(None, Some("schema"), "table")` |
/// | `db.schema.table` | `(Some("db"), Some("schema"), "table")` |
fn parse_qualified_name(name: &sp::ObjectName) -> (Option<String>, Option<String>, String) {
    let parts: Vec<String> = name
        .0
        .iter()
        .filter_map(|part| match part {
            sp::ObjectNamePart::Identifier(ident) => Some(ident.value.to_lowercase()),
            sp::ObjectNamePart::Function(_) => None,
        })
        .collect();
    match parts.len() {
        3 => (
            Some(parts[0].clone()),
            Some(parts[1].clone()),
            parts[2].clone(),
        ),
        2 => (None, Some(parts[0].clone()), parts[1].clone()),
        _ => (None, None, parts.into_iter().last().unwrap_or_default()),
    }
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
        sp::SetExpr::Select(select) => convert_select(*select, query.order_by, query.limit_clause)?,
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
                filter_by: None,
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
        view_as: None, // VIEW AS is a Phase 4 custom-grammar extension
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
        sp::SelectItem::UnnamedExpr(e) => {
            // Map EXPAND(col) function calls to SelectItem::Expand
            if let Some(expand) = try_expand_function(&e, None) {
                return Ok(expand);
            }
            Ok(SelectItem::Expr {
                expr: convert_expr(e)?,
                alias: None,
            })
        }
        sp::SelectItem::ExprWithAlias { expr, alias } => {
            let alias_str = ident_to_string(&alias);
            // Map EXPAND(col) AS alias to SelectItem::Expand
            if let Some(expand) = try_expand_function(&expr, Some(alias_str.clone())) {
                return Ok(expand);
            }
            Ok(SelectItem::Expr {
                expr: convert_expr(expr)?,
                alias: Some(alias_str),
            })
        }
    }
}

/// Checks whether `expr` is a call to `EXPAND(single_arg)` and, if so,
/// returns the corresponding [`SelectItem::Expand`] node.
fn try_expand_function(expr: &sp::Expr, alias: Option<String>) -> Option<SelectItem> {
    if let sp::Expr::Function(f) = expr {
        let name = object_name_to_string(&f.name).to_uppercase();
        if name == "EXPAND" {
            if let sp::FunctionArguments::List(ref list) = f.args {
                if list.args.len() == 1 {
                    if let sp::FunctionArg::Unnamed(sp::FunctionArgExpr::Expr(inner)) =
                        list.args[0].clone()
                    {
                        if let Ok(inner_expr) = convert_expr(inner) {
                            return Some(SelectItem::Expand {
                                expr: Box::new(inner_expr),
                                alias,
                            });
                        }
                    }
                }
            }
        }
    }
    None
}

fn convert_table_with_joins(twj: sp::TableWithJoins) -> Result<TableReference, AstError> {
    let mut result = convert_table_factor(twj.relation)?;
    for join in twj.joins {
        let right = convert_table_factor(join.relation)?;
        let (join_type, filter_by) = convert_join_operator(join.join_operator)?;
        result = TableReference::Join {
            left: Box::new(result),
            right: Box::new(right),
            join_type,
            filter_by,
        };
    }
    Ok(result)
}

fn convert_table_factor(factor: sp::TableFactor) -> Result<TableReference, AstError> {
    match factor {
        sp::TableFactor::Table { name, alias, .. } => {
            let (database, schema, table_name) = parse_qualified_name(&name);
            Ok(TableReference::Named {
                database,
                schema,
                name: table_name,
                alias: alias.map(|a| ident_to_string(&a.name)),
            })
        }
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
        sp::JoinConstraint::Natural => Err(AstError::Unsupported(
            "NATURAL joins are not supported in the internal AST".to_string(),
        )),
        sp::JoinConstraint::Using(columns) => Err(AstError::Unsupported(format!(
            "USING joins are not supported in the internal AST: USING ({})",
            columns
                .into_iter()
                .map(|c| object_name_to_string(&c))
                .collect::<Vec<_>>()
                .join(", ")
        ))),
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
        sp::Expr::ILike {
            expr,
            negated,
            pattern,
            ..
        } => Ok(Expr::BinaryOp {
            left: Box::new(convert_expr(*expr)?),
            op: if negated {
                BinaryOperator::NotILike
            } else {
                BinaryOperator::ILike
            },
            right: Box::new(convert_expr(*pattern)?),
        }),
        sp::Expr::SimilarTo {
            expr,
            negated,
            pattern,
            ..
        } => Ok(Expr::BinaryOp {
            left: Box::new(convert_expr(*expr)?),
            op: if negated {
                BinaryOperator::NotSimilarTo
            } else {
                BinaryOperator::SimilarTo
            },
            right: Box::new(convert_expr(*pattern)?),
        }),
        sp::Expr::AnyOp {
            left,
            compare_op,
            right,
            ..
        } => Ok(Expr::ArrayOp {
            expr: Box::new(convert_expr(*left)?),
            op: convert_binary_op(compare_op)?,
            quantifier: ArrayQuantifier::Any,
            right: Box::new(convert_expr(*right)?),
        }),
        sp::Expr::AllOp {
            left,
            compare_op,
            right,
        } => Ok(Expr::ArrayOp {
            expr: Box::new(convert_expr(*left)?),
            op: convert_binary_op(compare_op)?,
            quantifier: ArrayQuantifier::All,
            right: Box::new(convert_expr(*right)?),
        }),
        sp::Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => Ok(Expr::Substring {
            expr: Box::new(convert_expr(*expr)?),
            from_pos: substring_from
                .map(|e| convert_expr(*e))
                .transpose()?
                .map(Box::new),
            len: substring_for
                .map(|e| convert_expr(*e))
                .transpose()?
                .map(Box::new),
        }),
        sp::Expr::Position { expr, r#in } => Ok(Expr::Position {
            substr: Box::new(convert_expr(*expr)?),
            in_expr: Box::new(convert_expr(*r#in)?),
        }),
        sp::Expr::Trim {
            expr,
            trim_where,
            trim_what,
            ..
        } => Ok(Expr::Trim {
            expr: Box::new(convert_expr(*expr)?),
            trim_where: trim_where.map(|tw| match tw {
                sp::TrimWhereField::Leading => TrimWhereField::Leading,
                sp::TrimWhereField::Trailing => TrimWhereField::Trailing,
                sp::TrimWhereField::Both => TrimWhereField::Both,
            }),
            trim_what: trim_what
                .map(|e| convert_expr(*e))
                .transpose()?
                .map(Box::new),
        }),
        sp::Expr::MatchAgainst {
            columns,
            match_value,
            opt_search_modifier,
        } => {
            let cols = columns
                .into_iter()
                .map(|c| object_name_to_string(&c))
                .collect();
            let pattern = convert_value(match_value)?;
            let modifier = opt_search_modifier.map(|m| match m {
                sp::SearchModifier::InNaturalLanguageMode => TextSearchModifier::NaturalLanguage,
                sp::SearchModifier::InNaturalLanguageModeWithQueryExpansion => {
                    TextSearchModifier::NaturalLanguageWithExpansion
                }
                sp::SearchModifier::InBooleanMode => TextSearchModifier::Boolean,
                sp::SearchModifier::WithQueryExpansion => TextSearchModifier::WithExpansion,
            });
            Ok(Expr::MatchAgainst {
                columns: cols,
                match_value: Box::new(Expr::Literal(pattern)),
                modifier,
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
        sp::BinaryOperator::StringConcat => Ok(BinaryOperator::StringConcat),
        // ── Bitwise ──────────────────────────────────────────────────────────
        sp::BinaryOperator::BitwiseAnd => Ok(BinaryOperator::BitwiseAnd),
        sp::BinaryOperator::BitwiseOr => Ok(BinaryOperator::BitwiseOr),
        sp::BinaryOperator::BitwiseXor | sp::BinaryOperator::PGBitwiseXor => {
            Ok(BinaryOperator::BitwiseXor)
        }
        sp::BinaryOperator::PGBitwiseShiftLeft => Ok(BinaryOperator::ShiftLeft),
        sp::BinaryOperator::PGBitwiseShiftRight => Ok(BinaryOperator::ShiftRight),
        // ── Regex ────────────────────────────────────────────────────────────
        // NOT REGEXP is expressed as UnaryOp(Not, BinaryOp(Regexp, …)) in sqlparser.
        sp::BinaryOperator::Regexp => Ok(BinaryOperator::Regexp),
        sp::BinaryOperator::PGRegexMatch => Ok(BinaryOperator::RegexpMatch),
        sp::BinaryOperator::PGRegexIMatch => Ok(BinaryOperator::RegexpIMatch),
        sp::BinaryOperator::PGRegexNotMatch => Ok(BinaryOperator::NotRegexpMatch),
        sp::BinaryOperator::PGRegexNotIMatch => Ok(BinaryOperator::NotRegexpIMatch),
        other => Err(AstError::Unsupported(format!(
            "binary operator not supported: {other}"
        ))),
    }
}

fn convert_unary_op(op: sp::UnaryOperator) -> Result<UnaryOperator, AstError> {
    match op {
        sp::UnaryOperator::Minus => Ok(UnaryOperator::Minus),
        sp::UnaryOperator::Not => Ok(UnaryOperator::Not),
        sp::UnaryOperator::BitwiseNot => Ok(UnaryOperator::BitwiseNot),
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
        // ── Signed integers ────────────────────────────────────────────────
        sp::DataType::TinyInt(_) => Ok(DataType::TinyInt),
        sp::DataType::SmallInt(_) | sp::DataType::Int2(_) => Ok(DataType::SmallInt),
        sp::DataType::MediumInt(_) => Ok(DataType::MediumInt),
        sp::DataType::Int(_) | sp::DataType::Integer(_) | sp::DataType::Int4(_) => {
            Ok(DataType::Integer)
        }
        sp::DataType::BigInt(_)
        | sp::DataType::Int8(_)
        | sp::DataType::Int64
        | sp::DataType::Int16
        | sp::DataType::Int32
        | sp::DataType::Int128
        | sp::DataType::Int256 => Ok(DataType::BigInt),
        // ── Unsigned integers ──────────────────────────────────────────────
        sp::DataType::TinyIntUnsigned(_) | sp::DataType::UTinyInt | sp::DataType::UInt8 => {
            Ok(DataType::UnsignedTinyInt)
        }
        sp::DataType::SmallIntUnsigned(_)
        | sp::DataType::USmallInt
        | sp::DataType::Int2Unsigned(_)
        | sp::DataType::UInt16 => Ok(DataType::UnsignedSmallInt),
        sp::DataType::MediumIntUnsigned(_) => Ok(DataType::UnsignedMediumInt),
        sp::DataType::IntUnsigned(_)
        | sp::DataType::Int4Unsigned(_)
        | sp::DataType::IntegerUnsigned(_)
        | sp::DataType::UnsignedInteger
        | sp::DataType::UInt32 => Ok(DataType::UnsignedInt),
        sp::DataType::BigIntUnsigned(_)
        | sp::DataType::UBigInt
        | sp::DataType::UInt64
        | sp::DataType::UInt128
        | sp::DataType::UInt256
        | sp::DataType::HugeInt
        | sp::DataType::UHugeInt => Ok(DataType::UnsignedBigInt),
        // ── Floating-point ─────────────────────────────────────────────────
        sp::DataType::Float(_) | sp::DataType::Float4 | sp::DataType::Real => Ok(DataType::Float),
        sp::DataType::FloatUnsigned(_) => Err(AstError::Invalid(
            "FLOAT UNSIGNED is not supported; use FLOAT with a CHECK constraint for non-negative values".to_string(),
        )),
        sp::DataType::Double(_)
        | sp::DataType::DoublePrecision
        | sp::DataType::Float8
        | sp::DataType::Float64 => Ok(DataType::Double),
        sp::DataType::DoubleUnsigned(_) => Err(AstError::Invalid(
            "DOUBLE UNSIGNED is not supported; use DOUBLE with a CHECK constraint for non-negative values".to_string(),
        )),
        // ── Decimal / numeric ──────────────────────────────────────────────
        sp::DataType::Decimal(info) | sp::DataType::Numeric(info) | sp::DataType::Dec(info) => {
            let (p, s) = exact_number_info(info);
            Ok(DataType::Decimal(p, s))
        }
        sp::DataType::DecimalUnsigned(_) | sp::DataType::DecUnsigned(_) => Err(AstError::Invalid(
            "DECIMAL UNSIGNED is not supported; use DECIMAL with a CHECK constraint for non-negative values".to_string(),
        )),
        // ── Character ──────────────────────────────────────────────────────
        sp::DataType::Varchar(n)
        | sp::DataType::CharVarying(n)
        | sp::DataType::CharacterVarying(n) => Ok(DataType::Varchar(char_length_to_u64(n))),
        sp::DataType::Char(n) | sp::DataType::Character(n) => {
            Ok(DataType::Char(char_length_to_u64(n)))
        }
        sp::DataType::Text => Ok(DataType::Varchar(None)),
        sp::DataType::TinyText => Ok(DataType::TinyText),
        sp::DataType::MediumText => Ok(DataType::MediumText),
        sp::DataType::LongText => Ok(DataType::LongText),
        // ── Binary ─────────────────────────────────────────────────────────
        sp::DataType::Binary(n) => Ok(DataType::Binary(n)),
        sp::DataType::Varbinary(n) => {
            let len = n.and_then(|bl| match bl {
                sp::BinaryLength::IntegerLength { length } => Some(length),
                sp::BinaryLength::Max => None,
            });
            Ok(DataType::Varbinary(len))
        }
        sp::DataType::Blob(n) => Ok(DataType::Blob(n)),
        sp::DataType::Bytes(n) => Ok(DataType::Blob(n)),
        sp::DataType::TinyBlob => Ok(DataType::TinyBlob),
        sp::DataType::MediumBlob => Ok(DataType::MediumBlob),
        sp::DataType::LongBlob => Ok(DataType::LongBlob),
        // ── Boolean ────────────────────────────────────────────────────────
        sp::DataType::Boolean | sp::DataType::Bool => Ok(DataType::Boolean),
        // ── Date/time ──────────────────────────────────────────────────────
        sp::DataType::Date => Ok(DataType::Date),
        sp::DataType::Datetime(_) => Ok(DataType::DateTime),
        sp::DataType::Timestamp(_, sp::TimezoneInfo::None)
        | sp::DataType::Timestamp(_, sp::TimezoneInfo::WithoutTimeZone) => Ok(DataType::Timestamp),
        sp::DataType::Timestamp(_, sp::TimezoneInfo::WithTimeZone)
        | sp::DataType::Timestamp(_, sp::TimezoneInfo::Tz) => Ok(DataType::TimestampTz),
        sp::DataType::Time(_, sp::TimezoneInfo::None)
        | sp::DataType::Time(_, sp::TimezoneInfo::WithoutTimeZone) => Ok(DataType::Time),
        sp::DataType::Time(_, sp::TimezoneInfo::WithTimeZone)
        | sp::DataType::Time(_, sp::TimezoneInfo::Tz) => Ok(DataType::TimeTz),
        // ── Misc ───────────────────────────────────────────────────────────
        sp::DataType::Uuid => Ok(DataType::Uuid),
        sp::DataType::Enum(members, _) => {
            let variants = members
                .into_iter()
                .map(|m| {
                    let name_str = match &m {
                        sp::EnumMember::Name(s) => s.to_lowercase(),
                        sp::EnumMember::NamedValue(s, _) => s.to_lowercase(),
                    };
                    // Users supply names only — system assigns values at creation time.
                    // Any explicit value from NamedValue syntax is silently ignored
                    // to enforce the "system-assigned, immutable" contract.
                    EnumVariant {
                        is_none: name_str == "none",
                        name: name_str,
                    }
                })
                .collect();
            Ok(DataType::Enum { variants, flag: false })
        }
        sp::DataType::Array(sp::ArrayElemTypeDef::AngleBracket(inner))
        | sp::DataType::Array(sp::ArrayElemTypeDef::SquareBracket(inner, _)) => {
            Ok(DataType::Vector(Box::new(convert_data_type(*inner)?)))
        }
        // Custom type name — treated as a reference to a user-defined enum or
        // composite type registered in the catalog.  The validator resolves the
        // type and enforces existence checks.
        sp::DataType::Custom(name, _) => Ok(DataType::EnumRef(object_name_to_string(&name))),
        other => Ok(DataType::Other(format!("{other}"))),
    }
}

fn exact_number_info(info: sp::ExactNumberInfo) -> (Option<u64>, Option<u64>) {
    match info {
        sp::ExactNumberInfo::None => (None, None),
        sp::ExactNumberInfo::Precision(p) => (Some(p), None),
        sp::ExactNumberInfo::PrecisionAndScale(p, s) => (Some(p), Some(s as u64)),
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
                sp::AssignmentTarget::Tuple(names) => {
                    return Err(AstError::Unsupported(format!(
                        "UPDATE tuple assignment target not supported: ({})",
                        names
                            .iter()
                            .map(object_name_to_string)
                            .collect::<Vec<_>>()
                            .join(", ")
                    )))
                }
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
    let (database, schema, table) = parse_qualified_name(&ct.name);
    let columns = ct
        .columns
        .into_iter()
        .map(convert_column_def)
        .collect::<Result<Vec<_>, _>>()?;
    let inherits = ct
        .inherits
        .unwrap_or_default()
        .into_iter()
        .map(|n| object_name_to_string(&n))
        .collect();
    let on_commit = ct.on_commit.map(|oc| match oc {
        sp::OnCommit::DeleteRows => OnCommitBehavior::DeleteRows,
        sp::OnCommit::PreserveRows => OnCommitBehavior::PreserveRows,
        sp::OnCommit::Drop => OnCommitBehavior::Drop,
    });
    let constraints = ct
        .constraints
        .into_iter()
        .map(convert_table_constraint)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Statement::CreateTable(CreateTableStatement {
        database,
        schema,
        table,
        columns,
        if_not_exists: ct.if_not_exists,
        temporary: ct.temporary,
        inherits,
        on_commit,
        constraints,
        versioned: false,
        flat: false,
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
    let mut check = None;

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
            sp::ColumnOption::Check(cc) => {
                check = Some(convert_expr(*cc.expr)?);
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
        check,
        text_directive: None,
        terms_directives: vec![],
    })
}

fn convert_table_constraint(c: sp::TableConstraint) -> Result<TableConstraint, AstError> {
    match c {
        sp::TableConstraint::PrimaryKey(pk) => {
            let name = pk.name.as_ref().map(ident_to_string);
            let columns = pk
                .columns
                .iter()
                .map(|c| match &c.column.expr {
                    sp::Expr::Identifier(ident) => ident.value.to_lowercase(),
                    other => other.to_string().to_lowercase(),
                })
                .collect();
            Ok(TableConstraint::PrimaryKey { name, columns })
        }
        sp::TableConstraint::Unique(u) => {
            let name = u.name.as_ref().map(ident_to_string);
            let columns = u
                .columns
                .iter()
                .map(|c| match &c.column.expr {
                    sp::Expr::Identifier(ident) => ident.value.to_lowercase(),
                    other => other.to_string().to_lowercase(),
                })
                .collect();
            Ok(TableConstraint::Unique { name, columns })
        }
        sp::TableConstraint::ForeignKey(_) => Err(AstError::Invalid(
            "FOREIGN KEY constraints are not supported in AeternumDB; \
             use reference column types (e.g. `col_name table_name`) to express \
             relationships — they are resolved via objid at execution time"
                .to_string(),
        )),
        sp::TableConstraint::Check(cc) => {
            let name = cc.name.as_ref().map(ident_to_string);
            Ok(TableConstraint::Check {
                name,
                expr: convert_expr(*cc.expr)?,
            })
        }
        other => Err(AstError::Unsupported(format!(
            "table constraint not supported: {other}"
        ))),
    }
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
        sp::ObjectType::Index => {
            let idx_names = names.iter().map(object_name_to_string).collect();
            Ok(Statement::DropIndex(DropIndexStatement {
                names: idx_names,
                if_exists,
            }))
        }
        sp::ObjectType::User => {
            let user_names = names.iter().map(object_name_to_string).collect();
            Ok(Statement::DropUser(DropUserStatement {
                names: user_names,
                if_exists,
            }))
        }
        sp::ObjectType::Database => {
            let name = names
                .iter()
                .map(object_name_to_string)
                .next()
                .unwrap_or_default();
            Ok(Statement::DropDatabase(DropDatabaseStatement {
                name,
                if_exists,
            }))
        }
        sp::ObjectType::Schema => {
            let (database, schema_name) = if let Some(n) = names.first() {
                let full = object_name_to_string(n);
                let mut parts = full.splitn(2, '.').collect::<Vec<_>>();
                if parts.len() == 2 {
                    (Some(parts[0].to_string()), parts[1].to_string())
                } else {
                    (None, parts.remove(0).to_string())
                }
            } else {
                (None, String::new())
            };
            Ok(Statement::DropSchema(DropSchemaStatement {
                database,
                name: schema_name,
                if_exists,
            }))
        }
        // `DROP TYPE name` — could be a composite type or an enum.
        // The validator resolves which catalog object to remove.
        // We map to `DropType`; `DropEnum` is produced only by the
        // AeternumDB-native `DROP ENUM` grammar (Phase 4).
        sp::ObjectType::Type => {
            let name = names
                .iter()
                .map(object_name_to_string)
                .next()
                .unwrap_or_default();
            Ok(Statement::DropType(DropTypeStatement { name, if_exists }))
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
            AlterTableOperation::AddColumn(Box::new(convert_column_def(column_def)?)),
        ),
        sp::AlterTableOperation::DropColumn {
            column_names,
            if_exists,
            ..
        } => {
            // The internal AST supports dropping exactly one column per
            // operation. Reject multi-column drops until the internal AST can
            // represent a list, rather than collapsing multiple names into one
            // ambiguous string.
            if column_names.len() != 1 {
                return Err(AstError::Unsupported(
                    "ALTER TABLE DROP COLUMN with multiple columns is not supported".into(),
                ));
            }

            let name = column_names
                .into_iter()
                .next()
                .unwrap()
                .value
                .to_lowercase();
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
        columns: vec![],
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
        columns: vec![],
        on,
        from,
    }))
}

// ── CREATE MATERIALIZED VIEW conversion ──────────────────────────────────────

fn convert_create_materialized_view(cv: sp::CreateView) -> Result<Statement, AstError> {
    let name = object_name_to_string(&cv.name);
    let query = Box::new(convert_query(*cv.query)?);
    Ok(Statement::CreateMaterializedView(
        CreateMaterializedViewStatement {
            name,
            query,
            if_not_exists: cv.if_not_exists,
            or_replace: cv.or_replace,
        },
    ))
}
