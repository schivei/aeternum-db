//! AeternumDB SQL dialect.
//!
//! This module defines the custom SQL dialect used by AeternumDB.  It extends
//! the generic ANSI/SQL-92 dialect with minor tweaks that match the engine's
//! conventions (e.g. double-quoted identifiers, backtick identifiers, nested
//! block comments).  All syntax supported by [`sqlparser::dialect::GenericDialect`]
//! is available.
//!
//! # Example
//! ```
//! use aeternumdb_core::sql::dialect::AeternumDialect;
//! use sqlparser::dialect::Dialect;
//!
//! let d = AeternumDialect::default();
//! assert!(d.is_identifier_start('_'));
//! assert!(d.is_identifier_start('a'));
//! ```

use sqlparser::dialect::Dialect;

/// The AeternumDB SQL dialect.
///
/// Thin wrapper around [`GenericDialect`](sqlparser::dialect::GenericDialect)
/// with the following customizations:
///
/// - Identifiers may start with `_` or any Unicode letter.
/// - Identifiers may contain `-` after the first character (e.g. `my-table`
///   is valid when quoted).
/// - Nested block comments (`/* /* */ */`) are supported.
/// - Double-quote (`"`) and backtick (`` ` ``) are identifier quoting characters.
/// - Backticks allow using SQL keywords as identifiers.
#[derive(Debug, Default)]
pub struct AeternumDialect;

impl Dialect for AeternumDialect {
    /// Identifiers may begin with an ASCII letter, Unicode letter, or `_`.
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch == '_'
    }

    /// Identifier continuation characters: alphanumeric, `_`, or `$`.
    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphanumeric() || ch == '_' || ch == '$'
    }

    /// Use double-quote for identifier quoting (`"column_name"`).
    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        Some('"')
    }

    /// Support `/* nested /* comments */ */` which aids in commenting-out
    /// blocks of SQL that themselves contain comments.
    fn supports_nested_comments(&self) -> bool {
        true
    }

    /// Allow GROUP BY expressions (not only column references/positions).
    fn supports_group_by_expr(&self) -> bool {
        true
    }

    /// Support backtick (`` ` ``) as an identifier quoting character in
    /// addition to the standard double-quote.
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"' || ch == '`'
    }

    /// Enable bitwise shift operators (`<<` and `>>`).
    fn supports_bitwise_shift_operators(&self) -> bool {
        true
    }

    /// Enable `MATCH (cols) AGAINST (expr [modifier])` full-text search
    /// expression syntax.
    fn supports_match_against(&self) -> bool {
        true
    }
}
