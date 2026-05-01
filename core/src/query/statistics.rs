//! Table and column statistics used by the query cost model.
//!
//! Statistics are gathered from the storage engine and cached in the
//! [`PlannerContext`](super::PlannerContext).  When no statistics are
//! available, all estimate functions fall back to safe defaults so that
//! planning never fails due to missing data.
//!
//! # Design notes
//!
//! - [`TableStats`] is intentionally inexpensive to clone so it can be
//!   embedded in plan nodes without heap pressure.
//! - Histograms use equal-width buckets for simplicity; a future PR can
//!   replace them with equal-depth or compressed-histogram variants.

use std::collections::HashMap;

use crate::sql::ast::Value;

// ── Histogram ─────────────────────────────────────────────────────────────────

/// A single bucket in a column histogram.
#[derive(Debug, Clone)]
pub struct HistogramBucket {
    /// Inclusive lower bound of the bucket.
    pub lower_bound: Value,
    /// Exclusive upper bound of the bucket.
    pub upper_bound: Value,
    /// Number of rows whose value falls within this bucket.
    pub count: usize,
}

/// An equal-width histogram for a column.
///
/// Used to estimate selectivity of range predicates.  When the histogram is
/// absent the cost model defaults to a 10 % selectivity estimate.
#[derive(Debug, Clone)]
pub struct Histogram {
    /// Ordered list of buckets (non-overlapping, ascending).
    pub buckets: Vec<HistogramBucket>,
}

impl Histogram {
    /// Create a new histogram from pre-computed buckets.
    pub fn new(buckets: Vec<HistogramBucket>) -> Self {
        Self { buckets }
    }

    /// Estimate the fraction of rows falling in the range `[low, high]`.
    ///
    /// Returns a value in `[0.0, 1.0]`.  When the histogram has no buckets
    /// the method returns `0.1` (10 % default).
    pub fn selectivity_range(&self, low: &Value, high: &Value) -> f64 {
        if self.buckets.is_empty() {
            return 0.1;
        }
        let total: usize = self.buckets.iter().map(|b| b.count).sum();
        if total == 0 {
            return 0.1;
        }
        let matching: usize = self
            .buckets
            .iter()
            .filter(|b| value_lte(&b.lower_bound, high) && value_lt(low, &b.upper_bound))
            .map(|b| b.count)
            .sum();
        (matching as f64) / (total as f64)
    }
}

/// Returns `true` when `a <= b` under a numeric/lexicographic ordering.
///
/// Only `Integer` and `String` variants are compared for now; all other
/// pairs fall back to `true` (permissive).
fn value_lte(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Integer(x), Value::Integer(y)) => x <= y,
        (Value::Float(x), Value::Float(y)) => x <= y,
        (Value::String(x), Value::String(y)) => x <= y,
        _ => true,
    }
}

/// Returns `true` when `a < b` under a numeric/lexicographic ordering.
///
/// Used to check against the *exclusive* upper bound of histogram buckets.
/// Only `Integer`, `Float`, and `String` variants are compared for now; all
/// other pairs fall back to `true` (permissive).
fn value_lt(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Integer(x), Value::Integer(y)) => x < y,
        (Value::Float(x), Value::Float(y)) => x < y,
        (Value::String(x), Value::String(y)) => x < y,
        _ => true,
    }
}

// ── ColumnStats ───────────────────────────────────────────────────────────────

/// Per-column statistics used for selectivity estimation.
#[derive(Debug, Clone)]
pub struct ColumnStats {
    /// Column name (lowercased).
    pub column_name: String,
    /// Number of distinct non-null values.
    pub num_distinct: usize,
    /// Number of NULL values in the column.
    pub num_nulls: usize,
    /// Minimum observed value, if known.
    pub min_value: Option<Value>,
    /// Maximum observed value, if known.
    pub max_value: Option<Value>,
    /// Optional histogram for range-query selectivity.
    pub histogram: Option<Histogram>,
    /// For array-typed and indexed columns: the total inner element count
    /// tracked across all rows.  `None` for regular scalar columns.
    pub inner_count: Option<usize>,
}

impl ColumnStats {
    /// Estimate selectivity for an equality predicate (`col = val`).
    ///
    /// Uses `1 / num_distinct` when possible; falls back to `0.1`.
    pub fn selectivity_eq(&self) -> f64 {
        if self.num_distinct > 0 {
            1.0 / self.num_distinct as f64
        } else {
            0.1
        }
    }
}

// ── TableStats ────────────────────────────────────────────────────────────────

/// Aggregate statistics for a single table.
#[derive(Debug, Clone)]
pub struct TableStats {
    /// Table name (lowercased).
    pub table_name: String,
    /// Estimated number of rows.
    pub num_rows: usize,
    /// Estimated number of storage pages.
    pub num_pages: usize,
    /// Estimated average row size in bytes.
    pub avg_row_size: usize,
    /// Per-column statistics, keyed by lowercased column name.
    pub column_stats: HashMap<String, ColumnStats>,
    /// Whether this table was created as a FLAT table.
    pub is_flat: bool,
}

impl TableStats {
    /// Construct statistics with sensible defaults.
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            num_rows: 1000,
            num_pages: 10,
            avg_row_size: 128,
            column_stats: HashMap::new(),
            is_flat: false,
        }
    }

    /// Look up column statistics (case-insensitive).
    pub fn column(&self, name: &str) -> Option<&ColumnStats> {
        self.column_stats.get(&name.to_lowercase())
    }
}

// ── StatisticsRegistry ────────────────────────────────────────────────────────

/// A simple in-memory registry that maps table names to their statistics.
///
/// The registry is populated by the DDL executor (or by direct injection in
/// tests) before planning begins.  When a table has no registered statistics
/// [`StatisticsRegistry::get`] returns a default [`TableStats`] rather than
/// failing.
#[derive(Debug, Default)]
pub struct StatisticsRegistry {
    tables: HashMap<String, TableStats>,
}

impl StatisticsRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register statistics for a table.
    pub fn add(&mut self, stats: TableStats) {
        self.tables.insert(stats.table_name.to_lowercase(), stats);
    }

    /// Retrieve statistics for `table_name`.
    ///
    /// Returns a default [`TableStats`] when no entry has been registered.
    pub fn get(&self, table_name: &str) -> TableStats {
        match self.tables.get(&table_name.to_lowercase()) {
            Some(s) => s.clone(),
            None => TableStats::new(table_name),
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_selectivity_range_empty() {
        let h = Histogram::new(vec![]);
        assert!((h.selectivity_range(&Value::Integer(0), &Value::Integer(100)) - 0.1).abs() < 1e-9);
    }

    #[test]
    fn histogram_selectivity_range_full() {
        let h = Histogram::new(vec![HistogramBucket {
            lower_bound: Value::Integer(0),
            upper_bound: Value::Integer(100),
            count: 50,
        }]);
        assert!((h.selectivity_range(&Value::Integer(0), &Value::Integer(100)) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn histogram_selectivity_range_exclusive_upper_bound() {
        // Bucket covers [0, 100).  A query with low == upper_bound (100) must
        // NOT match the bucket because upper_bound is exclusive.
        let h = Histogram::new(vec![HistogramBucket {
            lower_bound: Value::Integer(0),
            upper_bound: Value::Integer(100),
            count: 50,
        }]);
        // Querying range [100, 200] should match 0 rows from this bucket.
        let sel = h.selectivity_range(&Value::Integer(100), &Value::Integer(200));
        assert!(
            sel < 1e-9,
            "expected 0 selectivity when low == exclusive upper_bound, got {sel}"
        );
    }

    #[test]
    fn column_stats_selectivity_eq_fallback() {
        let cs = ColumnStats {
            column_name: "x".into(),
            num_distinct: 0,
            num_nulls: 0,
            min_value: None,
            max_value: None,
            histogram: None,
            inner_count: None,
        };
        assert!((cs.selectivity_eq() - 0.1).abs() < 1e-9);
    }

    #[test]
    fn column_stats_selectivity_eq_known() {
        let cs = ColumnStats {
            column_name: "x".into(),
            num_distinct: 100,
            num_nulls: 0,
            min_value: None,
            max_value: None,
            histogram: None,
            inner_count: None,
        };
        assert!((cs.selectivity_eq() - 0.01).abs() < 1e-9);
    }

    #[test]
    fn registry_default_stats() {
        let reg = StatisticsRegistry::new();
        let stats = reg.get("unknown_table");
        assert_eq!(stats.table_name, "unknown_table");
        assert_eq!(stats.num_rows, 1000);
    }

    #[test]
    fn registry_registered_stats() {
        let mut reg = StatisticsRegistry::new();
        let mut ts = TableStats::new("users");
        ts.num_rows = 5000;
        reg.add(ts);
        assert_eq!(reg.get("users").num_rows, 5000);
        assert_eq!(reg.get("USERS").num_rows, 5000);
    }

    #[test]
    fn column_stats_inner_count_none_for_scalars() {
        let cs = ColumnStats {
            column_name: "id".into(),
            num_distinct: 10,
            num_nulls: 0,
            min_value: None,
            max_value: None,
            histogram: None,
            inner_count: None,
        };
        assert!(cs.inner_count.is_none());
    }

    #[test]
    fn column_stats_inner_count_some_for_arrays() {
        let cs = ColumnStats {
            column_name: "tags".into(),
            num_distinct: 5,
            num_nulls: 0,
            min_value: None,
            max_value: None,
            histogram: None,
            inner_count: Some(42),
        };
        assert_eq!(cs.inner_count, Some(42));
    }
}
