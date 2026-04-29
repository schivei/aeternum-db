//! Cost model for query plan operators.
//!
//! The cost model translates operator characteristics and table statistics
//! into a scalar cost value used by the optimizer to compare alternative
//! physical plans.  Costs are dimensionless but follow the convention that
//! *higher cost = slower execution*.
//!
//! ## Cost components
//!
//! | Component | Factor constant | When charged |
//! |-----------|----------------|--------------|
//! | I/O | [`CostModel::io_cost_factor`] | Page reads / writes |
//! | CPU | [`CostModel::cpu_cost_factor`] | Row-level comparisons / projections |
//! | Network | [`CostModel::network_cost_factor`] | Reserved for distributed plans (future) |
//!
//! ## Selectivity defaults
//!
//! When column statistics are unavailable the model uses 10 % selectivity
//! for equality predicates and 30 % for range predicates.

use crate::query::statistics::TableStats;

// ── CostModel ─────────────────────────────────────────────────────────────────

/// Cost estimation parameters.
///
/// Construct with [`CostModel::default()`] for reasonable defaults, or
/// override individual factors when tuning for a specific workload.
#[derive(Debug, Clone)]
pub struct CostModel {
    /// Relative cost of reading one storage page.
    pub io_cost_factor: f64,
    /// Relative cost of processing one row.
    pub cpu_cost_factor: f64,
    /// Relative cost of sending one row over the network (future use).
    pub network_cost_factor: f64,
}

impl Default for CostModel {
    fn default() -> Self {
        Self {
            io_cost_factor: 1.0,
            cpu_cost_factor: 0.01,
            network_cost_factor: 10.0,
        }
    }
}

impl CostModel {
    /// Create a `CostModel` with the given factor values.
    pub fn new(io_cost_factor: f64, cpu_cost_factor: f64, network_cost_factor: f64) -> Self {
        Self {
            io_cost_factor,
            cpu_cost_factor,
            network_cost_factor,
        }
    }

    // ── Operator estimates ────────────────────────────────────────────────

    /// Estimate the cost of a sequential scan over `stats`.
    ///
    /// ```
    /// use aeternumdb_core::query::cost_model::CostModel;
    /// use aeternumdb_core::query::statistics::TableStats;
    ///
    /// let model = CostModel::default();
    /// let mut stats = TableStats::new("users");
    /// stats.num_pages = 100;
    /// stats.num_rows  = 10_000;
    ///
    /// let cost = model.estimate_scan_cost(&stats);
    /// assert!(cost > 0.0);
    /// ```
    pub fn estimate_scan_cost(&self, stats: &TableStats) -> f64 {
        let io_cost = stats.num_pages as f64 * self.io_cost_factor;
        let cpu_cost = stats.num_rows as f64 * self.cpu_cost_factor;
        io_cost + cpu_cost
    }

    /// Estimate the cost of applying a filter that passes `selectivity`
    /// fraction of the rows produced by `input_rows`.
    ///
    /// Returns the I/O cost (inherited from the scan below) plus a CPU term
    /// for the predicate evaluation.
    pub fn estimate_filter_cost(&self, input_rows: usize, selectivity: f64) -> f64 {
        let sel = selectivity.clamp(0.0, 1.0);
        input_rows as f64 * self.cpu_cost_factor * (1.0 + sel)
    }

    /// Estimate the cost of a nested-loop join.
    ///
    /// Complexity is O(left × right).
    pub fn estimate_nested_loop_cost(&self, left_rows: usize, right_rows: usize) -> f64 {
        (left_rows as f64) * (right_rows as f64) * self.cpu_cost_factor
    }

    /// Estimate the cost of a hash join.
    ///
    /// The hash-build phase charges 1.5 × CPU per row; the probe phase
    /// charges 1 × CPU per row.
    pub fn estimate_hash_join_cost(&self, left_rows: usize, right_rows: usize) -> f64 {
        let build = left_rows as f64 * self.cpu_cost_factor * 1.5;
        let probe = right_rows as f64 * self.cpu_cost_factor;
        build + probe
    }

    /// Estimate the cost of an in-memory or external sort.
    ///
    /// Uses an O(n log₂ n) approximation.
    pub fn estimate_sort_cost(&self, num_rows: usize) -> f64 {
        if num_rows <= 1 {
            return 0.0;
        }
        let n = num_rows as f64;
        n * n.log2() * self.cpu_cost_factor
    }

    /// Estimate the cost of a hash aggregate.
    ///
    /// Charges one pass over the input plus a small factor for hash
    /// maintenance per group.
    pub fn estimate_aggregate_cost(&self, input_rows: usize, num_groups: usize) -> f64 {
        let input_cost = input_rows as f64 * self.cpu_cost_factor;
        let group_cost = num_groups as f64 * self.cpu_cost_factor * 2.0;
        input_cost + group_cost
    }

    /// Estimate the output cardinality after applying `selectivity` to
    /// `input_rows`.
    ///
    /// Always returns at least 1 to avoid downstream divide-by-zero.
    pub fn estimated_rows(input_rows: usize, selectivity: f64) -> usize {
        let sel = selectivity.clamp(0.0, 1.0);
        let rows = (input_rows as f64 * sel).ceil() as usize;
        rows.max(1)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn model() -> CostModel {
        CostModel::default()
    }

    fn stats(rows: usize, pages: usize) -> TableStats {
        let mut s = TableStats::new("t");
        s.num_rows = rows;
        s.num_pages = pages;
        s
    }

    #[test]
    fn scan_cost_positive() {
        let cost = model().estimate_scan_cost(&stats(1000, 10));
        assert!(cost > 0.0);
    }

    #[test]
    fn scan_cost_larger_table_higher_cost() {
        let m = model();
        assert!(m.estimate_scan_cost(&stats(10_000, 100)) > m.estimate_scan_cost(&stats(1000, 10)));
    }

    #[test]
    fn filter_cost_zero_selectivity() {
        let cost = model().estimate_filter_cost(1000, 0.0);
        assert!(cost >= 0.0);
    }

    #[test]
    fn hash_join_cheaper_than_nested_loop_for_large_tables() {
        let m = model();
        let nl = m.estimate_nested_loop_cost(1000, 1000);
        let hj = m.estimate_hash_join_cost(1000, 1000);
        assert!(hj < nl);
    }

    #[test]
    fn sort_cost_monotone() {
        let m = model();
        assert!(m.estimate_sort_cost(1000) > m.estimate_sort_cost(100));
    }

    #[test]
    fn sort_cost_zero_or_one_row() {
        let m = model();
        assert_eq!(m.estimate_sort_cost(0), 0.0);
        assert_eq!(m.estimate_sort_cost(1), 0.0);
    }

    #[test]
    fn aggregate_cost_positive() {
        let m = model();
        assert!(m.estimate_aggregate_cost(1000, 50) > 0.0);
    }

    #[test]
    fn estimated_rows_at_least_one() {
        assert_eq!(CostModel::estimated_rows(0, 0.0), 1);
        assert_eq!(CostModel::estimated_rows(100, 0.0), 1);
    }

    #[test]
    fn estimated_rows_full_selectivity() {
        assert_eq!(CostModel::estimated_rows(100, 1.0), 100);
    }
}
