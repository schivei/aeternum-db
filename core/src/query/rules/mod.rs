//! Optimization rule trait and rule-application engine.
//!
//! Each optimization rule implements [`OptimizationRule`] and transforms a
//! [`LogicalPlan`] into an equivalent (but potentially cheaper) plan.  The
//! [`RuleEngine`] applies a set of rules in repeated passes until no further
//! changes are made or a maximum iteration count is reached.
//!
//! ## Implementing a rule
//!
//! ```rust
//! use aeternumdb_core::query::rules::OptimizationRule;
//! use aeternumdb_core::query::logical_plan::LogicalPlan;
//!
//! struct NoOp;
//! impl OptimizationRule for NoOp {
//!     fn name(&self) -> &str { "no_op" }
//!     fn apply(&self, plan: LogicalPlan) -> LogicalPlan { plan }
//! }
//! ```

pub mod pushdown;

use crate::query::logical_plan::LogicalPlan;

// ── OptimizationRule ──────────────────────────────────────────────────────────

/// A single logical plan transformation.
///
/// Rules are applied to a plan tree and must return an *equivalent* plan
/// (same semantics, potentially lower cost).
pub trait OptimizationRule: Send + Sync {
    /// A short human-readable name used in debug output and EXPLAIN.
    fn name(&self) -> &str;

    /// Apply the rule to the root of `plan`, returning the (possibly
    /// transformed) plan.
    ///
    /// Rules should be idempotent: applying a rule twice should produce the
    /// same result as applying it once.
    fn apply(&self, plan: LogicalPlan) -> LogicalPlan;
}

// ── RuleEngine ────────────────────────────────────────────────────────────────

/// Applies a sequence of [`OptimizationRule`]s in repeated passes.
///
/// The engine converges when a full pass produces no change, up to a
/// configurable maximum number of iterations.
pub struct RuleEngine {
    rules: Vec<Box<dyn OptimizationRule>>,
    /// Maximum number of passes before giving up.
    pub max_iterations: usize,
}

impl Default for RuleEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl RuleEngine {
    /// Create an engine with the default rule set and iteration limit.
    pub fn new() -> Self {
        Self {
            rules: Vec::new(),
            max_iterations: 20,
        }
    }

    /// Add a rule to the engine.
    pub fn add_rule(&mut self, rule: Box<dyn OptimizationRule>) -> &mut Self {
        self.rules.push(rule);
        self
    }

    /// Optimize `plan` by applying all rules until convergence.
    ///
    /// The rules are applied in the order they were added.  One complete
    /// sweep through all rules counts as one iteration.
    pub fn optimize(&self, mut plan: LogicalPlan) -> LogicalPlan {
        for _ in 0..self.max_iterations {
            let before = format!("{plan:?}");
            for rule in &self.rules {
                plan = rule.apply(plan);
            }
            let after = format!("{plan:?}");
            if before == after {
                break;
            }
        }
        plan
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::logical_plan::LogicalPlan;

    struct IdentityRule;
    impl OptimizationRule for IdentityRule {
        fn name(&self) -> &str {
            "identity"
        }
        fn apply(&self, plan: LogicalPlan) -> LogicalPlan {
            plan
        }
    }

    #[test]
    fn engine_with_no_rules_returns_plan_unchanged() {
        let engine = RuleEngine::new();
        let plan = LogicalPlan::Values { rows: vec![] };
        let result = engine.optimize(plan.clone());
        assert_eq!(format!("{result:?}"), format!("{plan:?}"));
    }

    #[test]
    fn engine_with_identity_rule() {
        let mut engine = RuleEngine::new();
        engine.add_rule(Box::new(IdentityRule));
        let plan = LogicalPlan::Values { rows: vec![] };
        let result = engine.optimize(plan);
        assert!(matches!(result, LogicalPlan::Values { .. }));
    }

    #[test]
    fn rule_name_accessible() {
        let r = IdentityRule;
        assert_eq!(r.name(), "identity");
    }
}
