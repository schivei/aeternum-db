//! Hash aggregate executor with accumulator implementations.

use super::record_batch::{RecordBatch, Row, Value};
use super::{ExecutionContext, ExecutionPlan, Result};
use crate::query::logical_plan::AggregateExpr;
use crate::sql::ast::Expr;
use async_stream::stream;
use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use hashbrown::{HashMap, HashSet};
use std::sync::Arc;

/// Hash aggregate executor.
pub struct HashAggregateExec {
    /// Input execution plan.
    pub input: Arc<dyn ExecutionPlan>,
    /// GROUP BY expressions.
    pub group_by: Vec<Expr>,
    /// Aggregate expressions.
    pub aggregates: Vec<AggregateExpr>,
    /// HAVING predicate.
    pub having: Option<Expr>,
}

impl HashAggregateExec {
    /// Create a new hash aggregate executor.
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        group_by: Vec<Expr>,
        aggregates: Vec<AggregateExpr>,
        having: Option<Expr>,
    ) -> Self {
        Self {
            input,
            group_by,
            aggregates,
            having,
        }
    }
}

fn extract_agg_func_name(expr: &Expr) -> String {
    match expr {
        Expr::Function { name, .. } => name.to_uppercase(),
        Expr::Wildcard => "COUNT".to_string(),
        _ => "COUNT".to_string(),
    }
}

fn eval_agg_arg(expr: &Expr, row: &Row) -> Result<Value> {
    if let Expr::Function { ref args, .. } = expr {
        args.first()
            .map(|a| super::expressions::eval_expr(a, row))
            .transpose()
            .map(|v| v.unwrap_or(Value::Null))
    } else {
        super::expressions::eval_expr(expr, row)
    }
}

#[async_trait]
impl ExecutionPlan for HashAggregateExec {
    async fn execute(
        &self,
        ctx: &ExecutionContext,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let mut input_stream = self.input.execute(ctx).await?;
        let group_by = self.group_by.clone();
        let aggregates = self.aggregates.clone();
        let having = self.having.clone();

        let stream = stream! {
            let mut groups: HashMap<String, (Row, Vec<Box<dyn Accumulator>>)> = HashMap::new();

            while let Some(batch_result) = input_stream.next().await {
                let batch = batch_result?;

                for row in batch.rows {
                    let mut group_key = String::new();
                    let mut group_vals = Row::new();

                    for (i, group_expr) in group_by.iter().enumerate() {
                        let val = super::expressions::eval_expr(group_expr, &row)?;
                        group_key.push_str(&format!("{:?}", val));
                        group_key.push('|');
                        group_vals.insert(format!("group_{}", i), val);
                    }

                    let entry = groups.entry(group_key).or_insert_with(|| {
                        let accs: Vec<Box<dyn Accumulator>> = aggregates
                            .iter()
                            .map(|agg| create_accumulator(&extract_agg_func_name(&agg.func)))
                            .collect();
                        (group_vals.clone(), accs)
                    });

                    for (agg_expr, acc) in aggregates.iter().zip(entry.1.iter_mut()) {
                        let func_name = extract_agg_func_name(&agg_expr.func);
                        if func_name == "COUNT" && matches!(agg_expr.func, Expr::Wildcard) {
                            acc.accumulate(Value::Integer(1));
                        } else {
                            acc.accumulate(eval_agg_arg(&agg_expr.func, &row)?);
                        }
                    }
                }
            }

            let mut output_schema: Vec<String> = group_by.iter().enumerate()
                .map(|(i, _)| format!("group_{}", i))
                .collect();
            for agg in &aggregates {
                output_schema.push(agg.alias.clone().unwrap_or_else(|| "agg".to_string()));
            }

            let mut output_batch = RecordBatch::new(output_schema);

            for (_key, (group_row, accs)) in groups {
                let mut output_row = group_row.clone();

                for (agg_expr, acc) in aggregates.iter().zip(accs.iter()) {
                    let agg_val = acc.finalize();
                    let alias = agg_expr.alias.clone().unwrap_or_else(|| "agg".to_string());
                    output_row.insert(alias, agg_val);
                }

                if let Some(ref having_expr) = having {
                    let passes = super::expressions::eval_expr(having_expr, &output_row)?;
                    if let Some(true) = passes.as_bool() {
                        output_batch.add_row(output_row);
                    }
                } else {
                    output_batch.add_row(output_row);
                }
            }

            yield Ok(output_batch);
        };

        Ok(Box::pin(stream))
    }

    fn schema(&self) -> Vec<(String, String)> {
        let mut schema: Vec<(String, String)> = self
            .group_by
            .iter()
            .enumerate()
            .map(|(i, _)| (format!("group_{}", i), "unknown".to_string()))
            .collect();

        for agg in &self.aggregates {
            schema.push((
                agg.alias.clone().unwrap_or_else(|| "agg".to_string()),
                "unknown".to_string(),
            ));
        }

        schema
    }
}

/// Trait for aggregate accumulators.
trait Accumulator: Send {
    /// Accumulate a value.
    fn accumulate(&mut self, value: Value);
    /// Finalize and return the aggregate result.
    fn finalize(&self) -> Value;
}

fn create_accumulator(function: &str) -> Box<dyn Accumulator> {
    match function.to_uppercase().as_str() {
        "COUNT" => Box::new(CountAccumulator::new()),
        "COUNT_DISTINCT" => Box::new(CountDistinctAccumulator::new()),
        "SUM" => Box::new(SumAccumulator::new()),
        "AVG" => Box::new(AvgAccumulator::new()),
        "MIN" => Box::new(MinAccumulator::new()),
        "MAX" => Box::new(MaxAccumulator::new()),
        _ => Box::new(CountAccumulator::new()),
    }
}

struct CountAccumulator {
    count: i64,
}

impl CountAccumulator {
    fn new() -> Self {
        Self { count: 0 }
    }
}

impl Accumulator for CountAccumulator {
    fn accumulate(&mut self, value: Value) {
        if !value.is_null() {
            self.count += 1;
        }
    }

    fn finalize(&self) -> Value {
        Value::Integer(self.count)
    }
}

struct CountDistinctAccumulator {
    seen: HashSet<String>,
}

impl CountDistinctAccumulator {
    fn new() -> Self {
        Self {
            seen: HashSet::new(),
        }
    }
}

impl Accumulator for CountDistinctAccumulator {
    fn accumulate(&mut self, value: Value) {
        if !value.is_null() {
            self.seen.insert(format!("{:?}", value));
        }
    }

    fn finalize(&self) -> Value {
        Value::Integer(self.seen.len() as i64)
    }
}

struct SumAccumulator {
    sum: Option<f64>,
}

impl SumAccumulator {
    fn new() -> Self {
        Self { sum: None }
    }
}

impl Accumulator for SumAccumulator {
    fn accumulate(&mut self, value: Value) {
        if !value.is_null() {
            let num = match value {
                Value::Integer(i) => i as f64,
                Value::Float(f) => f,
                _ => return,
            };
            self.sum = Some(self.sum.unwrap_or(0.0) + num);
        }
    }

    fn finalize(&self) -> Value {
        self.sum.map(Value::Float).unwrap_or(Value::Null)
    }
}

struct AvgAccumulator {
    sum: f64,
    count: i64,
}

impl AvgAccumulator {
    fn new() -> Self {
        Self { sum: 0.0, count: 0 }
    }
}

impl Accumulator for AvgAccumulator {
    fn accumulate(&mut self, value: Value) {
        if !value.is_null() {
            let num = match value {
                Value::Integer(i) => i as f64,
                Value::Float(f) => f,
                _ => return,
            };
            self.sum += num;
            self.count += 1;
        }
    }

    fn finalize(&self) -> Value {
        if self.count > 0 {
            Value::Float(self.sum / self.count as f64)
        } else {
            Value::Null
        }
    }
}

struct MinAccumulator {
    min: Option<Value>,
}

impl MinAccumulator {
    fn new() -> Self {
        Self { min: None }
    }
}

impl Accumulator for MinAccumulator {
    fn accumulate(&mut self, value: Value) {
        if !value.is_null() {
            self.min = Some(match &self.min {
                None => value,
                Some(current) => {
                    if compare_values(&value, current) {
                        value
                    } else {
                        current.clone()
                    }
                }
            });
        }
    }

    fn finalize(&self) -> Value {
        self.min.clone().unwrap_or(Value::Null)
    }
}

struct MaxAccumulator {
    max: Option<Value>,
}

impl MaxAccumulator {
    fn new() -> Self {
        Self { max: None }
    }
}

impl Accumulator for MaxAccumulator {
    fn accumulate(&mut self, value: Value) {
        if !value.is_null() {
            self.max = Some(match &self.max {
                None => value,
                Some(current) => {
                    if compare_values(current, &value) {
                        value
                    } else {
                        current.clone()
                    }
                }
            });
        }
    }

    fn finalize(&self) -> Value {
        self.max.clone().unwrap_or(Value::Null)
    }
}

fn compare_values(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Integer(x), Value::Integer(y)) => x < y,
        (Value::Float(x), Value::Float(y)) => x < y,
        (Value::Integer(x), Value::Float(y)) => (*x as f64) < *y,
        (Value::Float(x), Value::Integer(y)) => *x < (*y as f64),
        (Value::String(x), Value::String(y)) => x < y,
        _ => false,
    }
}
