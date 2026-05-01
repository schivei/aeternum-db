//! SQL expression evaluation with NULL propagation and core functions.
//!
//! This module evaluates SQL expressions against row data during query execution.

use super::record_batch::{Row, Value};
use super::{ExecutorError, Result};
use crate::sql::ast::{BinaryOperator, Expr, UnaryOperator};

/// Evaluate an expression against a row.
pub fn eval_expr(expr: &Expr, row: &Row) -> Result<Value> {
    match expr {
        Expr::Literal(val) => eval_literal(val),
        Expr::Column { table, name } => eval_column(table, name, row),
        Expr::BinaryOp { left, op, right } => eval_binary_op(left, op, right, row),
        Expr::UnaryOp { op, expr } => eval_unary_op(op, expr, row),
        Expr::Function { name, args, .. } => eval_function(name, args, row),
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => eval_case(operand, conditions, else_result, row),
        Expr::Cast { expr, data_type } => eval_cast(expr, data_type, row),
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => eval_between(expr, low, high, *negated, row),
        Expr::InList {
            expr,
            list,
            negated,
        } => eval_in_list(expr, list, *negated, row),
        _ => Err(ExecutorError::EvalError(format!(
            "Unsupported expression: {:?}",
            expr
        ))),
    }
}

fn eval_literal(lit: &crate::sql::ast::Value) -> Result<Value> {
    use crate::sql::ast::Value as AstValue;
    match lit {
        AstValue::Null => Ok(Value::Null),
        AstValue::Boolean(b) => Ok(Value::Boolean(*b)),
        AstValue::Integer(i) => Ok(Value::Integer(*i)),
        AstValue::Float(f) => Ok(Value::Float(*f)),
        AstValue::String(s) => Ok(Value::String(s.clone())),
    }
}

fn eval_column(table: &Option<String>, name: &str, row: &Row) -> Result<Value> {
    let col_name = if let Some(t) = table {
        format!("{}.{}", t, name)
    } else {
        name.to_string()
    };

    if let Some(val) = row.get(&col_name) {
        return Ok(val.clone());
    }

    if table.is_none() {
        if let Some(val) = row.get(name) {
            return Ok(val.clone());
        }
    }

    Err(ExecutorError::ColumnNotFound(col_name))
}

fn eval_binary_op(left: &Expr, op: &BinaryOperator, right: &Expr, row: &Row) -> Result<Value> {
    use BinaryOperator::*;

    match op {
        And => eval_and(left, right, row),
        Or => eval_or(left, right, row),
        _ => {
            let left_val = eval_expr(left, row)?;
            let right_val = eval_expr(right, row)?;

            if left_val.is_null() || right_val.is_null() {
                return Ok(Value::Null);
            }

            match op {
                Plus => eval_add(&left_val, &right_val),
                Minus => eval_sub(&left_val, &right_val),
                Multiply => eval_mul(&left_val, &right_val),
                Divide => eval_div(&left_val, &right_val),
                Modulo => eval_mod(&left_val, &right_val),
                Eq => Ok(Value::Boolean(
                    compare_values(&left_val, &right_val)? == std::cmp::Ordering::Equal,
                )),
                NotEq => Ok(Value::Boolean(
                    compare_values(&left_val, &right_val)? != std::cmp::Ordering::Equal,
                )),
                Lt => Ok(Value::Boolean(
                    compare_values(&left_val, &right_val)? == std::cmp::Ordering::Less,
                )),
                LtEq => Ok(Value::Boolean(matches!(
                    compare_values(&left_val, &right_val)?,
                    std::cmp::Ordering::Less | std::cmp::Ordering::Equal
                ))),
                Gt => Ok(Value::Boolean(
                    compare_values(&left_val, &right_val)? == std::cmp::Ordering::Greater,
                )),
                GtEq => Ok(Value::Boolean(matches!(
                    compare_values(&left_val, &right_val)?,
                    std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
                ))),
                StringConcat => eval_concat(&left_val, &right_val),
                Like | NotLike | ILike | NotILike => eval_like_op(op, &left_val, &right_val),
                _ => Err(ExecutorError::EvalError(format!(
                    "Unsupported binary operator: {:?}",
                    op
                ))),
            }
        }
    }
}

fn eval_like_op(op: &BinaryOperator, left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::String(s), Value::String(pattern)) => {
            let case_insensitive = matches!(op, BinaryOperator::ILike | BinaryOperator::NotILike);
            let negated = matches!(op, BinaryOperator::NotLike | BinaryOperator::NotILike);

            let regex_pattern = like_to_regex(pattern);
            let regex = if case_insensitive {
                regex::RegexBuilder::new(&regex_pattern)
                    .case_insensitive(true)
                    .build()
            } else {
                regex::Regex::new(&regex_pattern)
            };

            match regex {
                Ok(r) => {
                    let matches = r.is_match(s);
                    Ok(Value::Boolean(if negated { !matches } else { matches }))
                }
                Err(_) => Err(ExecutorError::EvalError(format!(
                    "Invalid LIKE pattern: {}",
                    pattern
                ))),
            }
        }
        _ => Err(ExecutorError::TypeMismatch {
            expected: "string".to_string(),
            got: "non-string".to_string(),
        }),
    }
}

fn like_to_regex(pattern: &str) -> String {
    let mut regex = String::from("^");
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '%' => regex.push_str(".*"),
            '_' => regex.push('.'),
            '\\' => {
                if let Some(next) = chars.next() {
                    regex.push_str(&regex::escape(&next.to_string()));
                }
            }
            _ => regex.push_str(&regex::escape(&c.to_string())),
        }
    }
    regex.push('$');
    regex
}

fn eval_and(left: &Expr, right: &Expr, row: &Row) -> Result<Value> {
    let left_val = eval_expr(left, row)?;
    if let Some(false) = left_val.as_bool() {
        return Ok(Value::Boolean(false));
    }
    let right_val = eval_expr(right, row)?;
    match (left_val.as_bool(), right_val.as_bool()) {
        (Some(true), Some(true)) => Ok(Value::Boolean(true)),
        (Some(false), _) | (_, Some(false)) => Ok(Value::Boolean(false)),
        _ => Ok(Value::Null),
    }
}

fn eval_or(left: &Expr, right: &Expr, row: &Row) -> Result<Value> {
    let left_val = eval_expr(left, row)?;
    if let Some(true) = left_val.as_bool() {
        return Ok(Value::Boolean(true));
    }
    let right_val = eval_expr(right, row)?;
    match (left_val.as_bool(), right_val.as_bool()) {
        (Some(true), _) | (_, Some(true)) => Ok(Value::Boolean(true)),
        (Some(false), Some(false)) => Ok(Value::Boolean(false)),
        _ => Ok(Value::Null),
    }
}

fn eval_add(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a + b)),
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
        (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 + b)),
        (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a + *b as f64)),
        _ => Err(ExecutorError::TypeMismatch {
            expected: "numeric".to_string(),
            got: format!("{:?}, {:?}", left, right),
        }),
    }
}

fn eval_sub(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a - b)),
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a - b)),
        (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 - b)),
        (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a - *b as f64)),
        _ => Err(ExecutorError::TypeMismatch {
            expected: "numeric".to_string(),
            got: format!("{:?}, {:?}", left, right),
        }),
    }
}

fn eval_mul(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a * b)),
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a * b)),
        (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 * b)),
        (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a * *b as f64)),
        _ => Err(ExecutorError::TypeMismatch {
            expected: "numeric".to_string(),
            got: format!("{:?}, {:?}", left, right),
        }),
    }
}

fn eval_div(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => {
            if *b == 0 {
                return Err(ExecutorError::EvalError("Division by zero".to_string()));
            }
            Ok(Value::Integer(a / b))
        }
        (Value::Float(a), Value::Float(b)) => {
            if *b == 0.0 {
                return Err(ExecutorError::EvalError("Division by zero".to_string()));
            }
            Ok(Value::Float(a / b))
        }
        (Value::Integer(a), Value::Float(b)) => {
            if *b == 0.0 {
                return Err(ExecutorError::EvalError("Division by zero".to_string()));
            }
            Ok(Value::Float(*a as f64 / b))
        }
        (Value::Float(a), Value::Integer(b)) => {
            if *b == 0 {
                return Err(ExecutorError::EvalError("Division by zero".to_string()));
            }
            Ok(Value::Float(a / *b as f64))
        }
        _ => Err(ExecutorError::TypeMismatch {
            expected: "numeric".to_string(),
            got: format!("{:?}, {:?}", left, right),
        }),
    }
}

fn eval_mod(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => {
            if *b == 0 {
                return Err(ExecutorError::EvalError("Modulo by zero".to_string()));
            }
            Ok(Value::Integer(a % b))
        }
        _ => Err(ExecutorError::TypeMismatch {
            expected: "integer".to_string(),
            got: format!("{:?}, {:?}", left, right),
        }),
    }
}

fn eval_concat(left: &Value, right: &Value) -> Result<Value> {
    let left_str = left.as_string().unwrap_or_else(|| left.to_string());
    let right_str = right.as_string().unwrap_or_else(|| right.to_string());
    Ok(Value::String(format!("{}{}", left_str, right_str)))
}

fn compare_values(left: &Value, right: &Value) -> Result<std::cmp::Ordering> {
    use std::cmp::Ordering;
    match (left, right) {
        (Value::Null, Value::Null) => Ok(Ordering::Equal),
        (Value::Boolean(a), Value::Boolean(b)) => Ok(a.cmp(b)),
        (Value::Integer(a), Value::Integer(b)) => Ok(a.cmp(b)),
        (Value::Float(a), Value::Float(b)) => Ok(a.partial_cmp(b).unwrap_or(Ordering::Equal)),
        (Value::Integer(a), Value::Float(b)) => {
            Ok((*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal))
        }
        (Value::Float(a), Value::Integer(b)) => {
            Ok(a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal))
        }
        (Value::String(a), Value::String(b)) => Ok(a.cmp(b)),
        _ => Err(ExecutorError::TypeMismatch {
            expected: "comparable types".to_string(),
            got: format!("{:?}, {:?}", left, right),
        }),
    }
}

fn eval_unary_op(op: &UnaryOperator, expr: &Expr, row: &Row) -> Result<Value> {
    let val = eval_expr(expr, row)?;
    if val.is_null() {
        return Ok(Value::Null);
    }

    use UnaryOperator::*;
    match op {
        Not => match val.as_bool() {
            Some(b) => Ok(Value::Boolean(!b)),
            None => Err(ExecutorError::TypeMismatch {
                expected: "boolean".to_string(),
                got: format!("{:?}", val),
            }),
        },
        Minus => match val {
            Value::Integer(i) => Ok(Value::Integer(-i)),
            Value::Float(f) => Ok(Value::Float(-f)),
            _ => Err(ExecutorError::TypeMismatch {
                expected: "numeric".to_string(),
                got: format!("{:?}", val),
            }),
        },
        Plus => Ok(val),
    }
}

fn eval_function(name: &str, args: &[Expr], row: &Row) -> Result<Value> {
    let arg_vals: Vec<Value> = args
        .iter()
        .map(|a| eval_expr(a, row))
        .collect::<Result<Vec<_>>>()?;

    match name.to_uppercase().as_str() {
        "COALESCE" => eval_coalesce(&arg_vals),
        "ABS" => eval_abs(&arg_vals),
        "LOWER" => eval_lower(&arg_vals),
        "UPPER" => eval_upper(&arg_vals),
        "LENGTH" => eval_length(&arg_vals),
        "TRIM" => eval_trim(&arg_vals),
        "ROUND" => eval_round(&arg_vals),
        "FLOOR" => eval_floor(&arg_vals),
        "CEIL" | "CEILING" => eval_ceil(&arg_vals),
        "SQRT" => eval_sqrt(&arg_vals),
        "COUNT" => Ok(Value::Integer(arg_vals.len() as i64)),
        _ => Err(ExecutorError::EvalError(format!(
            "Unknown function: {}",
            name
        ))),
    }
}

fn eval_coalesce(args: &[Value]) -> Result<Value> {
    for val in args {
        if !val.is_null() {
            return Ok(val.clone());
        }
    }
    Ok(Value::Null)
}

fn eval_abs(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(ExecutorError::EvalError(
            "ABS requires 1 argument".to_string(),
        ));
    }
    if args[0].is_null() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Integer(i) => Ok(Value::Integer(i.abs())),
        Value::Float(f) => Ok(Value::Float(f.abs())),
        _ => Err(ExecutorError::TypeMismatch {
            expected: "numeric".to_string(),
            got: format!("{:?}", args[0]),
        }),
    }
}

fn eval_lower(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(ExecutorError::EvalError(
            "LOWER requires 1 argument".to_string(),
        ));
    }
    if args[0].is_null() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::String(s) => Ok(Value::String(s.to_lowercase())),
        _ => Err(ExecutorError::TypeMismatch {
            expected: "string".to_string(),
            got: format!("{:?}", args[0]),
        }),
    }
}

fn eval_upper(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(ExecutorError::EvalError(
            "UPPER requires 1 argument".to_string(),
        ));
    }
    if args[0].is_null() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::String(s) => Ok(Value::String(s.to_uppercase())),
        _ => Err(ExecutorError::TypeMismatch {
            expected: "string".to_string(),
            got: format!("{:?}", args[0]),
        }),
    }
}

fn eval_length(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(ExecutorError::EvalError(
            "LENGTH requires 1 argument".to_string(),
        ));
    }
    if args[0].is_null() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::String(s) => Ok(Value::Integer(s.len() as i64)),
        _ => Err(ExecutorError::TypeMismatch {
            expected: "string".to_string(),
            got: format!("{:?}", args[0]),
        }),
    }
}

fn eval_trim(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(ExecutorError::EvalError(
            "TRIM requires 1 argument".to_string(),
        ));
    }
    if args[0].is_null() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::String(s) => Ok(Value::String(s.trim().to_string())),
        _ => Err(ExecutorError::TypeMismatch {
            expected: "string".to_string(),
            got: format!("{:?}", args[0]),
        }),
    }
}

fn eval_round(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(ExecutorError::EvalError(
            "ROUND requires 1-2 arguments".to_string(),
        ));
    }
    if args[0].is_null() {
        return Ok(Value::Null);
    }
    let decimals = if args.len() > 1 {
        args[1].as_integer().unwrap_or(0)
    } else {
        0
    };
    match &args[0] {
        Value::Float(f) => {
            let multiplier = 10f64.powi(decimals as i32);
            Ok(Value::Float((f * multiplier).round() / multiplier))
        }
        Value::Integer(i) => Ok(Value::Integer(*i)),
        _ => Err(ExecutorError::TypeMismatch {
            expected: "numeric".to_string(),
            got: format!("{:?}", args[0]),
        }),
    }
}

fn eval_floor(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(ExecutorError::EvalError(
            "FLOOR requires 1 argument".to_string(),
        ));
    }
    if args[0].is_null() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Float(f) => Ok(Value::Integer(f.floor() as i64)),
        Value::Integer(i) => Ok(Value::Integer(*i)),
        _ => Err(ExecutorError::TypeMismatch {
            expected: "numeric".to_string(),
            got: format!("{:?}", args[0]),
        }),
    }
}

fn eval_ceil(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(ExecutorError::EvalError(
            "CEIL requires 1 argument".to_string(),
        ));
    }
    if args[0].is_null() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Float(f) => Ok(Value::Integer(f.ceil() as i64)),
        Value::Integer(i) => Ok(Value::Integer(*i)),
        _ => Err(ExecutorError::TypeMismatch {
            expected: "numeric".to_string(),
            got: format!("{:?}", args[0]),
        }),
    }
}

fn eval_sqrt(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(ExecutorError::EvalError(
            "SQRT requires 1 argument".to_string(),
        ));
    }
    if args[0].is_null() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Float(f) => Ok(Value::Float(f.sqrt())),
        Value::Integer(i) => Ok(Value::Float((*i as f64).sqrt())),
        _ => Err(ExecutorError::TypeMismatch {
            expected: "numeric".to_string(),
            got: format!("{:?}", args[0]),
        }),
    }
}

fn eval_case(
    operand: &Option<Box<Expr>>,
    conditions: &[(Expr, Expr)],
    else_result: &Option<Box<Expr>>,
    row: &Row,
) -> Result<Value> {
    if let Some(op) = operand {
        let op_val = eval_expr(op, row)?;
        for (cond, result) in conditions {
            let cond_val = eval_expr(cond, row)?;
            if !op_val.is_null() && !cond_val.is_null() {
                if compare_values(&op_val, &cond_val)? == std::cmp::Ordering::Equal {
                    return eval_expr(result, row);
                }
            }
        }
    } else {
        for (cond, result) in conditions {
            let cond_val = eval_expr(cond, row)?;
            if let Some(true) = cond_val.as_bool() {
                return eval_expr(result, row);
            }
        }
    }

    if let Some(else_expr) = else_result {
        eval_expr(else_expr, row)
    } else {
        Ok(Value::Null)
    }
}

fn eval_cast(expr: &Expr, data_type: &crate::sql::ast::DataType, row: &Row) -> Result<Value> {
    let val = eval_expr(expr, row)?;
    if val.is_null() {
        return Ok(Value::Null);
    }

    use crate::sql::ast::DataType;
    match data_type {
        DataType::Boolean => match val {
            Value::Boolean(_) => Ok(val),
            Value::Integer(i) => Ok(Value::Boolean(i != 0)),
            Value::String(s) => {
                let lower = s.to_lowercase();
                Ok(Value::Boolean(
                    lower == "true" || lower == "t" || lower == "1",
                ))
            }
            _ => Err(ExecutorError::TypeMismatch {
                expected: "boolean".to_string(),
                got: format!("{:?}", val),
            }),
        },
        DataType::Integer | DataType::BigInt | DataType::SmallInt => match val {
            Value::Integer(_) => Ok(val),
            Value::Float(f) => Ok(Value::Integer(f as i64)),
            Value::String(s) => s
                .parse::<i64>()
                .map(Value::Integer)
                .map_err(|_| ExecutorError::EvalError(format!("Cannot cast '{}' to integer", s))),
            _ => Err(ExecutorError::TypeMismatch {
                expected: "integer".to_string(),
                got: format!("{:?}", val),
            }),
        },
        DataType::Float | DataType::Double => match val {
            Value::Float(_) => Ok(val),
            Value::Integer(i) => Ok(Value::Float(i as f64)),
            Value::String(s) => s
                .parse::<f64>()
                .map(Value::Float)
                .map_err(|_| ExecutorError::EvalError(format!("Cannot cast '{}' to float", s))),
            _ => Err(ExecutorError::TypeMismatch {
                expected: "float".to_string(),
                got: format!("{:?}", val),
            }),
        },
        DataType::Varchar(_) | DataType::Char(_) => Ok(Value::String(val.to_string())),
        _ => Err(ExecutorError::EvalError(format!(
            "Unsupported CAST target: {:?}",
            data_type
        ))),
    }
}

fn eval_between(expr: &Expr, low: &Expr, high: &Expr, negated: bool, row: &Row) -> Result<Value> {
    let val = eval_expr(expr, row)?;
    let low_val = eval_expr(low, row)?;
    let high_val = eval_expr(high, row)?;

    if val.is_null() || low_val.is_null() || high_val.is_null() {
        return Ok(Value::Null);
    }

    let cmp_low = compare_values(&val, &low_val)?;
    let cmp_high = compare_values(&val, &high_val)?;

    let is_between = matches!(
        cmp_low,
        std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
    ) && matches!(
        cmp_high,
        std::cmp::Ordering::Less | std::cmp::Ordering::Equal
    );

    Ok(Value::Boolean(if negated {
        !is_between
    } else {
        is_between
    }))
}

fn eval_in_list(expr: &Expr, list: &[Expr], negated: bool, row: &Row) -> Result<Value> {
    let val = eval_expr(expr, row)?;
    if val.is_null() {
        return Ok(Value::Null);
    }

    for item_expr in list {
        let item_val = eval_expr(item_expr, row)?;
        if !item_val.is_null() {
            if compare_values(&val, &item_val)? == std::cmp::Ordering::Equal {
                return Ok(Value::Boolean(!negated));
            }
        }
    }

    Ok(Value::Boolean(negated))
}
