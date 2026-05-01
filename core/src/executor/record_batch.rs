//! Record batch and row structures for query execution.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// A value in a row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    /// NULL value.
    Null,
    /// Boolean value.
    Boolean(bool),
    /// 64-bit integer.
    Integer(i64),
    /// 64-bit floating point.
    Float(f64),
    /// Decimal value.
    Decimal(rust_decimal::Decimal),
    /// UTF-8 string.
    String(String),
    /// Binary data.
    Bytes(Vec<u8>),
    /// Array of values.
    Array(Vec<Value>),
    /// JSON value.
    Json(serde_json::Value),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::Integer(i) => write!(f, "{}", i),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::Decimal(d) => write!(f, "{}", d),
            Value::String(s) => write!(f, "{}", s),
            Value::Bytes(b) => write!(f, "<{} bytes>", b.len()),
            Value::Array(a) => {
                write!(f, "[")?;
                for (i, v) in a.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", v)?;
                }
                write!(f, "]")
            }
            Value::Json(j) => write!(f, "{}", j),
        }
    }
}

impl Value {
    /// Returns true if the value is NULL.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Convert to boolean, propagating NULL.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Boolean(b) => Some(*b),
            Value::Null => None,
            _ => None,
        }
    }

    /// Convert to integer, propagating NULL.
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Value::Integer(i) => Some(*i),
            Value::Null => None,
            _ => None,
        }
    }

    /// Convert to float, propagating NULL.
    pub fn as_float(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            Value::Integer(i) => Some(*i as f64),
            Value::Null => None,
            _ => None,
        }
    }

    /// Convert to string, propagating NULL.
    pub fn as_string(&self) -> Option<String> {
        match self {
            Value::String(s) => Some(s.clone()),
            Value::Null => None,
            _ => Some(self.to_string()),
        }
    }

    /// Convert to array, propagating NULL.
    pub fn as_array(&self) -> Option<Vec<Value>> {
        match self {
            Value::Array(a) => Some(a.clone()),
            Value::Null => None,
            _ => None,
        }
    }
}

/// A single row of data with named columns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    /// Column name to value mapping.
    pub columns: HashMap<String, Value>,
}

impl Row {
    /// Create a new empty row.
    pub fn new() -> Self {
        Self {
            columns: HashMap::new(),
        }
    }

    /// Create a row from a column name-value pairs iterator.
    pub fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (String, Value)>,
    {
        Self {
            columns: iter.into_iter().collect(),
        }
    }

    /// Get a value by column name.
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.columns.get(name)
    }

    /// Insert or update a column value.
    pub fn insert(&mut self, name: String, value: Value) {
        self.columns.insert(name, value);
    }

    /// Merge another row's columns into this row.
    pub fn merge(&mut self, other: &Row) {
        for (k, v) in &other.columns {
            self.columns.insert(k.clone(), v.clone());
        }
    }

    /// Get all column names.
    pub fn column_names(&self) -> Vec<String> {
        self.columns.keys().cloned().collect()
    }
}

impl Default for Row {
    fn default() -> Self {
        Self::new()
    }
}

/// A batch of rows with a common schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordBatch {
    /// Column names (in order).
    pub schema: Vec<String>,
    /// Rows in this batch.
    pub rows: Vec<Row>,
}

impl RecordBatch {
    /// Create a new empty record batch with the given schema.
    pub fn new(schema: Vec<String>) -> Self {
        Self {
            schema,
            rows: Vec::new(),
        }
    }

    /// Create a record batch with schema and rows.
    pub fn with_rows(schema: Vec<String>, rows: Vec<Row>) -> Self {
        Self { schema, rows }
    }

    /// Add a row to this batch.
    pub fn add_row(&mut self, row: Row) {
        self.rows.push(row);
    }

    /// Get the number of rows in this batch.
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Get the number of columns.
    pub fn column_count(&self) -> usize {
        self.schema.len()
    }

    /// Check if the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

impl Default for RecordBatch {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}
