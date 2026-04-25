# PR 1.9: Data Types System

## 📋 Overview

**PR Number:** 1.9
**Phase:** 1 - Core Foundation
**Priority:** 🟡 High
**Estimated Effort:** 4 days
**Dependencies:** None

## 🎯 Objectives

Define a comprehensive type system supporting all SQL data types with proper type conversion, NULL handling, and binary encoding. This includes:

- Full SQL data type support (INT, DECIMAL, VARCHAR, TEXT, TIMESTAMP, JSON, BOOLEAN)
- Type checking and validation
- Type conversion and coercion rules
- Binary serialization and deserialization
- NULL value handling
- Comparison and arithmetic operators

## 📝 Detailed Prompt for Implementation

```
Implement a complete type system for AeternumDB with the following requirements:

1. **Data Types**
   - Boolean (1 byte)
   - Int32, Int64 (4, 8 bytes)
   - Decimal (arbitrary precision, enhance existing)
   - Varchar(n), Text (variable length strings)
   - Timestamp (microsecond precision)
   - Json (enhance existing)
   - Array types (future extension point)

2. **Type Operations**
   - Comparison operators (<, <=, =, >=, >, !=)
   - Arithmetic operators (+, -, *, /, %)
   - Logical operators (AND, OR, NOT)
   - String operations (concat, substring, etc.)
   - Type casting and coercion

3. **NULL Handling**
   - Three-valued logic (TRUE, FALSE, NULL)
   - NULL propagation in expressions
   - IS NULL, IS NOT NULL predicates
   - COALESCE function

4. **Binary Encoding**
   - Compact binary representation
   - Length-prefixed for variable types
   - Network byte order
   - Version compatibility

5. **Type System**
   - Type trait for all types
   - Type registry
   - Dynamic type resolution
   - Type validation

6. **Performance Requirements**
   - Type operations inline-able
   - Minimal overhead
   - Zero-copy where possible

Use Rust best practices, comprehensive error handling, and include detailed documentation.
```

## 🏗️ Files to Create

### Core Modules

1. **`core/src/types/mod.rs`**
   - Public API for type system
   - DataType enum
   - Value enum
   - Type trait

2. **`core/src/types/boolean.rs`**
   - Boolean type implementation

3. **`core/src/types/integer.rs`**
   - Int32, Int64 implementations

4. **`core/src/types/decimal.rs`**
   - Enhance existing decimal module
   - Arithmetic operations
   - Precision handling

5. **`core/src/types/string.rs`**
   - Varchar and Text implementations
   - String operations

6. **`core/src/types/timestamp.rs`**
   - Timestamp type
   - Date/time operations
   - Timezone support

7. **`core/src/types/json.rs`**
   - Enhance existing JSON module
   - JSON operations
   - Path expressions

8. **`core/src/types/conversion.rs`**
   - Type casting
   - Implicit conversions
   - Conversion rules

9. **`core/src/types/null.rs`**
   - NULL value handling
   - Three-valued logic

### Test Files

10. **`core/tests/types_tests.rs`**
    - Integration tests for types

## 🔧 Implementation Details

### DataType Enum

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Int32,
    Int64,
    Decimal { precision: u8, scale: u8 },
    Varchar { max_length: Option<usize> },
    Text,
    Timestamp,
    Json,
}

impl DataType {
    pub fn size(&self) -> Option<usize> {
        match self {
            DataType::Boolean => Some(1),
            DataType::Int32 => Some(4),
            DataType::Int64 => Some(8),
            DataType::Decimal { .. } => Some(16),
            DataType::Timestamp => Some(8),
            _ => None, // Variable length
        }
    }

    pub fn is_numeric(&self) -> bool {
        matches!(self, DataType::Int32 | DataType::Int64 | DataType::Decimal { .. })
    }

    pub fn is_string(&self) -> bool {
        matches!(self, DataType::Varchar { .. } | DataType::Text)
    }
}
```

### Value Enum

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Decimal(Decimal),
    Varchar(String),
    Text(String),
    Timestamp(Timestamp),
    Json(JsonValue),
}

impl Value {
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Null => DataType::Null,
            Value::Boolean(_) => DataType::Boolean,
            Value::Int32(_) => DataType::Int32,
            Value::Int64(_) => DataType::Int64,
            Value::Decimal(_) => DataType::Decimal { precision: 38, scale: 10 },
            Value::Varchar(s) => DataType::Varchar { max_length: Some(s.len()) },
            Value::Text(_) => DataType::Text,
            Value::Timestamp(_) => DataType::Timestamp,
            Value::Json(_) => DataType::Json,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        match self {
            Value::Null => Ok(vec![0]),
            Value::Boolean(b) => Ok(vec![1, *b as u8]),
            Value::Int32(i) => {
                let mut buf = vec![2];
                buf.extend_from_slice(&i.to_le_bytes());
                Ok(buf)
            }
            Value::Int64(i) => {
                let mut buf = vec![3];
                buf.extend_from_slice(&i.to_le_bytes());
                Ok(buf)
            }
            Value::Decimal(d) => {
                let mut buf = vec![4];
                buf.extend_from_slice(&d.to_bytes());
                Ok(buf)
            }
            Value::Varchar(s) | Value::Text(s) => {
                let bytes = s.as_bytes();
                let mut buf = vec![5];
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
                Ok(buf)
            }
            Value::Timestamp(ts) => {
                let mut buf = vec![6];
                buf.extend_from_slice(&ts.to_micros().to_le_bytes());
                Ok(buf)
            }
            Value::Json(j) => {
                let bytes = serde_json::to_vec(j)?;
                let mut buf = vec![7];
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(&bytes);
                Ok(buf)
            }
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() {
            return Err(Error::InvalidValue);
        }

        match bytes[0] {
            0 => Ok(Value::Null),
            1 => Ok(Value::Boolean(bytes[1] != 0)),
            2 => {
                let arr: [u8; 4] = bytes[1..5].try_into()?;
                Ok(Value::Int32(i32::from_le_bytes(arr)))
            }
            3 => {
                let arr: [u8; 8] = bytes[1..9].try_into()?;
                Ok(Value::Int64(i64::from_le_bytes(arr)))
            }
            // ... other types
            _ => Err(Error::InvalidValue),
        }
    }
}
```

### Type Conversion

```rust
pub trait TypeConversion {
    fn can_cast_to(&self, target: &DataType) -> bool;
    fn cast(&self, target: &DataType) -> Result<Value>;
}

impl TypeConversion for Value {
    fn can_cast_to(&self, target: &DataType) -> bool {
        match (self.data_type(), target) {
            (DataType::Int32, DataType::Int64) => true,
            (DataType::Int32, DataType::Decimal { .. }) => true,
            (DataType::Int64, DataType::Decimal { .. }) => true,
            (DataType::Varchar { .. }, DataType::Text) => true,
            (DataType::Text, DataType::Varchar { .. }) => true,
            _ => false,
        }
    }

    fn cast(&self, target: &DataType) -> Result<Value> {
        match (self, target) {
            (Value::Int32(i), DataType::Int64) => Ok(Value::Int64(*i as i64)),
            (Value::Int32(i), DataType::Decimal { .. }) => {
                Ok(Value::Decimal(Decimal::from(*i)))
            }
            (Value::Varchar(s), DataType::Text) => Ok(Value::Text(s.clone())),
            _ => Err(Error::TypeMismatch),
        }
    }
}
```

### Comparison Operations

```rust
impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => None,
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            (Value::Int32(a), Value::Int32(b)) => a.partial_cmp(b),
            (Value::Int64(a), Value::Int64(b)) => a.partial_cmp(b),
            (Value::Decimal(a), Value::Decimal(b)) => a.partial_cmp(b),
            (Value::Varchar(a), Value::Varchar(b)) => a.partial_cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.partial_cmp(b),
            _ => None, // Type mismatch
        }
    }
}

impl Value {
    pub fn add(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int32(a), Value::Int32(b)) => {
                Ok(Value::Int32(a.checked_add(*b).ok_or(Error::Overflow)?))
            }
            (Value::Int64(a), Value::Int64(b)) => {
                Ok(Value::Int64(a.checked_add(*b).ok_or(Error::Overflow)?))
            }
            (Value::Decimal(a), Value::Decimal(b)) => {
                Ok(Value::Decimal(a + b))
            }
            _ => Err(Error::TypeMismatch),
        }
    }

    // Similarly implement sub, mul, div, etc.
}
```

## ✅ Tests Required

### Unit Tests

1. **Boolean Tests**
   - ✅ TRUE, FALSE, NULL
   - ✅ Logical operations
   - ✅ Comparison

2. **Integer Tests**
   - ✅ Int32 and Int64
   - ✅ Arithmetic operations
   - ✅ Overflow handling
   - ✅ Comparison

3. **Decimal Tests**
   - ✅ Precision and scale
   - ✅ Arithmetic
   - ✅ Rounding

4. **String Tests**
   - ✅ Varchar and Text
   - ✅ Concatenation
   - ✅ Substring
   - ✅ Comparison

5. **Timestamp Tests**
   - ✅ Creation and formatting
   - ✅ Arithmetic
   - ✅ Timezone handling

6. **JSON Tests**
   - ✅ Parsing and serialization
   - ✅ Path expressions
   - ✅ Type conversion

7. **Conversion Tests**
   - ✅ All valid conversions
   - ✅ Invalid conversions
   - ✅ Implicit casting

8. **NULL Tests**
   - ✅ Three-valued logic
   - ✅ NULL propagation
   - ✅ IS NULL checks

### Integration Tests

9. **Type System Tests** (`types_tests.rs`)
   - ✅ All types roundtrip (serialize/deserialize)
   - ✅ Type validation
   - ✅ Expression evaluation
   - ✅ Mixed type operations

## 📊 Performance Targets

| Metric | Target | Measurement |
|--------|--------|-------------|
| Type operations | Inline-able | Compiler output |
| Serialization | >1M values/sec | Benchmark |
| Comparison | <10ns | Benchmark |
| Conversion overhead | <5% | Benchmark |

## 📚 Documentation Requirements

1. **Module Documentation** (in code)
   - Comprehensive rustdoc comments
   - Type conversion rules
   - Examples for each type

2. **Type System Guide** (`docs/types.md`)
   - Supported types
   - Type conversion rules
   - NULL handling
   - Performance considerations

## 🔍 Acceptance Criteria

### Functional Requirements
- [ ] All SQL types implemented
- [ ] Type conversions work
- [ ] NULL handling correct
- [ ] Binary encoding efficient
- [ ] All operations supported

### Quality Requirements
- [ ] All tests pass
- [ ] Code coverage >85%
- [ ] Documentation complete

### Performance Requirements
- [ ] Operations are fast
- [ ] Minimal memory overhead
- [ ] Efficient serialization

## 🔗 Dependencies

This PR depends on:
- Existing `core/src/decimal.rs` (enhance)
- Existing `core/src/json2.rs` (enhance)

This PR is required by:
- **PR 1.10**: Tuple & Record Format
- **PR 1.5**: Query Executor (uses types)

## 📦 Dependencies to Add

```toml
[dependencies]
chrono = "0.4"  # For Timestamp handling
rust_decimal = "1.33"  # For Decimal type
```

## 🚀 Implementation Steps

### Day 1: Core Type System
- Define DataType and Value enums
- Implement Boolean, Int32, Int64
- Write tests

### Day 2: Decimal, String, Timestamp
- Enhance Decimal type
- Implement Varchar and Text
- Implement Timestamp
- Write tests

### Day 3: Type Operations
- Implement comparison
- Implement arithmetic
- Implement conversion
- Write tests

### Day 4: Integration & Documentation
- Binary encoding
- NULL handling
- Integration tests
- Write type system guide

## 🐛 Known Edge Cases to Handle

1. **Overflow in arithmetic**: Check and error
2. **Invalid conversions**: Clear error messages
3. **NULL in expressions**: Propagate correctly
4. **String encoding**: UTF-8 validation
5. **Timestamp precision**: Microseconds
6. **Decimal overflow**: Handle gracefully

## 💡 Future Enhancements (Out of Scope)

- Array types → Phase 3
- UUID type → Phase 3
- Geospatial types → Phase 5
- Binary/Blob types → Phase 3

## 🏁 Definition of Done

This PR is complete when:
1. All code implemented and tested
2. All acceptance criteria met
3. CI/CD passes
4. Documentation complete
5. Performance targets met

---

**Ready to implement?** Use this document as your complete specification. Good luck! 🚀
