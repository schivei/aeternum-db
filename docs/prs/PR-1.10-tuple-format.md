# PR 1.10: Tuple & Record Format

## 📋 Overview

**PR Number:** 1.10
**Phase:** 1 - Core Foundation
**Priority:** 🟡 High
**Estimated Effort:** 3 days
**Dependencies:** PR 1.9 (Data Types System)

## 🎯 Objectives

Define the on-disk and in-memory tuple format for storing table rows efficiently. This includes:

- Compact binary row format
- NULL bitmap for efficient NULL handling
- Variable-length field support
- Row header with metadata
- Forward/backward compatibility
- Efficient serialization and deserialization

## 📝 Detailed Prompt for Implementation

```
Implement a complete tuple storage format for AeternumDB with the following requirements:

1. **Row Format**
   - Row header: version, null bitmap, field count
   - Fixed-length fields stored inline
   - Variable-length fields with offset table
   - Space-efficient encoding

2. **NULL Bitmap**
   - Bit vector for NULL values
   - Efficient space usage (1 bit per nullable column)
   - Fast NULL checks

3. **Variable-Length Fields**
   - Offset table at end of row
   - Support fields >64KB
   - Overflow pages for very large fields

4. **Row Header**
   - Format version (1 byte)
   - Field count (2 bytes)
   - NULL bitmap size (2 bytes)
   - Total row size (4 bytes)

5. **Codec**
   - Encode row from values
   - Decode row to values
   - Schema evolution support
   - Validation

6. **Performance Requirements**
   - Encode/decode: >100K rows/sec
   - Space-efficient: <5% overhead
   - Support rows up to 1MB

Use Rust best practices, comprehensive error handling, and include detailed documentation.
```

## 🏗️ Files to Create

### Core Modules

1. **`core/src/tuple/mod.rs`**
   - Public API for tuple handling
   - Row structure

2. **`core/src/tuple/row.rs`**
   - Row implementation
   - Row operations

3. **`core/src/tuple/codec.rs`**
   - Encoder and decoder
   - Binary format handling

4. **`core/src/tuple/format.rs`**
   - Format specification
   - Version handling

### Test Files

5. **`core/tests/tuple_tests.rs`**
   - Integration tests

## 🔧 Implementation Details

### Row Structure

```rust
pub struct Row {
    header: RowHeader,
    data: Vec<u8>,
    schema: Arc<Schema>,
}

#[derive(Debug, Clone)]
pub struct RowHeader {
    pub format_version: u8,
    pub field_count: u16,
    pub null_bitmap_size: u16,
    pub total_size: u32,
}

impl Row {
    pub fn new(values: Vec<Value>, schema: Arc<Schema>) -> Result<Self> {
        let encoder = RowEncoder::new(schema.clone());
        let data = encoder.encode(&values)?;

        let header = RowHeader {
            format_version: 1,
            field_count: values.len() as u16,
            null_bitmap_size: (values.len() + 7) / 8,
            total_size: data.len() as u32,
        };

        Ok(Self {
            header,
            data,
            schema,
        })
    }

    pub fn get_value(&self, index: usize) -> Result<Value> {
        let decoder = RowDecoder::new(self.schema.clone());
        decoder.decode_field(&self.data, index)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Write header
        buf.push(self.header.format_version);
        buf.extend_from_slice(&self.header.field_count.to_le_bytes());
        buf.extend_from_slice(&self.header.null_bitmap_size.to_le_bytes());
        buf.extend_from_slice(&self.header.total_size.to_le_bytes());

        // Write data
        buf.extend_from_slice(&self.data);

        buf
    }

    pub fn from_bytes(bytes: &[u8], schema: Arc<Schema>) -> Result<Self> {
        // Parse header
        let format_version = bytes[0];
        let field_count = u16::from_le_bytes([bytes[1], bytes[2]]);
        let null_bitmap_size = u16::from_le_bytes([bytes[3], bytes[4]]);
        let total_size = u32::from_le_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]);

        let header = RowHeader {
            format_version,
            field_count,
            null_bitmap_size,
            total_size,
        };

        // Extract data
        let data = bytes[9..].to_vec();

        Ok(Self {
            header,
            data,
            schema,
        })
    }
}
```

### Row Encoder

```rust
pub struct RowEncoder {
    schema: Arc<Schema>,
}

impl RowEncoder {
    pub fn encode(&self, values: &[Value]) -> Result<Vec<u8>> {
        if values.len() != self.schema.columns.len() {
            return Err(Error::SchemaMismatch);
        }

        let mut buf = Vec::new();

        // 1. Write NULL bitmap
        let null_bitmap = self.create_null_bitmap(values);
        buf.extend_from_slice(&null_bitmap);

        // 2. Write fixed-length fields
        let mut var_len_data = Vec::new();
        let mut offsets = Vec::new();

        for (i, value) in values.iter().enumerate() {
            let col_type = &self.schema.columns[i].data_type;

            if value.is_null() {
                // NULL value - skip
                if col_type.is_fixed_length() {
                    buf.extend_from_slice(&vec![0; col_type.size().unwrap()]);
                } else {
                    offsets.push(0u32); // NULL offset
                }
            } else {
                match col_type {
                    DataType::Boolean | DataType::Int32 | DataType::Int64 => {
                        // Fixed-length: write inline
                        buf.extend_from_slice(&value.to_bytes()?);
                    }
                    DataType::Varchar { .. } | DataType::Text => {
                        // Variable-length: write to var_len_data
                        let offset = var_len_data.len() as u32;
                        offsets.push(offset);

                        let bytes = value.to_bytes()?;
                        var_len_data.extend_from_slice(&bytes);
                    }
                    _ => {}
                }
            }
        }

        // 3. Write variable-length data
        buf.extend_from_slice(&var_len_data);

        // 4. Write offset table
        for offset in offsets {
            buf.extend_from_slice(&offset.to_le_bytes());
        }

        Ok(buf)
    }

    fn create_null_bitmap(&self, values: &[Value]) -> Vec<u8> {
        let num_bytes = (values.len() + 7) / 8;
        let mut bitmap = vec![0u8; num_bytes];

        for (i, value) in values.iter().enumerate() {
            if value.is_null() {
                let byte_idx = i / 8;
                let bit_idx = i % 8;
                bitmap[byte_idx] |= 1 << bit_idx;
            }
        }

        bitmap
    }
}
```

### Row Decoder

```rust
pub struct RowDecoder {
    schema: Arc<Schema>,
}

impl RowDecoder {
    pub fn decode(&self, data: &[u8]) -> Result<Vec<Value>> {
        let mut values = Vec::new();
        let mut pos = 0;

        // 1. Read NULL bitmap
        let null_bitmap_size = (self.schema.columns.len() + 7) / 8;
        let null_bitmap = &data[pos..pos + null_bitmap_size];
        pos += null_bitmap_size;

        // 2. Read fields
        for (i, col) in self.schema.columns.iter().enumerate() {
            // Check if NULL
            let byte_idx = i / 8;
            let bit_idx = i % 8;
            let is_null = (null_bitmap[byte_idx] & (1 << bit_idx)) != 0;

            if is_null {
                values.push(Value::Null);
                if col.data_type.is_fixed_length() {
                    pos += col.data_type.size().unwrap();
                }
            } else {
                let value = match &col.data_type {
                    DataType::Boolean => {
                        let v = data[pos] != 0;
                        pos += 1;
                        Value::Boolean(v)
                    }
                    DataType::Int32 => {
                        let arr: [u8; 4] = data[pos..pos + 4].try_into()?;
                        pos += 4;
                        Value::Int32(i32::from_le_bytes(arr))
                    }
                    DataType::Int64 => {
                        let arr: [u8; 8] = data[pos..pos + 8].try_into()?;
                        pos += 8;
                        Value::Int64(i64::from_le_bytes(arr))
                    }
                    _ => Value::Null, // Handle other types
                };
                values.push(value);
            }
        }

        Ok(values)
    }

    pub fn decode_field(&self, data: &[u8], index: usize) -> Result<Value> {
        // Decode single field (optimization)
        // Similar to decode() but only extract one field
        todo!()
    }
}
```

## ✅ Tests Required

### Unit Tests

1. **Row Tests** (`row.rs`)
   - ✅ Create row
   - ✅ Get value by index
   - ✅ Serialize/deserialize
   - ✅ NULL values

2. **Encoder Tests** (`codec.rs`)
   - ✅ Encode all types
   - ✅ NULL bitmap
   - ✅ Variable-length fields
   - ✅ Large rows

3. **Decoder Tests** (`codec.rs`)
   - ✅ Decode all types
   - ✅ NULL handling
   - ✅ Field extraction
   - ✅ Error cases

### Integration Tests

4. **Tuple Tests** (`tuple_tests.rs`)
   - ✅ Roundtrip (encode/decode)
   - ✅ Schema evolution
   - ✅ Large rows (>64KB)
   - ✅ 10K rows

## 📊 Performance Targets

| Metric | Target | Measurement |
|--------|--------|-------------|
| Encode throughput | >100K rows/sec | Benchmark |
| Decode throughput | >100K rows/sec | Benchmark |
| Space overhead | <5% | Analysis |
| Max row size | 1MB | Test |

## 📚 Documentation Requirements

1. **Module Documentation** (in code)
   - Comprehensive rustdoc comments
   - Format specification

2. **Tuple Format Spec** (`docs/tuple-format.md`)
   - Binary format details
   - Diagrams
   - Examples

## 🔍 Acceptance Criteria

### Functional Requirements
- [ ] Rows encode/decode correctly
- [ ] NULL handling works
- [ ] Variable-length fields supported
- [ ] Schema evolution supported
- [ ] Large rows handled

### Quality Requirements
- [ ] All tests pass
- [ ] Code coverage >85%
- [ ] Documentation complete

### Performance Requirements
- [ ] Meets throughput targets
- [ ] Space-efficient
- [ ] Supports 1MB rows

## 🔗 Dependencies

This PR depends on:
- **PR 1.9**: Data Types System

This PR is required by:
- **PR 1.5**: Query Executor (uses rows)
- **PR 1.1**: Storage Engine (stores rows)

## 📦 Dependencies to Add

```toml
[dependencies]
# No new dependencies needed
```

## 🚀 Implementation Steps

### Day 1: Row Structure & Header
- Define Row and RowHeader
- Implement serialization
- Write tests

### Day 2: Encoder
- Implement RowEncoder
- NULL bitmap
- Variable-length handling
- Write tests

### Day 3: Decoder & Documentation
- Implement RowDecoder
- Integration tests
- Write format specification

## 🐛 Known Edge Cases to Handle

1. **Empty rows**: Handle gracefully
2. **All NULLs**: Space-efficient
3. **Very large fields**: Overflow handling
4. **Schema changes**: Version handling
5. **Corrupted data**: Validation

## 💡 Future Enhancements (Out of Scope)

- Compression → Phase 5
- Columnar format → Phase 5
- Encryption → Phase 6

## 🏁 Definition of Done

This PR is complete when:
1. All code implemented and tested
2. All acceptance criteria met
3. CI/CD passes
4. Documentation complete
5. Performance targets met

---

**Ready to implement?** Use this document as your complete specification. Good luck! 🚀
