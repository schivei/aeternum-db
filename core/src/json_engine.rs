// JSON/JSON2 Engine with fixed schema support
// Licensed under AGPLv3.0

use serde::{Deserialize, Serialize};
use serde_json::{Value, Map};

/// JSON document with schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonDocument {
    schema: Option<JsonSchema>,
    data: Value,
}

/// JSON schema definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonSchema {
    name: String,
    version: String,
    fields: Map<String, Value>,
}

impl JsonDocument {
    /// Create a new JSON document
    pub fn new(data: Value) -> Self {
        JsonDocument {
            schema: None,
            data,
        }
    }

    /// Create a JSON document with schema
    pub fn with_schema(schema: JsonSchema, data: Value) -> Result<Self, String> {
        // TODO: Validate data against schema
        Ok(JsonDocument {
            schema: Some(schema),
            data,
        })
    }

    /// Get document data
    pub fn data(&self) -> &Value {
        &self.data
    }

    /// Get document schema
    pub fn schema(&self) -> Option<&JsonSchema> {
        self.schema.as_ref()
    }

    /// Validate document against its schema
    pub fn validate(&self) -> Result<(), String> {
        if let Some(_schema) = &self.schema {
            // TODO: Implement schema validation
            Ok(())
        } else {
            Ok(())
        }
    }
}

impl JsonSchema {
    /// Create a new schema
    pub fn new(name: String, version: String) -> Self {
        JsonSchema {
            name,
            version,
            fields: Map::new(),
        }
    }

    /// Add a field to the schema
    pub fn add_field(&mut self, name: String, field_type: Value) {
        self.fields.insert(name, field_type);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_json_document_creation() {
        let data = json!({"name": "test", "value": 42});
        let doc = JsonDocument::new(data.clone());
        assert_eq!(doc.data(), &data);
    }

    #[test]
    fn test_json_schema_creation() {
        let mut schema = JsonSchema::new("test_schema".to_string(), "1.0".to_string());
        schema.add_field("name".to_string(), json!("string"));
        schema.add_field("value".to_string(), json!("number"));
        assert_eq!(schema.fields.len(), 2);
    }

    #[test]
    fn test_json_document_with_schema() {
        let mut schema = JsonSchema::new("test_schema".to_string(), "1.0".to_string());
        schema.add_field("name".to_string(), json!("string"));

        let data = json!({"name": "test"});
        let doc = JsonDocument::with_schema(schema, data).unwrap();
        assert!(doc.schema().is_some());
    }
}
