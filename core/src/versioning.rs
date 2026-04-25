// Versioning Layer for table/row history
// Licensed under AGPLv3.0

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// Version identifier
pub type VersionId = u64;

/// Versioned record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionedRecord {
    id: String,
    current_version: VersionId,
    versions: HashMap<VersionId, RecordVersion>,
}

/// Individual record version
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordVersion {
    version_id: VersionId,
    timestamp: u64,
    data: Vec<u8>,
    author: Option<String>,
}

impl VersionedRecord {
    /// Create a new versioned record
    pub fn new(id: String, initial_data: Vec<u8>) -> Self {
        let mut versions = HashMap::new();
        let initial_version = RecordVersion {
            version_id: 1,
            timestamp: Self::current_timestamp(),
            data: initial_data,
            author: None,
        };
        versions.insert(1, initial_version);

        VersionedRecord {
            id,
            current_version: 1,
            versions,
        }
    }

    /// Add a new version
    pub fn add_version(&mut self, data: Vec<u8>, author: Option<String>) -> VersionId {
        let new_version_id = self.current_version + 1;
        let new_version = RecordVersion {
            version_id: new_version_id,
            timestamp: Self::current_timestamp(),
            data,
            author,
        };
        self.versions.insert(new_version_id, new_version);
        self.current_version = new_version_id;
        new_version_id
    }

    /// Get current version data
    pub fn get_current(&self) -> Option<&RecordVersion> {
        self.versions.get(&self.current_version)
    }

    /// Get specific version
    pub fn get_version(&self, version_id: VersionId) -> Option<&RecordVersion> {
        self.versions.get(&version_id)
    }

    /// Get all version IDs
    pub fn list_versions(&self) -> Vec<VersionId> {
        let mut versions: Vec<VersionId> = self.versions.keys().copied().collect();
        versions.sort();
        versions
    }

    fn current_timestamp() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_versioned_record_creation() {
        let record = VersionedRecord::new("test-1".to_string(), vec![1, 2, 3]);
        assert_eq!(record.current_version, 1);
        assert_eq!(record.list_versions(), vec![1]);
    }

    #[test]
    fn test_add_version() {
        let mut record = VersionedRecord::new("test-1".to_string(), vec![1, 2, 3]);
        let new_version = record.add_version(vec![4, 5, 6], Some("user1".to_string()));
        assert_eq!(new_version, 2);
        assert_eq!(record.current_version, 2);
        assert_eq!(record.list_versions(), vec![1, 2]);
    }

    #[test]
    fn test_get_version() {
        let mut record = VersionedRecord::new("test-1".to_string(), vec![1, 2, 3]);
        record.add_version(vec![4, 5, 6], None);

        let version1 = record.get_version(1).unwrap();
        assert_eq!(version1.data, vec![1, 2, 3]);

        let version2 = record.get_version(2).unwrap();
        assert_eq!(version2.data, vec![4, 5, 6]);
    }
}
