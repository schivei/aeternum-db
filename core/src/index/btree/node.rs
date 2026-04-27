//! B-tree node types, operations, and serialization.
//!
//! A B-tree node is either an [`InternalNode`] (holds keys and child page
//! pointers) or a [`LeafNode`] (holds keys, associated values, and sibling
//! pointers for range scans).
//!
//! # Serialization format
//!
//! Both node types are serialized to/from raw byte slices so they can be
//! stored in storage-engine pages.
//!
//! ```text
//! ┌──────────────────────────────────────────────┐
//! │ node_type : u8   (0 = internal, 1 = leaf)    │
//! │ num_keys  : u32 (little-endian)               │
//! │ ... key/value/pointer entries ...             │
//! └──────────────────────────────────────────────┘
//! ```
//!
//! Keys and values are length-prefixed byte sequences (4-byte LE length +
//! payload).  Page IDs are stored as 8-byte LE `u64` values.

use crate::index::IndexError;
use crate::storage::page::PageId;

// ── Node type tag ─────────────────────────────────────────────────────────────

const NODE_TYPE_INTERNAL: u8 = 0;
const NODE_TYPE_LEAF: u8 = 1;

// ── Internal node ─────────────────────────────────────────────────────────────

/// An internal (non-leaf) B-tree node.
///
/// Contains `n` keys and `n + 1` child pointers.  Child `0` holds keys less
/// than `keys[0]`; for `0 < i < n`, child `i` holds keys in the range
/// `[keys[i - 1], keys[i])`; and child `n` holds keys greater than or equal
/// to `keys[n - 1]`.
#[derive(Debug, Clone)]
pub struct InternalNode {
    /// Separator keys (sorted, ascending).
    pub keys: Vec<Vec<u8>>,
    /// Child page IDs — always `keys.len() + 1` entries.
    pub children: Vec<PageId>,
}

impl InternalNode {
    /// Create an empty internal node.
    pub fn new() -> Self {
        InternalNode {
            keys: Vec::new(),
            children: Vec::new(),
        }
    }

    /// Number of keys currently in this node.
    #[inline]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// True when the node has no keys.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Find the child index to follow for a given raw key bytes.
    ///
    /// Returns the index `i` such that the child at `children[i]` is the
    /// correct subtree to search.
    pub fn find_child(&self, key: &[u8]) -> usize {
        // Binary search: find the first key >= search key.
        match self.keys.binary_search_by(|k| k.as_slice().cmp(key)) {
            Ok(i) => i + 1,   // key found → go right of that separator
            Err(i) => i,      // key not found → go to the child before insertion point
        }
    }

    /// Serialize this node into a byte vector.
    pub fn serialize(&self) -> Vec<u8> {
        let n = self.keys.len();
        let mut buf = Vec::new();
        buf.push(NODE_TYPE_INTERNAL);
        buf.extend_from_slice(&(n as u32).to_le_bytes());
        for key in &self.keys {
            write_bytes(&mut buf, key);
        }
        // children: n+1 PageIds
        for &child in &self.children {
            buf.extend_from_slice(&child.to_le_bytes());
        }
        buf
    }

    /// Deserialize an internal node from bytes.
    pub fn deserialize(data: &[u8]) -> Result<Self, IndexError> {
        let mut pos = 0usize;
        if data.is_empty() {
            return Err(IndexError::Serialization("empty data".into()));
        }
        let node_type = data[pos];
        pos += 1;
        if node_type != NODE_TYPE_INTERNAL {
            return Err(IndexError::Serialization(format!(
                "expected internal node type {NODE_TYPE_INTERNAL}, got {node_type}"
            )));
        }
        let n = read_u32(data, &mut pos)? as usize;
        let mut keys = Vec::with_capacity(n);
        for _ in 0..n {
            keys.push(read_bytes(data, &mut pos)?);
        }
        let mut children = Vec::with_capacity(n + 1);
        for _ in 0..=n {
            children.push(read_u64(data, &mut pos)?);
        }
        Ok(InternalNode { keys, children })
    }
}

impl Default for InternalNode {
    fn default() -> Self {
        Self::new()
    }
}

// ── Leaf node ─────────────────────────────────────────────────────────────────

/// A leaf B-tree node.
///
/// Stores the actual key-value pairs.  Siblings are linked for efficient
/// range scans.
#[derive(Debug, Clone)]
pub struct LeafNode {
    /// Keys in sorted order.
    pub keys: Vec<Vec<u8>>,
    /// Values corresponding to each key (parallel array).
    pub values: Vec<Vec<u8>>,
    /// Page ID of the next leaf (right sibling), for forward range scans.
    pub next_leaf: Option<PageId>,
    /// Page ID of the previous leaf (left sibling), for backward iteration.
    pub prev_leaf: Option<PageId>,
}

impl LeafNode {
    /// Create an empty leaf node.
    pub fn new() -> Self {
        LeafNode {
            keys: Vec::new(),
            values: Vec::new(),
            next_leaf: None,
            prev_leaf: None,
        }
    }

    /// Number of key-value pairs in this leaf.
    #[inline]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// True when the leaf has no entries.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Find the index of `key` using binary search.
    ///
    /// Returns `Ok(i)` when found at position `i`, or `Err(i)` for the
    /// insertion position when not found.
    pub fn find_key(&self, key: &[u8]) -> Result<usize, usize> {
        self.keys.binary_search_by(|k| k.as_slice().cmp(key))
    }

    /// Insert a key-value pair maintaining sorted order.
    ///
    /// Returns `Err(IndexError::DuplicateKey)` if the key already exists.
    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), IndexError> {
        match self.find_key(&key) {
            Ok(_) => Err(IndexError::DuplicateKey),
            Err(i) => {
                self.keys.insert(i, key);
                self.values.insert(i, value);
                Ok(())
            }
        }
    }

    /// Update the value for an existing key.
    ///
    /// Returns `true` if the key was found and updated, `false` otherwise.
    pub fn update(&mut self, key: &[u8], value: Vec<u8>) -> bool {
        match self.find_key(key) {
            Ok(i) => {
                self.values[i] = value;
                true
            }
            Err(_) => false,
        }
    }

    /// Remove a key-value pair.  Returns `true` if the key was present.
    pub fn remove(&mut self, key: &[u8]) -> bool {
        match self.find_key(key) {
            Ok(i) => {
                self.keys.remove(i);
                self.values.remove(i);
                true
            }
            Err(_) => false,
        }
    }

    /// Serialize this leaf node into a byte vector.
    pub fn serialize(&self) -> Vec<u8> {
        let n = self.keys.len();
        let mut buf = Vec::new();
        buf.push(NODE_TYPE_LEAF);
        buf.extend_from_slice(&(n as u32).to_le_bytes());
        for (k, v) in self.keys.iter().zip(self.values.iter()) {
            write_bytes(&mut buf, k);
            write_bytes(&mut buf, v);
        }
        write_optional_page_id(&mut buf, self.next_leaf);
        write_optional_page_id(&mut buf, self.prev_leaf);
        buf
    }

    /// Deserialize a leaf node from bytes.
    pub fn deserialize(data: &[u8]) -> Result<Self, IndexError> {
        let mut pos = 0usize;
        if data.is_empty() {
            return Err(IndexError::Serialization("empty data".into()));
        }
        let node_type = data[pos];
        pos += 1;
        if node_type != NODE_TYPE_LEAF {
            return Err(IndexError::Serialization(format!(
                "expected leaf node type {NODE_TYPE_LEAF}, got {node_type}"
            )));
        }
        let n = read_u32(data, &mut pos)? as usize;
        let mut keys = Vec::with_capacity(n);
        let mut values = Vec::with_capacity(n);
        for _ in 0..n {
            keys.push(read_bytes(data, &mut pos)?);
            values.push(read_bytes(data, &mut pos)?);
        }
        let next_leaf = read_optional_page_id(data, &mut pos)?;
        let prev_leaf = read_optional_page_id(data, &mut pos)?;
        Ok(LeafNode {
            keys,
            values,
            next_leaf,
            prev_leaf,
        })
    }
}

impl Default for LeafNode {
    fn default() -> Self {
        Self::new()
    }
}

// ── Node enum ─────────────────────────────────────────────────────────────────

/// Either an internal or a leaf node.
#[derive(Debug, Clone)]
pub enum Node {
    /// An internal node holds separator keys and child pointers.
    Internal(InternalNode),
    /// A leaf node holds actual key-value pairs.
    Leaf(LeafNode),
}

impl Node {
    /// Serialize the node to bytes.
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            Node::Internal(n) => n.serialize(),
            Node::Leaf(n) => n.serialize(),
        }
    }

    /// Deserialize a node from bytes, auto-detecting the type from the first byte.
    pub fn deserialize(data: &[u8]) -> Result<Self, IndexError> {
        if data.is_empty() {
            return Err(IndexError::Serialization("empty data".into()));
        }
        match data[0] {
            NODE_TYPE_INTERNAL => Ok(Node::Internal(InternalNode::deserialize(data)?)),
            NODE_TYPE_LEAF => Ok(Node::Leaf(LeafNode::deserialize(data)?)),
            t => Err(IndexError::Serialization(format!(
                "unknown node type byte: {t}"
            ))),
        }
    }

    /// True if this is a leaf node.
    pub fn is_leaf(&self) -> bool {
        matches!(self, Node::Leaf(_))
    }
}

// ── Split / merge helpers ─────────────────────────────────────────────────────

/// Result of splitting a leaf node.
pub struct LeafSplitResult {
    /// The new right sibling.
    pub right: LeafNode,
    /// The smallest key in the right sibling (used as separator in the parent).
    pub split_key: Vec<u8>,
}

/// Result of splitting an internal node.
pub struct InternalSplitResult {
    /// The new right sibling.
    pub right: InternalNode,
    /// The key pushed up to the parent.
    pub push_up_key: Vec<u8>,
}

impl LeafNode {
    /// Split this leaf in half.
    ///
    /// After the call `self` contains the left half and the returned
    /// [`LeafSplitResult`] contains the right half.  Sibling pointers are
    /// updated so that `self.next_leaf` points to the new right sibling.
    ///
    /// # Note
    /// The caller must:
    /// 1. Allocate a new page for `result.right` and record its `PageId`.
    /// 2. Set `result.right.prev_leaf = Some(left_page_id)`.
    /// 3. Set `result.right.next_leaf` to the original `self.next_leaf`.
    /// 4. Update the previously-next node's `prev_leaf` pointer.
    pub fn split(&mut self) -> LeafSplitResult {
        let mid = self.keys.len() / 2;
        let split_key = self.keys[mid].clone();

        let right_keys = self.keys.split_off(mid);
        let right_values = self.values.split_off(mid);

        let right = LeafNode {
            keys: right_keys,
            values: right_values,
            next_leaf: self.next_leaf,
            prev_leaf: None, // caller sets this
        };

        // self.next_leaf is set to the new right page by the caller
        LeafSplitResult { right, split_key }
    }
}

impl InternalNode {
    /// Split this internal node in half, pushing the middle key up.
    ///
    /// After the call `self` becomes the left half, and the returned
    /// [`InternalSplitResult`] contains the right half and the key to push up.
    pub fn split(&mut self) -> InternalSplitResult {
        let mid = self.keys.len() / 2;
        let push_up_key = self.keys[mid].clone();

        // right node gets keys[mid+1..] and children[mid+1..]
        let right_keys = self.keys.split_off(mid + 1);
        let right_children = self.children.split_off(mid + 1);

        // remove the push-up key from self
        self.keys.pop();

        let right = InternalNode {
            keys: right_keys,
            children: right_children,
        };

        InternalSplitResult {
            right,
            push_up_key,
        }
    }
}

// ── Serialization helpers ─────────────────────────────────────────────────────

fn write_bytes(buf: &mut Vec<u8>, data: &[u8]) {
    buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
    buf.extend_from_slice(data);
}

fn write_optional_page_id(buf: &mut Vec<u8>, id: Option<PageId>) {
    match id {
        Some(p) => {
            buf.push(1u8);
            buf.extend_from_slice(&p.to_le_bytes());
        }
        None => {
            buf.push(0u8);
        }
    }
}

fn read_u32(data: &[u8], pos: &mut usize) -> Result<u32, IndexError> {
    let end = *pos + 4;
    if end > data.len() {
        return Err(IndexError::Serialization("unexpected end of data (u32)".into()));
    }
    let v = u32::from_le_bytes(data[*pos..end].try_into().unwrap());
    *pos = end;
    Ok(v)
}

fn read_u64(data: &[u8], pos: &mut usize) -> Result<u64, IndexError> {
    let end = *pos + 8;
    if end > data.len() {
        return Err(IndexError::Serialization("unexpected end of data (u64)".into()));
    }
    let v = u64::from_le_bytes(data[*pos..end].try_into().unwrap());
    *pos = end;
    Ok(v)
}

fn read_bytes(data: &[u8], pos: &mut usize) -> Result<Vec<u8>, IndexError> {
    let len = read_u32(data, pos)? as usize;
    let end = *pos + len;
    if end > data.len() {
        return Err(IndexError::Serialization("unexpected end of data (bytes)".into()));
    }
    let bytes = data[*pos..end].to_vec();
    *pos = end;
    Ok(bytes)
}

fn read_optional_page_id(data: &[u8], pos: &mut usize) -> Result<Option<PageId>, IndexError> {
    if *pos >= data.len() {
        return Err(IndexError::Serialization(
            "unexpected end of data (optional page id presence byte)".into(),
        ));
    }
    let present = data[*pos];
    *pos += 1;
    if present == 0 {
        return Ok(None);
    }
    Ok(Some(read_u64(data, pos)?))
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn kb(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    // ── LeafNode ──────────────────────────────────────────────────────────────

    #[test]
    fn leaf_insert_sorted() {
        let mut leaf = LeafNode::new();
        leaf.insert(kb("c"), kb("3")).unwrap();
        leaf.insert(kb("a"), kb("1")).unwrap();
        leaf.insert(kb("b"), kb("2")).unwrap();
        assert_eq!(leaf.keys, vec![kb("a"), kb("b"), kb("c")]);
        assert_eq!(leaf.values, vec![kb("1"), kb("2"), kb("3")]);
    }

    #[test]
    fn leaf_duplicate_key_is_error() {
        let mut leaf = LeafNode::new();
        leaf.insert(kb("a"), kb("1")).unwrap();
        assert!(matches!(
            leaf.insert(kb("a"), kb("2")),
            Err(IndexError::DuplicateKey)
        ));
    }

    #[test]
    fn leaf_remove_present() {
        let mut leaf = LeafNode::new();
        leaf.insert(kb("a"), kb("1")).unwrap();
        leaf.insert(kb("b"), kb("2")).unwrap();
        assert!(leaf.remove(kb("a").as_slice()));
        assert_eq!(leaf.len(), 1);
        assert_eq!(leaf.keys[0], kb("b"));
    }

    #[test]
    fn leaf_remove_absent_returns_false() {
        let mut leaf = LeafNode::new();
        assert!(!leaf.remove(kb("z").as_slice()));
    }

    #[test]
    fn leaf_serialization_roundtrip() {
        let mut leaf = LeafNode::new();
        leaf.insert(kb("hello"), kb("world")).unwrap();
        leaf.insert(kb("foo"), kb("bar")).unwrap();
        leaf.next_leaf = Some(42);
        leaf.prev_leaf = Some(7);

        let bytes = leaf.serialize();
        let restored = LeafNode::deserialize(&bytes).unwrap();
        assert_eq!(restored.keys, leaf.keys);
        assert_eq!(restored.values, leaf.values);
        assert_eq!(restored.next_leaf, leaf.next_leaf);
        assert_eq!(restored.prev_leaf, leaf.prev_leaf);
    }

    #[test]
    fn leaf_split_halves() {
        let mut leaf = LeafNode::new();
        for i in 0..10u8 {
            leaf.insert(vec![i], vec![i + 100]).unwrap();
        }
        let result = leaf.split();
        assert_eq!(leaf.len(), 5);
        assert_eq!(result.right.len(), 5);
        assert_eq!(result.split_key, vec![5u8]);
    }

    // ── InternalNode ──────────────────────────────────────────────────────────

    #[test]
    fn internal_find_child() {
        let node = InternalNode {
            keys: vec![kb("d"), kb("h"), kb("m")],
            children: vec![0, 1, 2, 3],
        };
        // key < "d" → child 0
        assert_eq!(node.find_child(kb("a").as_slice()), 0);
        // key == "d" → child 1 (≥ separator)
        assert_eq!(node.find_child(kb("d").as_slice()), 1);
        // "d" < key < "h" → child 1
        assert_eq!(node.find_child(kb("f").as_slice()), 1);
        // key == "h" → child 2
        assert_eq!(node.find_child(kb("h").as_slice()), 2);
        // key > "m" → child 3
        assert_eq!(node.find_child(kb("z").as_slice()), 3);
    }

    #[test]
    fn internal_serialization_roundtrip() {
        let node = InternalNode {
            keys: vec![kb("b"), kb("e")],
            children: vec![10, 20, 30],
        };
        let bytes = node.serialize();
        let restored = InternalNode::deserialize(&bytes).unwrap();
        assert_eq!(restored.keys, node.keys);
        assert_eq!(restored.children, node.children);
    }

    #[test]
    fn internal_split() {
        // 5 keys → push up index 2, left gets [0,1], right gets [3,4]
        let mut node = InternalNode {
            keys: vec![kb("a"), kb("b"), kb("c"), kb("d"), kb("e")],
            children: vec![0, 1, 2, 3, 4, 5],
        };
        let result = node.split();
        assert_eq!(node.keys, vec![kb("a"), kb("b")]);
        assert_eq!(node.children, vec![0, 1, 2]);
        assert_eq!(result.push_up_key, kb("c"));
        assert_eq!(result.right.keys, vec![kb("d"), kb("e")]);
        assert_eq!(result.right.children, vec![3, 4, 5]);
    }

    // ── Node enum ─────────────────────────────────────────────────────────────

    #[test]
    fn node_deserialize_internal() {
        let internal = InternalNode {
            keys: vec![kb("x")],
            children: vec![1, 2],
        };
        let bytes = Node::Internal(internal).serialize();
        let node = Node::deserialize(&bytes).unwrap();
        assert!(!node.is_leaf());
    }

    #[test]
    fn node_deserialize_leaf() {
        let mut leaf = LeafNode::new();
        leaf.insert(kb("k"), kb("v")).unwrap();
        let bytes = Node::Leaf(leaf).serialize();
        let node = Node::deserialize(&bytes).unwrap();
        assert!(node.is_leaf());
    }

    #[test]
    fn node_deserialize_unknown_type_is_error() {
        let data = vec![99u8, 0, 0, 0, 0];
        assert!(Node::deserialize(&data).is_err());
    }
}
