//! Production-ready B-tree index backed by the AeternumDB storage engine.
//!
//! # Design
//!
//! This is a B+ tree variant where all values reside in leaf nodes.  Internal
//! nodes hold only separator keys and child page pointers.  Leaf nodes form a
//! doubly-linked list for efficient range scans.
//!
//! ## Concurrency
//!
//! The tree is protected by a single `tokio::sync::RwLock` on its metadata
//! (root page ID, height, etc.).  This provides safe concurrent reads and
//! serialised writes.  A future upgrade can implement latch coupling (crabbing)
//! for finer-grained concurrency.
//!
//! ## Serialization
//!
//! Generic keys and values must implement [`BTreeKey`] and [`BTreeValue`]
//! respectively, which define how they are converted to/from raw byte slices.
//! This keeps the storage layer free of generics.
//!
//! ## Page layout
//!
//! Each B-tree node occupies exactly one storage page.  The first byte of the
//! page data encodes the node type (`0` = internal, `1` = leaf); the rest is
//! the serialized node content.
//!
//! ## Metadata page
//!
//! One storage page is reserved at tree creation time to hold tree-level
//! metadata.  Its page ID is returned by [`BTree::meta_page_id`] and must be
//! passed to [`BTree::open`] to reopen an existing tree.  This page is **not**
//! fixed at page 0; it is the first page allocated by [`BTree::new`].
//!
//! ```text
//! ┌─────────────────────────────────┐
//! │ root_page_id : u64              │
//! │ height       : u64              │
//! │ num_keys     : u64              │
//! │ fanout       : u32              │
//! └─────────────────────────────────┘
//! ```

pub mod iterator;
pub mod node;

use std::ops::RangeBounds;
use std::sync::Arc;
use tokio::sync::RwLock;

use self::iterator::{BTreeIterator, EndBound};
use self::node::{InternalNode, LeafNode, Node};
use crate::index::IndexError;
use crate::storage::page::PageId;
use crate::storage::StorageEngine;

// ── Configuration ─────────────────────────────────────────────────────────────

/// Configuration parameters for a [`BTree`].
#[derive(Debug, Clone)]
pub struct BTreeConfig {
    /// Maximum number of keys per node.
    ///
    /// Must be in the range `[4, 1000]`.  Default is `100`.
    pub fanout: usize,
}

impl Default for BTreeConfig {
    fn default() -> Self {
        BTreeConfig { fanout: 100 }
    }
}

// ── Key / Value trait bounds ──────────────────────────────────────────────────

/// Conversion between a typed key and raw bytes used for on-disk storage.
///
/// Implementations must produce a byte representation that preserves the
/// natural ordering of the type (i.e. `a < b ⟺ a.to_bytes() < b.to_bytes()`
/// under lexicographic comparison).
pub trait BTreeKey: Ord + Clone + Send + Sync + 'static {
    /// Serialize the key to bytes.
    fn to_bytes(&self) -> Vec<u8>;
    /// Deserialize a key from bytes.
    fn from_bytes(bytes: &[u8]) -> Result<Self, IndexError>
    where
        Self: Sized;
}

/// Conversion between a typed value and raw bytes used for on-disk storage.
pub trait BTreeValue: Clone + Send + Sync + 'static {
    /// Serialize the value to bytes.
    fn to_bytes(&self) -> Vec<u8>;
    /// Deserialize a value from bytes.
    fn from_bytes(bytes: &[u8]) -> Result<Self, IndexError>
    where
        Self: Sized;
}

// ── Built-in key/value implementations ───────────────────────────────────────

impl BTreeKey for i64 {
    fn to_bytes(&self) -> Vec<u8> {
        // Big-endian with sign-bit flip so that lexicographic order equals
        // numeric order for signed integers.
        let bits = (*self as u64) ^ (1u64 << 63);
        bits.to_be_bytes().to_vec()
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, IndexError> {
        if bytes.len() != 8 {
            return Err(IndexError::Serialization("i64 key must be 8 bytes".into()));
        }
        let bits = u64::from_be_bytes(bytes.try_into().unwrap());
        Ok((bits ^ (1u64 << 63)) as i64)
    }
}

impl BTreeKey for u64 {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, IndexError> {
        if bytes.len() != 8 {
            return Err(IndexError::Serialization("u64 key must be 8 bytes".into()));
        }
        Ok(u64::from_be_bytes(bytes.try_into().unwrap()))
    }
}

impl BTreeKey for String {
    fn to_bytes(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, IndexError> {
        String::from_utf8(bytes.to_vec())
            .map_err(|e| IndexError::Serialization(format!("invalid UTF-8 in key: {e}")))
    }
}

impl BTreeKey for Vec<u8> {
    fn to_bytes(&self) -> Vec<u8> {
        self.clone()
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, IndexError> {
        Ok(bytes.to_vec())
    }
}

impl BTreeValue for String {
    fn to_bytes(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, IndexError> {
        String::from_utf8(bytes.to_vec())
            .map_err(|e| IndexError::Serialization(format!("invalid UTF-8 in value: {e}")))
    }
}

impl BTreeValue for Vec<u8> {
    fn to_bytes(&self) -> Vec<u8> {
        self.clone()
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, IndexError> {
        Ok(bytes.to_vec())
    }
}

impl BTreeValue for u64 {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, IndexError> {
        if bytes.len() != 8 {
            return Err(IndexError::Serialization(
                "u64 value must be 8 bytes".into(),
            ));
        }
        Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
    }
}

impl BTreeValue for i64 {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, IndexError> {
        if bytes.len() != 8 {
            return Err(IndexError::Serialization(
                "i64 value must be 8 bytes".into(),
            ));
        }
        Ok(i64::from_le_bytes(bytes.try_into().unwrap()))
    }
}

// ── Metadata ──────────────────────────────────────────────────────────────────

const META_ROOT_OFFSET: usize = 0;
const META_HEIGHT_OFFSET: usize = 8;
const META_NUM_KEYS_OFFSET: usize = 16;
const META_FANOUT_OFFSET: usize = 24;
const META_SIZE: usize = 28;

/// In-memory tree metadata.
#[derive(Debug, Clone)]
struct TreeMeta {
    /// Page ID that holds the B-tree metadata (first allocated page).
    meta_page_id: PageId,
    /// Page ID of the current root node.
    root_page_id: PageId,
    /// Height of the tree (1 = root is a leaf).
    height: usize,
    /// Total number of key-value pairs in the tree.
    num_keys: u64,
    /// Maximum keys per node.
    fanout: usize,
}

// ── BTree struct ──────────────────────────────────────────────────────────────

/// A persistent B+ tree index backed by the AeternumDB storage engine.
///
/// `K` is the key type; `V` is the value type.  Both must implement
/// [`BTreeKey`] / [`BTreeValue`] for serialization.
///
/// # Cloning
///
/// `BTree` is cheap to clone — all clones share the same underlying storage
/// handle and tree metadata through an [`Arc`].
pub struct BTree<K, V> {
    storage: Arc<StorageEngine>,
    meta: Arc<RwLock<TreeMeta>>,
    _key: std::marker::PhantomData<K>,
    _val: std::marker::PhantomData<V>,
}

impl<K: BTreeKey, V: BTreeValue> Clone for BTree<K, V> {
    fn clone(&self) -> Self {
        BTree {
            storage: self.storage.clone(),
            meta: self.meta.clone(),
            _key: std::marker::PhantomData,
            _val: std::marker::PhantomData,
        }
    }
}

impl<K: BTreeKey, V: BTreeValue> BTree<K, V> {
    // ── Construction ──────────────────────────────────────────────────────────

    /// Create a new, empty B-tree with the given storage engine and config.
    ///
    /// Allocates a metadata page and a root leaf page.
    pub async fn new(storage: Arc<StorageEngine>, config: BTreeConfig) -> Result<Self, IndexError> {
        if config.fanout < 4 || config.fanout > 1000 {
            return Err(IndexError::InvalidFanout(config.fanout));
        }

        // Allocate metadata page.
        let meta_page_id = storage.allocate_page().await?;
        // Allocate root leaf page.
        let root_page_id = storage.allocate_page().await?;

        // Write empty root leaf to storage.
        let root_leaf = LeafNode::new();
        let node = Node::Leaf(root_leaf);
        write_node(&storage, root_page_id, &node).await?;

        let meta = TreeMeta {
            meta_page_id,
            root_page_id,
            height: 1,
            num_keys: 0,
            fanout: config.fanout,
        };

        let tree = BTree {
            storage,
            meta: Arc::new(RwLock::new(meta)),
            _key: std::marker::PhantomData,
            _val: std::marker::PhantomData,
        };

        // Persist metadata.
        tree.write_meta().await?;

        Ok(tree)
    }

    /// Open an existing B-tree from the metadata page.
    ///
    /// `meta_page_id` is the first page allocated by [`BTree::new`].
    pub async fn open(
        storage: Arc<StorageEngine>,
        meta_page_id: PageId,
    ) -> Result<Self, IndexError> {
        let data = storage
            .read_page_data(meta_page_id, 0, META_SIZE)
            .await
            .map_err(IndexError::Storage)?;

        let root_page_id = u64::from_le_bytes(
            data[META_ROOT_OFFSET..META_ROOT_OFFSET + 8]
                .try_into()
                .unwrap(),
        );
        let height = u64::from_le_bytes(
            data[META_HEIGHT_OFFSET..META_HEIGHT_OFFSET + 8]
                .try_into()
                .unwrap(),
        ) as usize;
        let num_keys = u64::from_le_bytes(
            data[META_NUM_KEYS_OFFSET..META_NUM_KEYS_OFFSET + 8]
                .try_into()
                .unwrap(),
        );
        let fanout = u32::from_le_bytes(
            data[META_FANOUT_OFFSET..META_FANOUT_OFFSET + 4]
                .try_into()
                .unwrap(),
        ) as usize;

        if !(4..=1000).contains(&fanout) {
            return Err(IndexError::InvalidFanout(fanout));
        }
        if height == 0 {
            return Err(IndexError::Corrupt(
                "btree metadata corrupt: height must be at least 1".into(),
            ));
        }
        if root_page_id == meta_page_id {
            return Err(IndexError::Corrupt(
                "btree metadata corrupt: root page must not be the metadata page".into(),
            ));
        }

        let meta = TreeMeta {
            meta_page_id,
            root_page_id,
            height,
            num_keys,
            fanout,
        };

        Ok(BTree {
            storage,
            meta: Arc::new(RwLock::new(meta)),
            _key: std::marker::PhantomData,
            _val: std::marker::PhantomData,
        })
    }

    /// The page ID of the B-tree metadata page (used to reopen with [`open`]).
    pub async fn meta_page_id(&self) -> PageId {
        self.meta.read().await.meta_page_id
    }

    // ── Public CRUD API ───────────────────────────────────────────────────────

    /// Insert a key-value pair into the tree.
    ///
    /// Returns `Err(IndexError::DuplicateKey)` if the key already exists.
    /// Use [`upsert`](Self::upsert) to replace an existing value.
    pub async fn insert(&self, key: K, value: V) -> Result<(), IndexError> {
        let key_bytes = key.to_bytes();
        let val_bytes = value.to_bytes();
        let mut meta = self.meta.write().await;
        let result = self
            .insert_recursive(
                meta.root_page_id,
                &key_bytes,
                &val_bytes,
                meta.height,
                meta.fanout,
            )
            .await?;

        if let Some((push_up_key, new_right_page_id)) = result {
            // Root was split; create a new root.
            let new_root = InternalNode {
                keys: vec![push_up_key],
                children: vec![meta.root_page_id, new_right_page_id],
            };
            let new_root_page_id = self.storage.allocate_page().await?;
            write_node(&self.storage, new_root_page_id, &Node::Internal(new_root)).await?;
            meta.root_page_id = new_root_page_id;
            meta.height += 1;
        }

        meta.num_keys += 1;
        // Persist metadata while the write lock is still held to prevent
        // another writer from seeing or overwriting a partial update.
        self.write_meta_inner(&meta).await
    }

    /// Insert or update a key-value pair.
    ///
    /// If the key already exists its value is replaced; otherwise a new entry
    /// is inserted.  The operation is atomic under a single exclusive tree lock.
    pub async fn upsert(&self, key: K, value: V) -> Result<(), IndexError> {
        let key_bytes = key.to_bytes();
        let val_bytes = value.to_bytes();
        let mut meta = self.meta.write().await;

        match self
            .insert_recursive(
                meta.root_page_id,
                &key_bytes,
                &val_bytes,
                meta.height,
                meta.fanout,
            )
            .await
        {
            Ok(result) => {
                if let Some((push_up_key, new_right_page_id)) = result {
                    let new_root = InternalNode {
                        keys: vec![push_up_key],
                        children: vec![meta.root_page_id, new_right_page_id],
                    };
                    let new_root_page_id = self.storage.allocate_page().await?;
                    write_node(&self.storage, new_root_page_id, &Node::Internal(new_root)).await?;
                    meta.root_page_id = new_root_page_id;
                    meta.height += 1;
                }
                meta.num_keys += 1;
                self.write_meta_inner(&meta).await
            }
            Err(IndexError::DuplicateKey) => {
                let result = self
                    .update_recursive(
                        meta.root_page_id,
                        &key_bytes,
                        &val_bytes,
                        meta.height,
                        meta.fanout,
                    )
                    .await;
                drop(meta);
                result
            }
            Err(e) => Err(e),
        }
    }

    /// Update the value for an existing key.
    ///
    /// Returns `Err(IndexError::Corrupt)` if the key does not exist.
    pub async fn update(&self, key: K, value: V) -> Result<(), IndexError> {
        let key_bytes = key.to_bytes();
        let val_bytes = value.to_bytes();
        let meta = self.meta.write().await;
        let root = meta.root_page_id;
        let height = meta.height;
        let fanout = meta.fanout;
        let result = self
            .update_recursive(root, &key_bytes, &val_bytes, height, fanout)
            .await;
        drop(meta);
        result
    }

    /// Search for an exact key.
    ///
    /// Returns `Ok(Some(value))` when found, `Ok(None)` when not found.
    pub async fn search(&self, key: &K) -> Result<Option<V>, IndexError> {
        let key_bytes = key.to_bytes();
        let meta = self.meta.read().await;
        let root = meta.root_page_id;
        let height = meta.height;
        self.search_recursive(root, &key_bytes, height).await
    }

    /// Delete a key from the tree.
    ///
    /// Returns `Ok(true)` if the key was found and deleted, `Ok(false)` if the
    /// key did not exist.
    pub async fn delete(&self, key: &K) -> Result<bool, IndexError> {
        let key_bytes = key.to_bytes();
        let mut meta = self.meta.write().await;
        let deleted = self
            .delete_recursive(meta.root_page_id, &key_bytes, meta.height)
            .await?;

        if deleted {
            meta.num_keys = meta.num_keys.saturating_sub(1);

            // Collapse root if it's an empty internal node.
            let root_node = load_node(&self.storage, meta.root_page_id).await?;
            if let Node::Internal(ref internal) = root_node {
                if internal.is_empty() && !internal.children.is_empty() {
                    let old_root = meta.root_page_id;
                    meta.root_page_id = internal.children[0];
                    meta.height -= 1;
                    self.write_meta_inner(&meta).await?;
                    self.storage.deallocate_page(old_root).await?;
                    drop(meta);
                    return Ok(true);
                }
            }
            self.write_meta_inner(&meta).await?;
            drop(meta);
        }

        Ok(deleted)
    }

    /// Perform a range scan, returning a [`BTreeIterator`] over matching entries.
    ///
    /// Accepts any `RangeBounds<K>` (e.g. `10..=50`, `"a".."z"`, `..`).
    ///
    /// The entire leaf chain within the range is pre-loaded at call time, so
    /// iteration itself is synchronous.
    pub async fn range<R: RangeBounds<K>>(&self, range: R) -> Result<BTreeIterator, IndexError> {
        use std::ops::Bound;

        let start_bytes: Option<Vec<u8>> = match range.start_bound() {
            Bound::Included(k) | Bound::Excluded(k) => Some(k.to_bytes()),
            Bound::Unbounded => None,
        };
        let start_excluded = matches!(range.start_bound(), Bound::Excluded(_));

        let end_bound: EndBound = match range.end_bound() {
            Bound::Included(k) => EndBound::Included(k.to_bytes()),
            Bound::Excluded(k) => EndBound::Excluded(k.to_bytes()),
            Bound::Unbounded => EndBound::Unbounded,
        };

        let leaves = {
            let meta = self.meta.read().await;

            // Keep the metadata read lock held while descending from the current
            // root and walking the leaf chain so concurrent writers cannot
            // change tree metadata or reclaim pages during the scan setup.
            let (_, start_leaf) = self
                .find_leaf(meta.root_page_id, start_bytes.as_deref(), meta.height)
                .await?;

            // Walk the leaf chain, collecting all leaves that can contribute to
            // the range.  The starting leaf is reused directly to avoid a
            // redundant reload.
            self.collect_range_leaves(start_leaf, &end_bound).await?
        };

        if leaves.is_empty() {
            return Ok(BTreeIterator::empty());
        }

        let mut leaves_deque = std::collections::VecDeque::from(leaves);
        let first_leaf = leaves_deque.pop_front().expect("non-empty");
        let start_pos = start_pos_in_leaf(&first_leaf, start_bytes.as_deref(), start_excluded);

        Ok(BTreeIterator::new(
            first_leaf,
            start_pos,
            leaves_deque.into_iter().collect(),
            end_bound,
        ))
    }

    /// Walk the leaf chain starting at `start_leaf`, collecting all leaves
    /// that fall within `end_bound`.
    async fn collect_range_leaves(
        &self,
        start_leaf: LeafNode,
        end_bound: &EndBound,
    ) -> Result<Vec<LeafNode>, IndexError> {
        let mut leaves: Vec<LeafNode> = Vec::new();
        let mut current_leaf = start_leaf;
        loop {
            if leaf_past_range_end(&current_leaf, end_bound) {
                break;
            }
            let next = current_leaf.next_leaf;
            leaves.push(current_leaf);

            let Some(leaf_id) = next else {
                break;
            };

            let node = load_node(&self.storage, leaf_id).await?;
            if let Node::Leaf(leaf) = node {
                current_leaf = leaf;
            } else {
                return Err(IndexError::Corrupt(format!(
                    "expected leaf at page {leaf_id}"
                )));
            }
        }
        Ok(leaves)
    }

    /// Bulk-load key-value pairs into the tree.
    ///
    /// Entries are inserted using the normal insert path, so input does not
    /// need to be sorted for correctness.  However, providing entries sorted by
    /// ascending key may offer better performance.  Duplicate keys are not
    /// allowed and will cause `IndexError::DuplicateKey`.
    ///
    /// A future optimisation can use a bottom-up bulk-load algorithm.
    pub async fn bulk_load(&self, entries: Vec<(K, V)>) -> Result<(), IndexError> {
        for (k, v) in entries {
            self.insert(k, v).await?;
        }
        Ok(())
    }

    /// Return the total number of key-value pairs in the tree.
    pub async fn len(&self) -> u64 {
        self.meta.read().await.num_keys
    }

    /// Return the current height of the tree (1 = root is a leaf).
    pub async fn height(&self) -> usize {
        self.meta.read().await.height
    }

    /// Return `true` if the tree contains no entries.
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /// Persist metadata to the metadata page.
    /// Persist the metadata page from an already-held `TreeMeta` reference.
    ///
    /// Callers that already hold (or have just updated) a `RwLockWriteGuard`
    /// must use this instead of `write_meta()` to avoid a deadlock caused by
    /// reacquiring the read lock while the write lock is still held.
    async fn write_meta_inner(&self, meta: &TreeMeta) -> Result<(), IndexError> {
        let mut buf = vec![0u8; META_SIZE];
        buf[META_ROOT_OFFSET..META_ROOT_OFFSET + 8]
            .copy_from_slice(&meta.root_page_id.to_le_bytes());
        buf[META_HEIGHT_OFFSET..META_HEIGHT_OFFSET + 8]
            .copy_from_slice(&(meta.height as u64).to_le_bytes());
        buf[META_NUM_KEYS_OFFSET..META_NUM_KEYS_OFFSET + 8]
            .copy_from_slice(&meta.num_keys.to_le_bytes());
        buf[META_FANOUT_OFFSET..META_FANOUT_OFFSET + 4]
            .copy_from_slice(&(meta.fanout as u32).to_le_bytes());
        self.storage
            .write_page_data(meta.meta_page_id, 0, &buf)
            .await
            .map_err(IndexError::Storage)
    }

    /// Persist the current in-memory metadata to its page.
    ///
    /// Acquires a shared read lock; do **not** call while holding a write lock
    /// on `self.meta` (use [`write_meta_inner`] instead).
    async fn write_meta(&self) -> Result<(), IndexError> {
        let meta = self.meta.read().await;
        self.write_meta_inner(&meta).await
    }

    /// Recursively insert into the subtree rooted at `page_id`.
    ///
    /// Returns `Some((push_up_key, new_right_page_id))` if the node was split,
    /// `None` otherwise.
    async fn insert_recursive(
        &self,
        page_id: PageId,
        key: &[u8],
        value: &[u8],
        height: usize,
        fanout: usize,
    ) -> Result<Option<(Vec<u8>, PageId)>, IndexError> {
        let node = load_node(&self.storage, page_id).await?;

        match node {
            Node::Leaf(mut leaf) => {
                leaf.insert(key.to_vec(), value.to_vec())?;
                self.split_leaf_node(page_id, leaf, fanout).await
            }
            Node::Internal(internal) => {
                if height == 0 {
                    return Err(IndexError::Corrupt(format!(
                        "found internal node at height 0, page {page_id}"
                    )));
                }
                let child_idx = internal.find_child(key);
                let child_page_id = internal.children[child_idx];

                let result =
                    Box::pin(self.insert_recursive(child_page_id, key, value, height - 1, fanout))
                        .await?;

                match result {
                    Some((push_up_key, new_right_page_id)) => {
                        self.handle_internal_push_up(
                            page_id,
                            internal,
                            child_idx,
                            push_up_key,
                            new_right_page_id,
                            fanout,
                        )
                        .await
                    }
                    None => Ok(None),
                }
            }
        }
    }

    /// Handle a leaf that may need splitting after an insert.
    ///
    /// Writes the (possibly-split) leaf to storage and returns the split key
    /// and new right sibling page ID, or `None` if no split was needed.
    async fn split_leaf_node(
        &self,
        page_id: PageId,
        mut leaf: LeafNode,
        fanout: usize,
    ) -> Result<Option<(Vec<u8>, PageId)>, IndexError> {
        if leaf.len() <= fanout {
            write_node(&self.storage, page_id, &Node::Leaf(leaf)).await?;
            return Ok(None);
        }

        let split_result = leaf.split();
        let right_page_id = self.storage.allocate_page().await?;

        // Wire up sibling pointers.
        let old_next = leaf.next_leaf;
        leaf.next_leaf = Some(right_page_id);
        let mut right = split_result.right;
        right.prev_leaf = Some(page_id);
        right.next_leaf = old_next;

        // Update the old next leaf's back-pointer.
        if let Some(next_id) = old_next {
            let mut next_node = load_node(&self.storage, next_id).await?;
            match next_node {
                Node::Leaf(ref mut next_leaf) => {
                    next_leaf.prev_leaf = Some(right_page_id);
                    write_node(&self.storage, next_id, &next_node).await?;
                }
                Node::Internal(_) => {
                    return Err(IndexError::Corrupt(format!(
                        "leaf split encountered non-leaf next_leaf pointer at page {next_id}"
                    )));
                }
            }
        }

        write_node(&self.storage, page_id, &Node::Leaf(leaf)).await?;
        write_node(&self.storage, right_page_id, &Node::Leaf(right)).await?;
        Ok(Some((split_result.split_key, right_page_id)))
    }

    /// Insert a pushed-up key into an internal node, splitting it if necessary.
    ///
    /// Returns the split key and new right sibling page ID if the internal node
    /// was split, `None` otherwise.
    async fn handle_internal_push_up(
        &self,
        page_id: PageId,
        mut internal: InternalNode,
        child_idx: usize,
        push_up_key: Vec<u8>,
        new_right_page_id: PageId,
        fanout: usize,
    ) -> Result<Option<(Vec<u8>, PageId)>, IndexError> {
        internal.keys.insert(child_idx, push_up_key);
        internal.children.insert(child_idx + 1, new_right_page_id);

        if internal.len() > fanout {
            let split_result = internal.split();
            let right_page_id = self.storage.allocate_page().await?;
            write_node(&self.storage, page_id, &Node::Internal(internal)).await?;
            write_node(
                &self.storage,
                right_page_id,
                &Node::Internal(split_result.right),
            )
            .await?;
            Ok(Some((split_result.push_up_key, right_page_id)))
        } else {
            write_node(&self.storage, page_id, &Node::Internal(internal)).await?;
            Ok(None)
        }
    }

    /// Recursively search for `key` in the subtree rooted at `page_id`.
    async fn search_recursive(
        &self,
        page_id: PageId,
        key: &[u8],
        height: usize,
    ) -> Result<Option<V>, IndexError> {
        let node = load_node(&self.storage, page_id).await?;
        match node {
            Node::Leaf(leaf) => match leaf.find_key(key) {
                Ok(i) => {
                    let value = V::from_bytes(&leaf.values[i])?;
                    Ok(Some(value))
                }
                Err(_) => Ok(None),
            },
            Node::Internal(internal) => {
                if height == 0 {
                    return Err(IndexError::Corrupt(format!(
                        "found internal node at height 0, page {page_id}"
                    )));
                }
                let child_idx = internal.find_child(key);
                let child_page_id = internal.children[child_idx];
                Box::pin(self.search_recursive(child_page_id, key, height - 1)).await
            }
        }
    }

    /// Recursively update the value for `key` in the subtree.
    async fn update_recursive(
        &self,
        page_id: PageId,
        key: &[u8],
        value: &[u8],
        height: usize,
        fanout: usize,
    ) -> Result<(), IndexError> {
        let node = load_node(&self.storage, page_id).await?;
        match node {
            Node::Leaf(mut leaf) => {
                if !leaf.update(key, value.to_vec()) {
                    return Err(IndexError::Corrupt(format!(
                        "key not found during update at leaf page {page_id}"
                    )));
                }
                write_node(&self.storage, page_id, &Node::Leaf(leaf)).await
            }
            Node::Internal(internal) => {
                if height == 0 {
                    return Err(IndexError::Corrupt(format!(
                        "found internal node at height 0, page {page_id}"
                    )));
                }
                let child_idx = internal.find_child(key);
                let child_page_id = internal.children[child_idx];
                Box::pin(self.update_recursive(child_page_id, key, value, height - 1, fanout)).await
            }
        }
    }

    /// Recursively delete `key` from the subtree.
    ///
    /// Returns `true` if the key was found and deleted.
    ///
    /// This implementation performs simple deletion from leaves without
    /// rebalancing (lazy deletion).  Nodes can underflow below half capacity,
    /// but tree correctness is maintained.  A future enhancement can add
    /// merge/redistribute for strict B-tree invariants.
    async fn delete_recursive(
        &self,
        page_id: PageId,
        key: &[u8],
        height: usize,
    ) -> Result<bool, IndexError> {
        let node = load_node(&self.storage, page_id).await?;

        match node {
            Node::Leaf(mut leaf) => {
                let deleted = leaf.remove(key);
                if deleted {
                    write_node(&self.storage, page_id, &Node::Leaf(leaf)).await?;
                }
                Ok(deleted)
            }
            Node::Internal(internal) => {
                if height == 0 {
                    return Err(IndexError::Corrupt(format!(
                        "found internal node at height 0, page {page_id}"
                    )));
                }
                let child_idx = internal.find_child(key);
                let child_page_id = internal.children[child_idx];

                let deleted =
                    Box::pin(self.delete_recursive(child_page_id, key, height - 1)).await?;

                if deleted {
                    let child_node = load_node(&self.storage, child_page_id).await?;
                    if is_node_empty(&child_node) && !internal.is_empty() {
                        self.remove_empty_child_from_parent(
                            page_id,
                            internal,
                            child_idx,
                            child_node,
                            child_page_id,
                        )
                        .await?;
                    }
                }

                Ok(deleted)
            }
        }
    }

    /// Remove an empty child from its parent internal node.
    ///
    /// Unlinks the child from the sibling chain (if it is a leaf), removes the
    /// corresponding separator key and child pointer from `internal`, writes the
    /// updated parent, and deallocates the child page.
    async fn remove_empty_child_from_parent(
        &self,
        page_id: PageId,
        mut internal: InternalNode,
        child_idx: usize,
        child_node: Node,
        child_page_id: PageId,
    ) -> Result<(), IndexError> {
        if let Node::Leaf(ref empty_leaf) = child_node {
            unlink_leaf_from_chain(&self.storage, empty_leaf).await?;
        }
        // Remove the separator key that was to the left of this child (when it
        // is not the first child), or the first separator key otherwise.
        if child_idx > 0 {
            // child_idx - 1 is safe: child_idx > 0 here.
            internal.keys.remove(child_idx - 1);
        } else if !internal.keys.is_empty() {
            internal.keys.remove(0);
        }
        internal.children.remove(child_idx);
        write_node(&self.storage, page_id, &Node::Internal(internal)).await?;
        self.storage.deallocate_page(child_page_id).await?;
        Ok(())
    }

    /// Descend to the leaf node that would contain `key` (or the leftmost leaf
    /// when `key` is `None`).
    ///
    /// Returns `(leaf_page_id, leaf_node)`.
    async fn find_leaf(
        &self,
        root: PageId,
        key: Option<&[u8]>,
        height: usize,
    ) -> Result<(PageId, LeafNode), IndexError> {
        let mut current_page_id = root;
        let mut current_height = height;

        loop {
            let node = load_node(&self.storage, current_page_id).await?;
            match node {
                Node::Leaf(leaf) => {
                    return Ok((current_page_id, leaf));
                }
                Node::Internal(internal) => {
                    if current_height == 0 {
                        return Err(IndexError::Corrupt(format!(
                            "found internal at height 0, page {current_page_id}"
                        )));
                    }
                    let child_idx = match key {
                        Some(k) => internal.find_child(k),
                        None => 0, // leftmost
                    };
                    current_page_id = internal.children[child_idx];
                    current_height -= 1;
                }
            }
        }
    }
}

// ── Storage helpers ───────────────────────────────────────────────────────────

/// Load and deserialize a node from storage page `page_id`.
async fn load_node(storage: &StorageEngine, page_id: PageId) -> Result<Node, IndexError> {
    let data_cap = storage.page_size() - crate::storage::page::HEADER_SIZE;
    let data = storage
        .read_page_data(page_id, 0, data_cap)
        .await
        .map_err(IndexError::Storage)?;
    // Find actual content length (trim trailing zeros after node data).
    // The node serialization is self-delimiting so we just pass the full slice.
    Node::deserialize(&data)
}

/// Serialize `node` and write it to storage page `page_id`.
async fn write_node(
    storage: &StorageEngine,
    page_id: PageId,
    node: &Node,
) -> Result<(), IndexError> {
    let bytes = node.serialize();
    let page_data_cap = storage.page_size() - crate::storage::page::HEADER_SIZE;
    if bytes.len() > page_data_cap {
        return Err(IndexError::Serialization(format!(
            "node serialization ({} bytes) exceeds page data capacity ({} bytes)",
            bytes.len(),
            page_data_cap
        )));
    }
    // Pad to full page size.
    let mut padded = vec![0u8; page_data_cap];
    padded[..bytes.len()].copy_from_slice(&bytes);

    storage
        .write_page_data(page_id, 0, &padded)
        .await
        .map_err(IndexError::Storage)
}

// ── Range-scan helpers (free functions) ──────────────────────────────────────

/// Return `true` if the first key in `leaf` already exceeds `end_bound`,
/// meaning no key in this leaf (or any subsequent leaf) can fall within the
/// range.
fn leaf_past_range_end(leaf: &LeafNode, end_bound: &EndBound) -> bool {
    match end_bound {
        EndBound::Unbounded => false,
        EndBound::Included(end) => leaf
            .keys
            .first()
            .is_some_and(|k| k.as_slice() > end.as_slice()),
        EndBound::Excluded(end) => leaf
            .keys
            .first()
            .is_some_and(|k| k.as_slice() >= end.as_slice()),
    }
}

/// Return the index of the first entry in `leaf` that satisfies the start
/// bound described by (`start`, `excluded`).
fn start_pos_in_leaf(leaf: &LeafNode, start: Option<&[u8]>, excluded: bool) -> usize {
    let Some(start) = start else {
        return 0;
    };
    let pos = leaf.keys.partition_point(|k| k.as_slice() < start);
    if excluded {
        // Skip any entries that are exactly equal to the exclusive start key.
        let mut p = pos;
        while p < leaf.len() && leaf.keys[p].as_slice() == start {
            p += 1;
        }
        p
    } else {
        pos
    }
}

/// Return `true` if `node` holds no keys/children.
fn is_node_empty(node: &Node) -> bool {
    match node {
        Node::Leaf(l) => l.is_empty(),
        Node::Internal(i) => i.is_empty() && i.children.is_empty(),
    }
}

/// Update the doubly-linked sibling pointers of the leaves that neighbour
/// `empty_leaf`, effectively removing it from the leaf chain.
async fn unlink_leaf_from_chain(
    storage: &StorageEngine,
    empty_leaf: &LeafNode,
) -> Result<(), IndexError> {
    if let Some(prev_id) = empty_leaf.prev_leaf {
        let mut prev_node = load_node(storage, prev_id).await?;
        match prev_node {
            Node::Leaf(ref mut prev_leaf) => {
                prev_leaf.next_leaf = empty_leaf.next_leaf;
                write_node(storage, prev_id, &prev_node).await?;
            }
            Node::Internal(_) => {
                return Err(IndexError::Corrupt(format!(
                    "leaf prev pointer references non-leaf page {prev_id}"
                )));
            }
        }
    }
    if let Some(next_id) = empty_leaf.next_leaf {
        let mut next_node = load_node(storage, next_id).await?;
        match next_node {
            Node::Leaf(ref mut next_leaf) => {
                next_leaf.prev_leaf = empty_leaf.prev_leaf;
                write_node(storage, next_id, &next_node).await?;
            }
            Node::Internal(_) => {
                return Err(IndexError::Corrupt(format!(
                    "leaf next pointer references non-leaf page {next_id}"
                )));
            }
        }
    }
    Ok(())
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageConfig;
    use tempfile::NamedTempFile;

    async fn make_engine(page_size: usize) -> (Arc<StorageEngine>, NamedTempFile) {
        let tmp = NamedTempFile::new().unwrap();
        let engine = StorageEngine::new(StorageConfig {
            data_path: tmp.path().to_path_buf(),
            buffer_pool_size: 512,
            page_size,
        })
        .await
        .unwrap();
        (Arc::new(engine), tmp)
    }

    async fn make_tree(page_size: usize) -> (BTree<i64, String>, NamedTempFile) {
        let (engine, tmp) = make_engine(page_size).await;
        let tree = BTree::<i64, String>::new(engine, BTreeConfig::default())
            .await
            .unwrap();
        (tree, tmp)
    }

    #[tokio::test]
    async fn insert_and_search_single() {
        let (tree, _tmp) = make_tree(4096).await;
        tree.insert(42i64, "hello".to_string()).await.unwrap();
        let v = tree.search(&42i64).await.unwrap();
        assert_eq!(v.as_deref(), Some("hello"));
    }

    #[tokio::test]
    async fn search_missing_key_returns_none() {
        let (tree, _tmp) = make_tree(4096).await;
        let v = tree.search(&99i64).await.unwrap();
        assert!(v.is_none());
    }

    #[tokio::test]
    async fn insert_duplicate_returns_error() {
        let (tree, _tmp) = make_tree(4096).await;
        tree.insert(1i64, "a".to_string()).await.unwrap();
        let result = tree.insert(1i64, "b".to_string()).await;
        assert!(matches!(result, Err(IndexError::DuplicateKey)));
    }

    #[tokio::test]
    async fn upsert_replaces_value() {
        let (tree, _tmp) = make_tree(4096).await;
        tree.upsert(1i64, "a".to_string()).await.unwrap();
        tree.upsert(1i64, "b".to_string()).await.unwrap();
        let v = tree.search(&1i64).await.unwrap();
        assert_eq!(v.as_deref(), Some("b"));
    }

    #[tokio::test]
    async fn delete_existing_key() {
        let (tree, _tmp) = make_tree(4096).await;
        tree.insert(10i64, "ten".to_string()).await.unwrap();
        assert!(tree.delete(&10i64).await.unwrap());
        let v = tree.search(&10i64).await.unwrap();
        assert!(v.is_none());
    }

    #[tokio::test]
    async fn delete_missing_key_returns_false() {
        let (tree, _tmp) = make_tree(4096).await;
        assert!(!tree.delete(&99i64).await.unwrap());
    }

    #[tokio::test]
    async fn insert_many_sequential() {
        let (tree, _tmp) = make_tree(8192).await;
        for i in 0..200i64 {
            tree.insert(i, i.to_string()).await.unwrap();
        }
        for i in 0..200i64 {
            let v = tree.search(&i).await.unwrap();
            assert_eq!(v, Some(i.to_string()), "key {i} missing");
        }
    }

    #[tokio::test]
    async fn insert_random_order() {
        let (tree, _tmp) = make_tree(8192).await;
        let mut keys: Vec<i64> = (0..100).collect();
        // Deterministic shuffle via XOR pattern.
        for i in 0..keys.len() {
            let j = (i ^ 37) % keys.len();
            keys.swap(i, j);
        }
        for &k in &keys {
            tree.insert(k, k.to_string()).await.unwrap();
        }
        for k in 0..100i64 {
            let v = tree.search(&k).await.unwrap();
            assert_eq!(v, Some(k.to_string()), "key {k} missing");
        }
    }

    #[tokio::test]
    async fn range_scan_inclusive() {
        let (tree, _tmp) = make_tree(8192).await;
        for i in 0..20i64 {
            tree.insert(i, i.to_string()).await.unwrap();
        }
        let iter = tree.range(5i64..=10i64).await.unwrap();
        let results: Vec<_> = iter.collect();
        assert_eq!(results.len(), 6);
        for (i, (k, _v)) in results.iter().enumerate() {
            let expected_key = BTreeKey::to_bytes(&(5i64 + i as i64));
            assert_eq!(k, &expected_key);
        }
    }

    #[tokio::test]
    async fn range_scan_exclusive() {
        let (tree, _tmp) = make_tree(8192).await;
        for i in 0..10i64 {
            tree.insert(i, i.to_string()).await.unwrap();
        }
        let iter = tree.range(2i64..5i64).await.unwrap();
        let results: Vec<_> = iter.collect();
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn range_scan_unbounded() {
        let (tree, _tmp) = make_tree(8192).await;
        for i in 0..10i64 {
            tree.insert(i, i.to_string()).await.unwrap();
        }
        let iter = tree.range::<std::ops::RangeFull>(..).await.unwrap();
        let results: Vec<_> = iter.collect();
        assert_eq!(results.len(), 10);
    }

    #[tokio::test]
    async fn tree_height_increases_with_inserts() {
        let (engine, _tmp) = make_engine(4096).await;
        let tree = BTree::<i64, String>::new(engine, BTreeConfig { fanout: 4 })
            .await
            .unwrap();
        let initial_height = tree.meta.read().await.height;
        assert_eq!(initial_height, 1);

        // Insert enough to cause several splits.
        for i in 0..50i64 {
            tree.insert(i, i.to_string()).await.unwrap();
        }
        let final_height = tree.meta.read().await.height;
        assert!(final_height > 1, "height should have grown: {final_height}");
    }

    #[tokio::test]
    async fn persistence_open_existing() {
        let (engine, tmp) = make_engine(4096).await;
        let meta_id = {
            let tree = BTree::<i64, String>::new(engine.clone(), BTreeConfig::default())
                .await
                .unwrap();
            for i in 0..10i64 {
                tree.insert(i, i.to_string()).await.unwrap();
            }
            tree.meta_page_id().await
        };

        // Reopen using the same engine handle.
        let tree2 = BTree::<i64, String>::open(engine, meta_id).await.unwrap();
        for i in 0..10i64 {
            let v = tree2.search(&i).await.unwrap();
            assert_eq!(v, Some(i.to_string()), "key {i} missing after reopen");
        }
        let _ = tmp;
    }

    #[tokio::test]
    async fn bulk_load_sequential() {
        let (tree, _tmp) = make_tree(8192).await;
        let entries: Vec<(i64, String)> = (0..50).map(|i: i64| (i, i.to_string())).collect();
        tree.bulk_load(entries).await.unwrap();
        assert_eq!(tree.len().await, 50);
        for i in 0..50i64 {
            assert!(tree.search(&i).await.unwrap().is_some());
        }
    }

    #[tokio::test]
    async fn len_tracks_inserts_and_deletes() {
        let (tree, _tmp) = make_tree(4096).await;
        assert_eq!(tree.len().await, 0);
        tree.insert(1i64, "a".to_string()).await.unwrap();
        tree.insert(2i64, "b".to_string()).await.unwrap();
        assert_eq!(tree.len().await, 2);
        tree.delete(&1i64).await.unwrap();
        assert_eq!(tree.len().await, 1);
    }

    #[tokio::test]
    async fn fanout_validation() {
        let (engine, _tmp) = make_engine(4096).await;
        let result = BTree::<i64, String>::new(engine, BTreeConfig { fanout: 3 }).await;
        assert!(matches!(result, Err(IndexError::InvalidFanout(3))));
    }

    #[tokio::test]
    async fn concurrent_inserts() {
        use std::sync::Arc;
        let (engine, _tmp) = make_engine(8192).await;
        let tree = Arc::new(
            BTree::<i64, String>::new(engine, BTreeConfig { fanout: 20 })
                .await
                .unwrap(),
        );

        let n_threads = 10usize;
        let keys_per_thread = 20usize;
        let mut handles = Vec::new();

        for t in 0..n_threads {
            let tree_clone = tree.clone();
            handles.push(tokio::spawn(async move {
                for i in 0..keys_per_thread {
                    let k = (t * keys_per_thread + i) as i64;
                    tree_clone.insert(k, k.to_string()).await.unwrap();
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        let total = tree.len().await;
        assert_eq!(total, (n_threads * keys_per_thread) as u64);
    }

    /// Verify that `upsert` is correctly atomic: a duplicate key results in an
    /// update (not an error), and no ghost entries are created.
    #[tokio::test]
    async fn upsert_is_atomic() {
        let (tree, _tmp) = make_tree(4096).await;
        tree.insert(1i64, "original".to_string()).await.unwrap();
        tree.upsert(1i64, "updated".to_string()).await.unwrap();
        let v = tree.search(&1i64).await.unwrap();
        assert_eq!(v.as_deref(), Some("updated"));
        // num_keys must not have incremented
        assert_eq!(tree.len().await, 1);
    }
}
