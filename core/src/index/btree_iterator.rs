//! B-tree range scan iterator.
//!
//! [`BTreeIterator`] walks leaf nodes in order, following the `next_leaf`
//! sibling pointer chain.  It supports open and closed range bounds and
//! materialises key-value pairs as `(Vec<u8>, Vec<u8>)` tuples.
//!
//! The iterator is *not* lazy with respect to storage: each call to
//! [`Iterator::next`] is synchronous and returns a value that was loaded when
//! the iterator was constructed or on the previous advance.  Because storage
//! I/O is async, the iterator pre-loads leaves in [`BTreeIterator::new`] and
//! advances lazily within an already-loaded leaf.

use crate::index::btree_node::LeafNode;
use crate::storage::page::PageId;

/// A forward-only iterator over a contiguous range of B-tree leaf entries.
///
/// Yields `(key_bytes, value_bytes)` pairs in ascending key order.
pub struct BTreeIterator {
    /// Current leaf node being iterated.
    current_leaf: LeafNode,
    /// Index within `current_leaf` for the next entry to yield.
    pos: usize,
    /// Remaining leaves to load (page IDs, in order).
    ///
    /// The iterator follows `next_leaf` pointers.  To avoid async I/O inside
    /// `next()`, additional leaves are loaded by the async helper
    /// [`BTreeIterator::advance_leaf`] which is called from [`BTree`].
    remaining_leaves: Vec<LeafNode>,
    /// Upper bound (exclusive or inclusive) as raw bytes.
    end_bound: EndBound,
}

/// Upper bound for the range.
#[derive(Clone)]
pub enum EndBound {
    /// No upper bound — iterate to the last leaf.
    Unbounded,
    /// The last key to include (inclusive).
    Included(Vec<u8>),
    /// Stop before this key (exclusive).
    Excluded(Vec<u8>),
}

impl BTreeIterator {
    /// Create a new iterator starting at position `pos` within `leaf`.
    ///
    /// `additional_leaves` contains the rest of the leaf chain (in order) that
    /// will be consumed after `leaf` is exhausted.
    pub fn new(
        leaf: LeafNode,
        pos: usize,
        additional_leaves: Vec<LeafNode>,
        end_bound: EndBound,
    ) -> Self {
        BTreeIterator {
            current_leaf: leaf,
            pos,
            remaining_leaves: additional_leaves,
            end_bound,
        }
    }

    /// Create an empty iterator (e.g. when the range has no matches).
    pub fn empty() -> Self {
        BTreeIterator {
            current_leaf: LeafNode::new(),
            pos: 0,
            remaining_leaves: Vec::new(),
            end_bound: EndBound::Unbounded,
        }
    }

    /// Return the page ID of the next leaf to load (following the sibling chain).
    pub fn next_leaf_page(&self) -> Option<PageId> {
        self.current_leaf.next_leaf
    }

    /// Advance to the next leaf in the chain.
    ///
    /// Returns `true` if a leaf was available, `false` if the chain is
    /// exhausted.
    pub fn advance_to_next_leaf(&mut self, leaf: LeafNode) {
        self.current_leaf = leaf;
        self.pos = 0;
    }

    /// Check whether the current entry at `pos` satisfies the upper bound.
    fn within_bound(&self) -> bool {
        if self.pos >= self.current_leaf.len() {
            return false;
        }
        let key = &self.current_leaf.keys[self.pos];
        match &self.end_bound {
            EndBound::Unbounded => true,
            EndBound::Included(end) => key.as_slice() <= end.as_slice(),
            EndBound::Excluded(end) => key.as_slice() < end.as_slice(),
        }
    }
}

impl Iterator for BTreeIterator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.within_bound() {
                let key = self.current_leaf.keys[self.pos].clone();
                let value = self.current_leaf.values[self.pos].clone();
                self.pos += 1;
                return Some((key, value));
            }

            // Current leaf exhausted or past the upper bound.
            // Try the pre-loaded remaining leaves.
            if let Some(next_leaf) = self.remaining_leaves.first().cloned() {
                self.remaining_leaves.remove(0);
                self.current_leaf = next_leaf;
                self.pos = 0;
                // Check if the first key in the new leaf is still within bound.
                if !self.within_bound() {
                    return None;
                }
            } else {
                return None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_leaf(pairs: &[(&str, &str)]) -> LeafNode {
        let mut leaf = LeafNode::new();
        for (k, v) in pairs {
            leaf.insert(k.as_bytes().to_vec(), v.as_bytes().to_vec())
                .unwrap();
        }
        leaf
    }

    #[test]
    fn iterate_single_leaf_unbounded() {
        let leaf = make_leaf(&[("a", "1"), ("b", "2"), ("c", "3")]);
        let mut it = BTreeIterator::new(leaf, 0, vec![], EndBound::Unbounded);
        let keys: Vec<_> = it.by_ref().map(|(k, _)| k).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn iterate_with_inclusive_end_bound() {
        let leaf = make_leaf(&[("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")]);
        let mut it = BTreeIterator::new(
            leaf,
            0,
            vec![],
            EndBound::Included(b"c".to_vec()),
        );
        let keys: Vec<_> = it.by_ref().map(|(k, _)| k).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn iterate_with_exclusive_end_bound() {
        let leaf = make_leaf(&[("a", "1"), ("b", "2"), ("c", "3")]);
        let mut it = BTreeIterator::new(
            leaf,
            0,
            vec![],
            EndBound::Excluded(b"c".to_vec()),
        );
        let keys: Vec<_> = it.by_ref().map(|(k, _)| k).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);
    }

    #[test]
    fn iterate_starting_at_offset() {
        let leaf = make_leaf(&[("a", "1"), ("b", "2"), ("c", "3")]);
        let mut it = BTreeIterator::new(leaf, 1, vec![], EndBound::Unbounded);
        let keys: Vec<_> = it.by_ref().map(|(k, _)| k).collect();
        assert_eq!(keys, vec![b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn iterate_empty_returns_none() {
        let mut it = BTreeIterator::empty();
        assert!(it.next().is_none());
    }

    #[test]
    fn iterate_across_two_leaves() {
        let leaf1 = make_leaf(&[("a", "1"), ("b", "2")]);
        let leaf2 = make_leaf(&[("c", "3"), ("d", "4")]);
        let mut it = BTreeIterator::new(leaf1, 0, vec![leaf2], EndBound::Unbounded);
        let keys: Vec<_> = it.by_ref().map(|(k, _)| k).collect();
        assert_eq!(
            keys,
            vec![
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
                b"d".to_vec()
            ]
        );
    }

    #[test]
    fn iterate_across_leaves_with_bound() {
        let leaf1 = make_leaf(&[("a", "1"), ("b", "2")]);
        let leaf2 = make_leaf(&[("c", "3"), ("d", "4")]);
        let mut it = BTreeIterator::new(
            leaf1,
            0,
            vec![leaf2],
            EndBound::Included(b"c".to_vec()),
        );
        let keys: Vec<_> = it.by_ref().map(|(k, _)| k).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }
}
