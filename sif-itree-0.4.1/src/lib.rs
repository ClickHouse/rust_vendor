#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

//! A simple library implementing an immutable, flat representation of an [augmented interval tree](https://en.wikipedia.org/wiki/Interval_tree#Augmented_tree)
//!
//! Supports querying for overlapping intervals without temporary allocations and uses a flat memory layout that can be backed by memory maps.

mod query;
mod sort;

use std::marker::PhantomData;
use std::ops::{Deref, Range};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// The items stored in the tree consisting of an interval and an associated value
pub type Item<K, V> = (Range<K>, V);

/// The nodes of which the tree is built consisting of an item and the maximum of the interval upper bounds in the subtree
pub type Node<K, V> = (Item<K, V>, K);

/// Interval tree mapping half-open or closed intervals with boundaries of type `K` to values of type `V`
#[derive(Debug, Default, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct ITree<K, V, S = Box<[Node<K, V>]>> {
    nodes: S,
    _marker: PhantomData<(K, V)>,
}

impl<K, V, S> Deref for ITree<K, V, S>
where
    S: AsRef<[Node<K, V>]>,
{
    type Target = [Node<K, V>];

    fn deref(&self) -> &Self::Target {
        self.nodes.as_ref()
    }
}

impl<K, V, S> AsRef<[Node<K, V>]> for ITree<K, V, S>
where
    S: AsRef<[Node<K, V>]>,
{
    fn as_ref(&self) -> &[Node<K, V>] {
        self.nodes.as_ref()
    }
}

impl<K, V, S> ITree<K, V, S>
where
    S: AsRef<[Node<K, V>]>,
{
    /// Interprets the given `nodes` as a tree
    ///
    /// Supplying `nodes` which are not actually organized as an interval tree is safe but will lead to incorrect results.
    pub fn new_unchecked(nodes: S) -> Self {
        Self {
            nodes,
            _marker: PhantomData,
        }
    }

    /// Iterate over all intervals
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &Item<K, V>> {
        self.nodes.as_ref().iter().map(|node| &node.0)
    }
}
