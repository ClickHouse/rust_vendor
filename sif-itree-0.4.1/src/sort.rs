use std::marker::PhantomData;

#[cfg(feature = "rayon")]
use rayon::{join, slice::ParallelSliceMut};

use crate::{ITree, Item, Node};

impl<K, V, S> ITree<K, V, S>
where
    K: Ord + Clone,
    S: AsMut<[Node<K, V>]> + FromIterator<Node<K, V>>,
{
    /// Construct a new tree by sorting the given `items`
    pub fn new<I>(items: I) -> Self
    where
        I: IntoIterator<Item = Item<K, V>>,
    {
        let mut nodes = items
            .into_iter()
            .map(|(interval, value)| {
                let end = interval.end.clone();
                ((interval, value), end)
            })
            .collect::<S>();

        {
            let nodes = nodes.as_mut();

            nodes.sort_unstable_by(|lhs, rhs| (lhs.0).0.start.cmp(&(rhs.0).0.start));

            if !nodes.is_empty() {
                update_max(nodes);
            }
        }

        Self {
            nodes,
            _marker: PhantomData,
        }
    }

    #[cfg(feature = "rayon")]
    /// Construct a new tree by sorting the given `items`, in parallel
    ///
    /// Requires the `rayon` feature and dispatches tasks into the current [thread pool][rayon::ThreadPool].
    pub fn par_new<I>(items: I) -> Self
    where
        I: IntoIterator<Item = Item<K, V>>,
        K: Send,
        V: Send,
    {
        let mut nodes = items
            .into_iter()
            .map(|(interval, value)| {
                let end = interval.end.clone();
                ((interval, value), end)
            })
            .collect::<S>();

        {
            let nodes = nodes.as_mut();

            nodes.par_sort_unstable_by(|lhs, rhs| (lhs.0).0.start.cmp(&(rhs.0).0.start));

            if !nodes.is_empty() {
                par_update_max(nodes);
            }
        }

        Self {
            nodes,
            _marker: PhantomData,
        }
    }
}

impl<K, V, S> FromIterator<Item<K, V>> for ITree<K, V, S>
where
    K: Ord + Clone,
    S: AsMut<[Node<K, V>]> + FromIterator<Node<K, V>>,
{
    fn from_iter<I>(items: I) -> Self
    where
        I: IntoIterator<Item = Item<K, V>>,
    {
        Self::new(items)
    }
}

fn update_max<K, V>(nodes: &mut [Node<K, V>]) -> K
where
    K: Ord + Clone,
{
    let (left, [mid, right @ ..]) = nodes.split_at_mut(nodes.len() / 2) else {
        unreachable!()
    };

    if !left.is_empty() {
        mid.1 = mid.1.clone().max(update_max(left));
    }

    if !right.is_empty() {
        mid.1 = mid.1.clone().max(update_max(right));
    }

    mid.1.clone()
}

#[cfg(feature = "rayon")]
fn par_update_max<K, V>(nodes: &mut [Node<K, V>]) -> K
where
    K: Ord + Clone + Send,
    V: Send,
{
    let (left, [mid, right @ ..]) = nodes.split_at_mut(nodes.len() / 2) else {
        unreachable!()
    };

    match (left.is_empty(), right.is_empty()) {
        (true, true) => (),
        (false, true) => {
            mid.1 = mid.1.clone().max(update_max(left));
        }
        (true, false) => {
            mid.1 = mid.1.clone().max(update_max(right));
        }
        (false, false) => {
            let (left, right) = join(|| update_max(left), || update_max(right));

            mid.1 = mid.1.clone().max(left.max(right));
        }
    }

    mid.1.clone()
}
