use std::ops::{ControlFlow, Range, RangeInclusive};

#[cfg(feature = "rayon")]
use rayon::join;

use crate::{ITree, Item, Node};

impl<K, V, S> ITree<K, V, S>
where
    S: AsRef<[Node<K, V>]>,
{
    /// Query for all intervals overlapping the given interval
    ///
    /// The stored intervals are interpeted as half-open or closed,
    /// depending on the type of the given query interval,
    /// either [`Range`] or [`RangeInclusive`].
    pub fn query<'a, I, H, R>(&'a self, interval: I, handler: H) -> ControlFlow<R>
    where
        K: Ord,
        I: Interval<K>,
        H: FnMut(&'a Item<K, V>) -> ControlFlow<R>,
    {
        let nodes = self.nodes.as_ref();

        if !nodes.is_empty() {
            query(&mut QueryArgs { interval, handler }, nodes)?;
        }

        ControlFlow::Continue(())
    }

    #[cfg(feature = "rayon")]
    /// Query for all intervals overlapping the given interval, in parallel
    ///
    /// The stored intervals are interpeted as half-open or closed,
    /// depending on the type of the given query interval,
    /// either [`Range`] or [`RangeInclusive`].
    pub fn par_query<'a, I, H, R>(&'a self, interval: I, handler: H) -> ControlFlow<R>
    where
        K: Ord + Send + Sync,
        V: Sync,
        I: Interval<K> + Sync,
        H: Fn(&'a Item<K, V>) -> ControlFlow<R> + Sync,
        R: Send,
    {
        let nodes = self.nodes.as_ref();

        if !nodes.is_empty() {
            par_query(&QueryArgs { interval, handler }, nodes)?;
        }

        ControlFlow::Continue(())
    }
}

pub trait Interval<K> {
    fn go_left(&self, max: &K) -> bool;
    fn go_right(&self, start: &K) -> bool;
    fn overlaps(&self, end: &K) -> bool;
}

impl<K> Interval<K> for Range<K>
where
    K: Ord,
{
    fn go_left(&self, max: &K) -> bool {
        &self.start < max
    }

    fn go_right(&self, start: &K) -> bool {
        &self.end > start
    }

    fn overlaps(&self, end: &K) -> bool {
        &self.start < end
    }
}

impl<K> Interval<K> for RangeInclusive<K>
where
    K: Ord,
{
    fn go_left(&self, max: &K) -> bool {
        self.start() <= max
    }

    fn go_right(&self, start: &K) -> bool {
        self.end() >= start
    }

    fn overlaps(&self, end: &K) -> bool {
        self.start() <= end
    }
}

struct QueryArgs<I, H> {
    interval: I,
    handler: H,
}

fn query<'a, I, H, K, V, R>(
    args: &mut QueryArgs<I, H>,
    mut nodes: &'a [Node<K, V>],
) -> ControlFlow<R>
where
    K: Ord,
    I: Interval<K>,
    H: FnMut(&'a (Range<K>, V)) -> ControlFlow<R>,
{
    loop {
        let (left, [mid, right @ ..]) = nodes.split_at(nodes.len() / 2) else {
            unreachable!()
        };

        let mut go_left = false;
        let mut go_right = false;

        if args.interval.go_left(&mid.1) {
            if !left.is_empty() {
                go_left = true;
            }

            if args.interval.go_right(&(mid.0).0.start) {
                if !right.is_empty() {
                    go_right = true;
                }

                if args.interval.overlaps(&(mid.0).0.end) {
                    (args.handler)(&mid.0)?;
                }
            }
        }

        match (go_left, go_right) {
            (true, true) => {
                query(args, left)?;

                nodes = right;
            }
            (true, false) => nodes = left,
            (false, true) => nodes = right,
            (false, false) => return ControlFlow::Continue(()),
        }
    }
}

#[cfg(feature = "rayon")]
fn par_query<'a, I, H, K, V, R>(
    args: &QueryArgs<I, H>,
    mut nodes: &'a [Node<K, V>],
) -> ControlFlow<R>
where
    K: Ord + Send + Sync,
    V: Sync,
    I: Interval<K> + Sync,
    H: Fn(&'a (Range<K>, V)) -> ControlFlow<R> + Sync,
    R: Send,
{
    loop {
        let (left, [mid, right @ ..]) = nodes.split_at(nodes.len() / 2) else {
            unreachable!()
        };

        let mut go_left = false;
        let mut go_right = false;

        if args.interval.go_left(&mid.1) {
            if !left.is_empty() {
                go_left = true;
            }

            if args.interval.go_right(&(mid.0).0.start) {
                if !right.is_empty() {
                    go_right = true;
                }

                if args.interval.overlaps(&(mid.0).0.end) {
                    (args.handler)(&mid.0)?;
                }
            }
        }

        match (go_left, go_right) {
            (true, true) => {
                let (left, right) = join(|| par_query(args, left), || par_query(args, right));

                left?;
                right?;

                return ControlFlow::Continue(());
            }
            (true, false) => nodes = left,
            (false, true) => nodes = right,
            (false, false) => return ControlFlow::Continue(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "rayon")]
    use std::sync::Mutex;

    use proptest::{collection::vec, test_runner::TestRunner};

    #[test]
    fn query_random() {
        const DOM: Range<i32> = -1000..1000;
        const LEN: usize = 1000_usize;

        TestRunner::default()
            .run(
                &(vec(DOM, LEN), vec(DOM, LEN), DOM, DOM),
                |(start, end, query_start, query_end)| {
                    let tree = ITree::<_, _>::new(
                        start
                            .iter()
                            .zip(&end)
                            .map(|(&start, &end)| (start..end, ())),
                    );

                    let mut result1 = Vec::new();
                    tree.query(query_start..query_end, |(range, ())| {
                        result1.push(range);
                        ControlFlow::<()>::Continue(())
                    })
                    .continue_value()
                    .unwrap();

                    let mut result2 = tree
                        .iter()
                        .filter(|(range, ())| query_end > range.start && query_start < range.end)
                        .map(|(range, ())| range)
                        .collect::<Vec<_>>();

                    result1.sort_unstable_by_key(|range| (range.start, range.end));
                    result2.sort_unstable_by_key(|range| (range.start, range.end));
                    assert_eq!(result1, result2);

                    Ok(())
                },
            )
            .unwrap()
    }

    #[test]
    fn query_random_inclusive() {
        const DOM: Range<i32> = -1000..1000;
        const LEN: usize = 1000_usize;

        TestRunner::default()
            .run(
                &(vec(DOM, LEN), vec(DOM, LEN), DOM, DOM),
                |(start, end, query_start, query_end)| {
                    let tree = ITree::<_, _>::new(
                        start
                            .iter()
                            .zip(&end)
                            .map(|(&start, &end)| (start..end, ())),
                    );

                    let mut result1 = Vec::new();
                    tree.query(query_start..=query_end, |(range, ())| {
                        result1.push(range);
                        ControlFlow::<()>::Continue(())
                    })
                    .continue_value()
                    .unwrap();

                    let mut result2 = tree
                        .iter()
                        .filter(|(range, ())| query_end >= range.start && query_start <= range.end)
                        .map(|(range, ())| range)
                        .collect::<Vec<_>>();

                    result1.sort_unstable_by_key(|range| (range.start, range.end));
                    result2.sort_unstable_by_key(|range| (range.start, range.end));
                    assert_eq!(result1, result2);

                    Ok(())
                },
            )
            .unwrap()
    }

    #[cfg(feature = "rayon")]
    #[test]
    fn par_query_random() {
        const DOM: Range<i32> = -1000..1000;
        const LEN: usize = 1000_usize;

        TestRunner::default()
            .run(
                &(vec(DOM, LEN), vec(DOM, LEN), DOM, DOM),
                |(start, end, query_start, query_end)| {
                    let tree = ITree::<_, _>::par_new(
                        start
                            .iter()
                            .zip(&end)
                            .map(|(&start, &end)| (start..end, ())),
                    );

                    let result1 = Mutex::new(Vec::new());
                    tree.par_query(query_start..query_end, |(range, ())| {
                        result1.lock().unwrap().push(range);
                        ControlFlow::<()>::Continue(())
                    })
                    .continue_value()
                    .unwrap();
                    let mut result1 = result1.into_inner().unwrap();

                    let mut result2 = tree
                        .iter()
                        .filter(|(range, ())| query_end > range.start && query_start < range.end)
                        .map(|(range, ())| range)
                        .collect::<Vec<_>>();

                    result1.sort_unstable_by_key(|range| (range.start, range.end));
                    result2.sort_unstable_by_key(|range| (range.start, range.end));
                    assert_eq!(result1, result2);

                    Ok(())
                },
            )
            .unwrap()
    }

    #[cfg(feature = "rayon")]
    #[test]
    fn par_query_random_inclusive() {
        const DOM: Range<i32> = -1000..1000;
        const LEN: usize = 1000_usize;

        TestRunner::default()
            .run(
                &(vec(DOM, LEN), vec(DOM, LEN), DOM, DOM),
                |(start, end, query_start, query_end)| {
                    let tree = ITree::<_, _>::par_new(
                        start
                            .iter()
                            .zip(&end)
                            .map(|(&start, &end)| (start..end, ())),
                    );

                    let result1 = Mutex::new(Vec::new());
                    tree.par_query(query_start..=query_end, |(range, ())| {
                        result1.lock().unwrap().push(range);
                        ControlFlow::<()>::Continue(())
                    })
                    .continue_value()
                    .unwrap();
                    let mut result1 = result1.into_inner().unwrap();

                    let mut result2 = tree
                        .iter()
                        .filter(|(range, ())| query_end >= range.start && query_start <= range.end)
                        .map(|(range, ())| range)
                        .collect::<Vec<_>>();

                    result1.sort_unstable_by_key(|range| (range.start, range.end));
                    result2.sort_unstable_by_key(|range| (range.start, range.end));
                    assert_eq!(result1, result2);

                    Ok(())
                },
            )
            .unwrap()
    }
}
