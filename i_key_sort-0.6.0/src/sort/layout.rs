use alloc::vec;
use alloc::vec::Vec;
use crate::bin_key::index::{BinKey, BinLayout, BinLayoutOp};
use crate::sort::key_sort::Bin;
use core::cmp::Ordering;

pub struct BinStore<U> {
    pub layout: BinLayout<U>,
    pub bins: Vec<Bin>,
}

impl<U: Copy + Ord + BinLayoutOp> BinStore<U> {

    #[inline]
    pub fn empty(min_key: U, capacity: usize) -> Self {
        let layout = BinLayout { min_key, max_key: min_key, power: 0 };
        let bins = Vec::with_capacity(capacity);

        Self { layout, bins }
    }

    #[inline]
    pub fn init(&mut self, layout: BinLayout<U>) {
        let bin_count = layout.count();
        self.layout = layout;
        if self.bins.capacity() < bin_count {
            self.bins.reserve(bin_count - self.bins.capacity());
        }
        self.bins.clear();
        for _ in 0..bin_count {
            self.bins.push(Bin { offset: 0, data: 0 });
        }
    }

    #[inline]
    pub fn new(min: U, max: U, count: usize) -> Option<Self> {
        let layout = BinLayout::new(min..max, count)?;
        let bin_count = layout.index(max) + 1;
        let bins = vec![Bin { offset: 0, data: 0 }; bin_count];

        Some(Self { layout, bins })
    }

    #[inline]
    pub fn layout_bins<'a, I, T>(&mut self, iter: I)
    where
        I: Iterator<Item = &'a T>,
        T: 'a + BinKey<U> + Clone,
    {
        self.reserve_bins_space(iter);
        self.prepare_bins();
    }

    #[inline]
    pub fn reserve_bins_space<'a, I, T>(&mut self, iter: I)
    where
        I: Iterator<Item = &'a T>,
        T: 'a + BinKey<U> + Clone,
    {
        // calculate capacity for each bin
        for p in iter {
            let index = p.bin_index(&self.layout);
            unsafe { self.bins.get_unchecked_mut(index) }.data += 1;
        }
    }

    #[inline]
    pub fn prepare_bins(&mut self) -> usize {
        // calculate range for each bin
        let mut offset = 0;
        for bin in self.bins.iter_mut() {
            let next_offset = offset + bin.data;
            bin.offset = offset; // offset from start
            bin.data = offset; // iterator cursor
            offset = next_offset;
        }
        offset
    }

    #[inline]
    pub fn into_sorted_by_bins_vec<I, T, F>(mut self, count: usize, iter: I, compare: F) -> Vec<T>
    where
        I: IntoIterator<Item = T>,
        T: BinKey<U> + Clone + Default,
        F: Fn(&T, &T) -> Ordering,
    {
        let mut result = vec![T::default(); count];

        for p in iter {
            self.feed_vec(&mut result, p);
        }

        for bin in self.bins.iter() {
            let start = bin.offset;
            let end = bin.data;
            if start < end {
                result[start..end].sort_unstable_by(|a, b| compare(a, b));
            }
        }

        result.sort_by(compare);

        result
    }

    #[inline]
    pub fn feed_vec<T>(&mut self, vec: &mut [T], item: T)
    where
        T: BinKey<U>,
    {
        let index = item.bin_index(&self.layout);
        unsafe {
            let bin = self.bins.get_unchecked_mut(index);
            let item_index = bin.data;
            bin.data += 1;

            *vec.get_unchecked_mut(item_index) = item;
        }
    }
}

#[cfg(test)]
mod tests {
    use alloc::vec;
    use alloc::vec::Vec;
    use crate::sort::layout::BinStore;
    use crate::sort::min_max::min_max;
    use rand::Rng;
    use core::cmp::Ordering::Greater;

    #[test]
    fn test_0() {
        let array = vec![8, 3, 2, 6, 2, 1];
        let &min = array.iter().min().unwrap();
        let &max = array.iter().max().unwrap();
        let count = array.len();

        let mut store = BinStore::new(min, max, count).unwrap();
        store.layout_bins(array.iter());

        let bin_sorted = store.into_sorted_by_bins_vec(count, array.into_iter(), |a, b| a.cmp(b));

        assert_eq!(bin_sorted.len(), count);
        for w in bin_sorted.windows(2) {
            assert_ne!(w[0].cmp(&w[1]), Greater);
        }
    }

    #[test]
    fn test_random_bin_sort() {
        const COUNT: usize = 1000;
        let mut rng = rand::rng();

        for _ in 0..100 {
            let array: Vec<i32> = (0..COUNT).map(|_| rng.random_range(0..100)).collect();

            let (&min, &max) = min_max(array.iter()).unwrap();

            let mut store = if let Some(store) = BinStore::new(min, max, COUNT) {
                store
            } else {
                continue;
            };
            store.layout_bins(array.iter());

            let sorted = store.into_sorted_by_bins_vec(COUNT, array.into_iter(), |a, b| a.cmp(b));

            assert_eq!(sorted.len(), COUNT);

            for w in sorted.windows(2) {
                assert_ne!(w[0].cmp(&w[1]), Greater);
            }
        }
    }
}

impl BinKey<i32> for i32 {
    #[inline]
    fn bin_key(&self) -> i32 {
        *self
    }

    #[inline]
    fn bin_index(&self, layout: &BinLayout<i32>) -> usize {
        layout.index(*self)
    }
}
