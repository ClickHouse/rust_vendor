use alloc::vec;
use alloc::vec::Vec;
use crate::bin_key::index::{BinKey, BinLayoutOp};
use core::cmp::Ordering;
use core::ptr;
use crate::sort::layout::BinStore;

#[derive(Debug, Clone)]
pub struct Bin {
    pub offset: usize,
    pub data: usize,
}

pub trait KeyBinSort<T> {
    type Item;
    fn sort_by_bins(&mut self) -> Vec<Bin>;
    fn sort_with_bins<F>(&mut self, compare: F)
    where
        F: Fn(&Self::Item, &Self::Item) -> Ordering;
    fn sort_unstable_with_bins<F>(&mut self, compare: F)
    where
        F: Fn(&Self::Item, &Self::Item) -> Ordering;
}

impl<T, U> KeyBinSort<U> for [T]
where
    T: BinKey<U> + Clone,
    U: Copy + Ord + BinLayoutOp,
{
    type Item = T;

    fn sort_by_bins(&mut self) -> Vec<Bin> {
        let first_key = if let Some(item) = self.first() {
            item.bin_key()
        } else {
            return vec![];
        };

        let (max_key, min_key) = {
            let mut max_key = first_key;
            let mut min_key = first_key;

            for p in self.iter() {
                let key = p.bin_key();
                min_key = key.min(min_key);
                max_key = key.max(max_key);
            }

            (max_key, min_key)
        };

        let mut store = if let Some(store) = BinStore::new(min_key, max_key, self.len()) {
            store
        } else {
            return vec![Bin {
                offset: 0,
                data: self.len(),
            }];
        };

        store.layout_bins(self.iter());
        let layout = store.layout;
        let mut bins = store.bins;

        let mut unused = Vec::with_capacity(self.len() >> 1);
        let last_bin = bins.len() - 1;

        // move items from all bins except last
        let mut start = bins.first().unwrap().offset;
        for cursor in 0..last_bin {
            let end = unsafe { bins.get_unchecked(cursor + 1)}.offset;
            if start < end {
                let mut i0 = start;
                for index in start..end {
                    unsafe {
                        let src_ptr = self.as_ptr().add(index);
                        let bin_index = (*src_ptr).bin_index(&layout);
                        if bin_index > cursor { continue }

                        if i0 < index {
                            unused.extend_from_slice(&self[i0..index]);
                        }

                        let bin = bins.get_unchecked_mut(bin_index);
                        if bin.data != index {
                            let dst_ptr = self.as_mut_ptr().add(bin.data);
                            ptr::copy_nonoverlapping(src_ptr, dst_ptr, 1);
                        }
                        bin.data += 1;
                    }
                    i0 = index + 1;
                }
                if i0 < end {
                    unused.extend_from_slice(&self[i0..end]);
                }

                start = end;
            }
        }

        // move items from last bin
        {
            let start = unsafe { bins.get_unchecked(last_bin).offset };
            for index in start..self.len() {
                unsafe {
                    let src_ptr = self.as_ptr().add(index);
                    let bin_index = (*src_ptr).bin_index(&layout);
                    let bin = bins.get_unchecked_mut(bin_index);
                    if bin.data != index {
                        let dst_ptr = self.as_mut_ptr().add(bin.data);
                        ptr::copy_nonoverlapping(src_ptr, dst_ptr, 1);
                    }
                    bin.data += 1;
                }
            }
        }

        // move unused items
        for item in unused.into_iter() {
            let index = item.bin_index(&layout);
            unsafe {
                let bin = bins.get_unchecked_mut(index);
                *self.get_unchecked_mut(bin.data) = item;
                bin.data += 1;
            }
        }

        bins
    }

    #[inline]
    fn sort_with_bins<F>(&mut self, compare: F)
    where
        F: Fn(&T, &T) -> Ordering,
    {
        if self.len() <= 256 {
            self.sort_by(|a, b| compare(a, b));
            return;
        }
        let bins = self.sort_by_bins();

        for bin in bins.iter() {
            let start = bin.offset;
            let end = bin.data;
            if start < end {
                self[start..end].sort_by(|a, b| compare(a, b));
            }
        }
    }

    #[inline]
    fn sort_unstable_with_bins<F>(&mut self, compare: F)
    where
        F: Fn(&T, &T) -> Ordering,
    {
        if self.len() <= 256 {
            self.sort_unstable_by(|a, b| compare(a, b));
            return;
        }

        let bins = self.sort_by_bins();

        for bin in bins.iter() {
            let start = bin.offset;
            let end = bin.data;
            if start < end {
                self[start..end].sort_unstable_by(|a, b| compare(a, b));
            }
        }
    }
}