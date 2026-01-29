use super::{Entry, Key, MultiStash};
use alloc::vec;
use core::iter::{Enumerate, FusedIterator};
use core::slice;

/// Immutable [`MultiStash`] iterator.
///
/// This struct is created by [`MultiStash::iter`].
#[derive(Debug)]
pub struct Iter<'a, T> {
    /// The amount of remaining `Entry::Occupied` entries.
    remaining: usize,
    /// Iterator over the entries of the `MultiStash`.
    iter: Enumerate<slice::Iter<'a, Entry<T>>>,
}

impl<'a, T> Iter<'a, T> {
    /// Creates a new [`Iter`] for the [`MultiStash`].
    pub(crate) fn new(stash: &'a MultiStash<T>) -> Self {
        Self {
            remaining: stash.len_occupied,
            iter: stash.entries.iter().enumerate(),
        }
    }
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = (Key, usize, &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.iter.next() {
                None => return None,
                Some((_, Entry::Vacant(_))) => continue,
                Some((index, Entry::Occupied(entry))) => {
                    self.remaining -= 1;
                    return Some((Key(index), entry.remaining.get(), &entry.item));
                }
            }
        }
    }
}

impl<'a, T> DoubleEndedIterator for Iter<'a, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            match self.iter.next_back() {
                None => return None,
                Some((_, Entry::Vacant(_))) => continue,
                Some((index, Entry::Occupied(entry))) => {
                    self.remaining -= 1;
                    return Some((Key(index), entry.remaining.get(), &entry.item));
                }
            }
        }
    }
}

impl<'a, T> ExactSizeIterator for Iter<'a, T> {
    fn len(&self) -> usize {
        self.remaining
    }
}

impl<'a, T> FusedIterator for Iter<'a, T> {}

/// Mutable [`MultiStash`] iterator.
///
/// This struct is created by [`MultiStash::iter_mut`].
#[derive(Debug)]
pub struct IterMut<'a, T> {
    /// The amount of remaining `Entry::Occupied` entries.
    remaining: usize,
    /// Iterator over the entries of the `MultiStash`.
    iter: Enumerate<slice::IterMut<'a, Entry<T>>>,
}

impl<'a, T> IterMut<'a, T> {
    /// Creates a new [`IterMut`] for the [`MultiStash`].
    pub(crate) fn new(stash: &'a mut MultiStash<T>) -> Self {
        Self {
            remaining: stash.len_occupied,
            iter: stash.entries.iter_mut().enumerate(),
        }
    }
}

impl<'a, T> Iterator for IterMut<'a, T> {
    type Item = (Key, usize, &'a mut T);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.iter.next() {
                None => return None,
                Some((_, Entry::Vacant(_))) => continue,
                Some((index, Entry::Occupied(entry))) => {
                    self.remaining -= 1;
                    return Some((Key(index), entry.remaining.get(), &mut entry.item));
                }
            }
        }
    }
}

impl<'a, T> DoubleEndedIterator for IterMut<'a, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            match self.iter.next_back() {
                None => return None,
                Some((_, Entry::Vacant(_))) => continue,
                Some((index, Entry::Occupied(entry))) => {
                    self.remaining -= 1;
                    return Some((Key(index), entry.remaining.get(), &mut entry.item));
                }
            }
        }
    }
}

impl<'a, T> ExactSizeIterator for IterMut<'a, T> {
    fn len(&self) -> usize {
        self.remaining
    }
}

impl<'a, T> FusedIterator for IterMut<'a, T> {}

/// An iterator that moves out of a [`MultiStash`].
///
/// This `struct` is created by the `into_iter` method on [`MultiStash`]
/// (provided by the [`IntoIterator`] trait).
#[derive(Debug)]
pub struct IntoIter<T> {
    /// The amount of remaining `Entry::Occupied` entries.
    remaining: usize,
    /// Iterator over the entries of the `MultiStash`.
    iter: Enumerate<vec::IntoIter<Entry<T>>>,
}

impl<T> IntoIter<T> {
    /// Creates a new [`IntoIter`] for the [`MultiStash`].
    pub(crate) fn new(stash: MultiStash<T>) -> Self {
        Self {
            remaining: stash.len_occupied,
            iter: stash.entries.into_iter().enumerate(),
        }
    }
}

impl<T> Iterator for IntoIter<T> {
    type Item = (Key, usize, T);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.iter.next() {
                None => return None,
                Some((_, Entry::Vacant(_))) => continue,
                Some((index, Entry::Occupied(entry))) => {
                    self.remaining -= 1;
                    return Some((Key(index), entry.remaining.get(), entry.item));
                }
            }
        }
    }
}

impl<T> DoubleEndedIterator for IntoIter<T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            match self.iter.next_back() {
                None => return None,
                Some((_, Entry::Vacant(_))) => continue,
                Some((index, Entry::Occupied(entry))) => {
                    self.remaining -= 1;
                    return Some((Key(index), entry.remaining.get(), entry.item));
                }
            }
        }
    }
}

impl<T> ExactSizeIterator for IntoIter<T> {
    fn len(&self) -> usize {
        self.remaining
    }
}

impl<T> FusedIterator for IntoIter<T> {}
