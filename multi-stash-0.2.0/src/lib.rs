#![no_std]

mod entry;
mod iter;

#[cfg(test)]
mod tests;

extern crate alloc;

use self::entry::{Entry, OccupiedEntry, VacantEntry};
pub use self::iter::{IntoIter, Iter, IterMut};
use alloc::vec::Vec;
use core::mem;
use core::num::NonZeroUsize;
use core::ops::{Index, IndexMut};

/// A vector-like data structure that is able to reuse slots for new elements.
///
/// Specifically allows for (armortized) O(1) instructions for:
///
/// - [`MultiStash::put`]
/// - [`MultiStash::take_one`]
/// - [`MultiStash::take_all`]
/// - [`MultiStash::get`]
/// - [`MultiStash::get_mut`]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MultiStash<T> {
    /// The next vacant or free slot to allocate.
    free: usize,
    /// The number of items stored in the [`MultiStash`].
    ///
    /// # Note
    ///
    /// Each [`Entry::Occupied`] might store multiple items.
    len_items: usize,
    /// The number of occupied entries in the [`MultiStash`].
    ///
    /// # Note
    ///
    /// Each [`Entry::Occupied`] might store multiple items.
    len_occupied: usize,
    /// The entries of the [`MultiStash`].
    entries: Vec<Entry<T>>,
}

/// Allows to access elements stored in a [`MultiStash`].
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key(usize);

impl From<usize> for Key {
    #[inline]
    fn from(index: usize) -> Self {
        Self(index)
    }
}

impl From<Key> for usize {
    #[inline]
    fn from(key: Key) -> Self {
        key.0
    }
}

impl<T> Default for MultiStash<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> MultiStash<T> {
    /// Construct a new, empty [`MultiStash`].
    ///
    /// The [`MultiStash`] will not allocate until items are put into it.
    pub fn new() -> Self {
        Self {
            free: 0,
            len_items: 0,
            len_occupied: 0,
            entries: Vec::new(),
        }
    }

    /// Constructs a new, empty [`MultiStash`] with at least the specified capacity.
    ///
    /// The [`MultiStash`] will be able to hold at least `capacity` elements without reallocating.
    /// This method is allowed to allocate for more elements than `capacity`.
    /// If `capacity` is 0, the [`MultiStash`] will not allocate.
    ///
    /// It is important to note that although the returned [`MultiStash`] has the minimum
    /// *capacity* specified, the [`MultiStash`] will have a zero length.
    /// For an explanation of the difference between length and capacity, see *[Capacity and reallocation]*.
    ///
    /// If it is important to know the exact allocated capacity of a [`MultiStash`],
    /// always use the [`capacity`] method after construction.
    ///
    /// # Panics
    ///
    /// Panics if the new capacity exceeds `isize::MAX` bytes.
    ///
    /// [Capacity and reallocation]: https://doc.rust-lang.org/std/vec/struct.Vec.html#capacity-and-reallocation
    /// [`capacity`]: MultiStash::capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            free: 0,
            len_items: 0,
            len_occupied: 0,
            entries: Vec::with_capacity(capacity),
        }
    }

    /// Returns the total number of elements the [`MultiStash`] can hold without reallocating.
    pub fn capacity(&self) -> usize {
        self.entries.capacity()
    }

    /// Reserves capacity for at least `additional` more elements to be inserted
    /// in the given [`MultiStash`]. The collection may reserve more space to
    /// speculatively avoid frequent reallocations. After calling `reserve`,
    /// capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    ///
    /// # Panics
    ///
    /// Panics if the new capacity exceeds `isize::MAX` bytes.
    pub fn reserve(&mut self, additional: usize) {
        self.entries.reserve(additional);
    }

    /// Reserves the minimum capacity for at least `additional` more elements to
    /// be inserted in the given [`MultiStash`]. Unlike [`reserve`], this will not
    /// deliberately over-allocate to speculatively avoid frequent allocations.
    /// After calling `reserve_exact`, capacity will be greater than or equal to
    /// `self.len() + additional`. Does nothing if the capacity is already
    /// sufficient.
    ///
    /// Note that the allocator may give the collection more space than it
    /// requests. Therefore, capacity can not be relied upon to be precisely
    /// minimal. Prefer [`reserve`] if future insertions are expected.
    ///
    /// [`reserve`]: MultiStash::reserve
    ///
    /// # Panics
    ///
    /// Panics if the new capacity exceeds `isize::MAX` bytes.
    pub fn reserve_exact(&mut self, additional: usize) {
        self.entries.reserve_exact(additional);
    }

    /// Returns the number of vacant or occupied [`Entry`] in the [`MultiStash`].
    fn len_entries(&self) -> usize {
        self.entries.len()
    }

    /// Returns the number of items in the [`MultiStash`].
    ///
    /// # Note
    ///
    /// A single element might store multiple items.
    pub fn len_items(&self) -> usize {
        self.len_items
    }

    /// Returns the number of elements in the [`MultiStash`].
    ///
    /// # Note
    ///
    /// A single element might store multiple items.
    fn len_occupied(&self) -> usize {
        self.len_occupied
    }

    /// Returns the number of elements in the [`MultiStash`].
    ///
    /// # Note
    ///
    /// A single element might store multiple items.
    pub fn len(&self) -> usize {
        self.len_occupied()
    }

    /// Returns `true` if the [`MultiStash`] contains no elements.
    pub fn is_empty(&self) -> bool {
        self.len_occupied() == 0
    }

    /// Returns a reference to an element at the `key` if any.
    pub fn get(&self, key: Key) -> Option<(usize, &T)> {
        match self.entries.get(key.0) {
            Some(Entry::Occupied(entry)) => Some((entry.remaining.get(), &entry.item)),
            _ => None,
        }
    }

    /// Returns a mutable reference to an element at the `key` if any.
    pub fn get_mut(&mut self, key: Key) -> Option<(usize, &mut T)> {
        match self.entries.get_mut(key.0) {
            Some(Entry::Occupied(entry)) => Some((entry.remaining.get(), &mut entry.item)),
            _ => None,
        }
    }

    /// Puts an `amount` of `item` into the [`MultiStash`].
    ///
    /// # Panics
    ///
    /// Panics if the new capacity exceeds `isize::MAX` bytes.
    pub fn put(&mut self, amount: NonZeroUsize, item: T) -> Key {
        let key = Key(self.free);
        self.free = if self.free == self.len_entries() {
            self.entries
                .push(Entry::from(OccupiedEntry::new(item, amount)));
            self.free.checked_add(1).unwrap()
        } else {
            // # Safety: It is an invariant of `MultiStash` that `self.free` only ever stores
            //           indices to populated entries in `self.items` if `self.free != self.len_entries()`.
            let cell = unsafe { self.entries.get_unchecked_mut(self.free) };
            match mem::replace(cell, Entry::from(OccupiedEntry::new(item, amount))) {
                Entry::Vacant(entry) => entry.next_free,
                _ => unreachable!(
                    "asserted that the entry at `self.free` ({}) is vacant",
                    self.free
                ),
            }
        };
        self.bump_len_items(amount.get());
        self.len_occupied += 1;
        key
    }

    /// Bumps the number of items in the [`MultiStash`] by `amount`.
    ///
    /// # Panics
    ///
    /// If the number of items in the [`MultiStash`] overflows.
    fn bump_len_items(&mut self, amount: usize) {
        self.len_items = self.len_items.checked_add(amount).unwrap_or_else(|| {
            panic!(
                "failed to add {} items to MultiStash of length {}",
                amount, self.len_items
            )
        });
    }

    /// Clears the [`MultiStash`], removing all elements.
    ///
    /// Note that this method has no effect on the allocated capacity of the vector.
    pub fn clear(&mut self) {
        self.free = 0;
        self.len_items = 0;
        self.len_occupied = 0;
        self.entries.clear();
    }

    /// Removes and returns the `element` at `key` and its amount of remaining items.
    ///
    /// Returns `None` if `key` refers to a vacant entry or is out of bounds.
    pub fn take_all(&mut self, key: Key) -> Option<(usize, T)> {
        let index = key.0;
        let taken = match self.entries.get_mut(index) {
            None => None,
            Some(entry) => match mem::replace(entry, Entry::from(VacantEntry::new(self.free))) {
                Entry::Vacant(vacant) => {
                    *entry = Entry::from(VacantEntry::new(vacant.next_free));
                    None
                }
                Entry::Occupied(occupied) => {
                    self.free = index;
                    let item = occupied.item;
                    let len_taken = occupied.remaining.get();
                    self.len_items -= len_taken;
                    self.len_occupied -= 1;
                    Some((len_taken, item))
                }
            },
        };
        if self.is_empty() {
            self.clear()
        }
        taken
    }

    /// Bumps the amount of items of the element at `key` if any.
    ///
    /// Returns `None` if not element is found at the `key`.
    ///
    /// # Panics
    ///
    /// Panics if `amount` of the element at `key` overflows.
    pub fn bump(&mut self, key: Key, amount: usize) -> Option<usize> {
        let index = key.0;
        match self.entries.get_mut(index)? {
            Entry::Vacant(_) => None,
            Entry::Occupied(entry) => {
                let old_amount = entry.remaining;
                let new_amount = old_amount.checked_add(amount).unwrap_or_else(|| {
                    panic!(
                        "overflow when adding {} to the amount of MultiStash element at {}",
                        amount, index,
                    )
                });
                entry.remaining = new_amount;
                self.bump_len_items(amount);
                Some(old_amount.get())
            }
        }
    }

    /// Returns an iterator over the elements of the [`MultiStash`].
    ///
    /// The iterator yields all elements, their keys and remaining items from start to end.
    pub fn iter(&self) -> Iter<T> {
        Iter::new(self)
    }

    /// Returns an iterator over the elements of the [`MultiStash`].
    ///
    /// The iterator yields mutable references to all elements, their keys and remaining items from start to end.
    pub fn iter_mut(&mut self) -> IterMut<T> {
        IterMut::new(self)
    }
}

impl<T: Clone> MultiStash<T> {
    /// Returns a single item of the `element` at `key`
    /// and the amount of remaining items after this operation.
    ///
    /// Remove the `element` if no items are left after this operation.
    /// Returns `None` if `key` refers to a vacant entry or is out of bounds.
    pub fn take_one(&mut self, key: Key) -> Option<(usize, T)> {
        let index = key.0;
        let taken = match self.entries.get_mut(index) {
            None => None,
            Some(entry) => match mem::replace(entry, Entry::from(VacantEntry::new(self.free))) {
                Entry::Vacant(vacant) => {
                    *entry = Entry::from(VacantEntry::new(vacant.next_free));
                    None
                }
                Entry::Occupied(occupied) => {
                    let item = occupied.item;
                    self.len_items -= 1;
                    match NonZeroUsize::new(occupied.remaining.get().wrapping_sub(1)) {
                        Some(remaining) => {
                            *entry = Entry::from(OccupiedEntry::new(item.clone(), remaining));
                            Some((remaining.get(), item))
                        }
                        None => {
                            self.len_occupied -= 1;
                            self.free = index;
                            Some((0, item))
                        }
                    }
                }
            },
        };
        if self.is_empty() {
            self.clear()
        }
        taken
    }
}

impl<'a, T> IntoIterator for &'a MultiStash<T> {
    type Item = (Key, usize, &'a T);
    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T> IntoIterator for &'a mut MultiStash<T> {
    type Item = (Key, usize, &'a mut T);
    type IntoIter = IterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

impl<T> IntoIterator for MultiStash<T> {
    type Item = (Key, usize, T);
    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter::new(self)
    }
}

impl<T> Index<Key> for MultiStash<T> {
    type Output = T;

    fn index(&self, key: Key) -> &Self::Output {
        self.get(key)
            .map(|(_, item)| item)
            .unwrap_or_else(|| panic!("found no item at index {}", key.0))
    }
}

impl<T> IndexMut<Key> for MultiStash<T> {
    fn index_mut(&mut self, key: Key) -> &mut Self::Output {
        self.get_mut(key)
            .map(|(_, item)| item)
            .unwrap_or_else(|| panic!("found no item at index {}", key.0))
    }
}

impl<T> Extend<(NonZeroUsize, T)> for MultiStash<T> {
    fn extend<I: IntoIterator<Item = (NonZeroUsize, T)>>(&mut self, iter: I) {
        for (amount, item) in iter {
            self.put(amount, item);
        }
    }
}

impl<T> FromIterator<(NonZeroUsize, T)> for MultiStash<T> {
    fn from_iter<I: IntoIterator<Item = (NonZeroUsize, T)>>(iter: I) -> Self {
        let mut stash = Self::new();
        stash.extend(iter);
        stash
    }
}
