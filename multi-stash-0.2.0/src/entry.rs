use core::num::NonZeroUsize;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Entry<T> {
    Vacant(VacantEntry),
    Occupied(OccupiedEntry<T>),
}

impl<T> From<VacantEntry> for Entry<T> {
    fn from(entry: VacantEntry) -> Self {
        Self::Vacant(entry)
    }
}

impl<T> From<OccupiedEntry<T>> for Entry<T> {
    fn from(entry: OccupiedEntry<T>) -> Self {
        Self::Occupied(entry)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VacantEntry {
    pub next_free: usize,
}

impl VacantEntry {
    pub fn new(next_free: usize) -> Self {
        Self { next_free }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OccupiedEntry<T> {
    pub remaining: NonZeroUsize,
    pub item: T,
}

impl<T> OccupiedEntry<T> {
    pub fn new(item: T, amount: NonZeroUsize) -> Self {
        Self {
            remaining: amount,
            item,
        }
    }
}
