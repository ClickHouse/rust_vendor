#[derive(Clone)]
pub(super) struct Entity<K, V> {
    pub(super) key: K,
    pub(super) val: V,
}

impl<K: Copy, V: Clone> Entity<K, V> {
    #[inline]
    pub(super) fn new(key: K, val: V) -> Self {
        Self { key, val }
    }
}