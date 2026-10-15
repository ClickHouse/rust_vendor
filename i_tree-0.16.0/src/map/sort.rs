use core::cmp::Ordering;

pub trait MapCollection<K, V> {
    fn is_empty(&self) -> bool;
    fn insert(&mut self, key: K, val: V);
    fn delete(&mut self, key: K);
    fn delete_by_index(&mut self, index: u32);
    fn get_value(&self, key: K) -> Option<&V>;
    fn value_by_index(&self, index: u32) -> &V;
    fn value_by_index_mut(&mut self, index: u32) -> &mut V;
    fn first_index_less(&self, key: K) -> u32;
    fn first_index_less_by<F>(&self, f: F) -> u32
    where
        F: Fn(K) -> Ordering;

    fn clear(&mut self);
}
