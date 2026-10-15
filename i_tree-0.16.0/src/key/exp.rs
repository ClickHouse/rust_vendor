use core::cmp::Ordering;

pub trait KeyExpCollection<K, E, V> {
    fn is_empty(&self) -> bool;
    fn insert(&mut self, key: K, val: V, time: E);
    fn get_value(&mut self, time: E, key: K) -> Option<V>;
    fn first_less(&mut self, time: E, default: V, key: K) -> V;
    fn first_less_by<F>(&mut self, time: E, default: V, f: F) -> V
    where
        F: Fn(K) -> Ordering;
    fn first_less_or_equal(&mut self, time: E, default: V, key: K) -> V;
    fn first_less_or_equal_by<F>(&mut self, time: E, default: V, f: F) -> V
    where
        F: Fn(K) -> Ordering;
    fn clear(&mut self);
}
