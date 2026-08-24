use core::cmp::Ordering;

pub trait KeyValue<K> {
    fn key(&self) -> &K;
}

pub trait SetCollection<K, V> {
    fn is_empty(&self) -> bool;
    fn insert(&mut self, val: V);
    fn delete(&mut self, key: &K);
    fn delete_by_index(&mut self, index: u32);
    fn get_value(&self, key: &K) -> Option<&V>;
    fn index_after(&self, index: u32) -> u32;
    fn index_before(&self, index: u32) -> u32;
    fn value_by_index(&self, index: u32) -> &V;
    fn value_by_index_mut(&mut self, index: u32) -> &mut V;
    fn first_index_less(&self, key: &K) -> u32;
    fn first_index_less_by<F>(&self, f: F) -> u32
    where
        F: Fn(&K) -> Ordering;

    fn clear(&mut self);
}

impl KeyValue<i8> for i8 {
    fn key(&self) -> &i8 {
        self
    }
}

impl KeyValue<i16> for i16 {
    fn key(&self) -> &i16 {
        self
    }
}

impl KeyValue<i32> for i32 {
    fn key(&self) -> &i32 {
        self
    }
}

impl KeyValue<i64> for i64 {
    fn key(&self) -> &i64 {
        self
    }
}

impl KeyValue<u8> for u8 {
    fn key(&self) -> &u8 {
        self
    }
}

impl KeyValue<u16> for u16 {
    fn key(&self) -> &u16 {
        self
    }
}

impl KeyValue<u32> for u32 {
    fn key(&self) -> &u32 {
        self
    }
}

impl KeyValue<u64> for u64 {
    fn key(&self) -> &u64 {
        self
    }
}

impl KeyValue<usize> for usize {
    fn key(&self) -> &usize {
        self
    }
}