use alloc::vec::Vec;
use core::cmp::Ordering;
use crate::EMPTY_REF;
use crate::map::entity::Entity;
use crate::map::sort::MapCollection;

pub struct MapList<K, V> {
    pub(super) buffer: Vec<Entity<K, V>>,
}

impl<K: Copy, V: Clone> MapList<K, V> {
    #[inline(always)]
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity)
        }
    }
}

impl<K: Copy + Ord, V: Clone> MapCollection<K, V> for MapList<K, V> {
    #[inline]
    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    #[inline]
    fn insert(&mut self, key: K, val: V) {
        let index = self
            .buffer
            .binary_search_by_key(&key, |e| e.key)
            .unwrap_or_else(|index| index);
        self.buffer.insert(index, Entity::new(key, val));
    }

    #[inline]
    fn delete(&mut self, key: K) {
        if let Ok(index) = self.buffer.binary_search_by_key(&key, |e| e.key) {
            self.buffer.remove(index);
        }
    }

    #[inline]
    fn delete_by_index(&mut self, index: u32) {
        self.buffer.remove(index as usize);
    }

    #[inline]
    fn get_value(&self, key: K) -> Option<&V> {
        if let Ok(index) = self.buffer.binary_search_by_key(&key, |e| e.key) {
            Some(&unsafe { self.buffer.get_unchecked(index) }.val)
        } else {
            None
        }
    }

    #[inline]
    fn value_by_index(&self, index: u32) -> &V {
        &unsafe { self.buffer.get_unchecked(index as usize) }.val
    }

    #[inline]
    fn value_by_index_mut(&mut self, index: u32) -> &mut V {
        &mut unsafe { self.buffer.get_unchecked_mut(index as usize) }.val
    }

    #[inline]
    fn first_index_less(&self, key: K) -> u32 {
        match self.buffer.binary_search_by(|e| e.key.cmp(&key)) {
            Ok(index) => index as u32,
            Err(index) => {
                if index > 0 {
                    (index - 1) as u32
                } else {
                    EMPTY_REF
                }
            }
        }
    }

    #[inline]
    fn first_index_less_by<F>(&self, f: F) -> u32
    where
        F: Fn(K) -> Ordering,
    {
        match self.buffer.binary_search_by(|e| f(e.key)) {
            Ok(index) => index as u32,
            Err(index) => {
                if index > 0 {
                    (index - 1) as u32
                } else {
                    EMPTY_REF
                }
            }
        }
    }

    #[inline]
    fn clear(&mut self) {
        self.buffer.clear();
    }
}