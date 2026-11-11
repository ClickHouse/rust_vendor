use crate::{Expiration, ExpiredKey};
use crate::key::list::KeyExpList;

pub struct OrderedIterator<'a, K, E, V> {
    list: &'a KeyExpList<K, E, V>,
    index: usize,
}

impl<'a, K, E, V> OrderedIterator<'a, K, E, V> {
    #[inline]
    pub(crate) fn new(list: &'a KeyExpList<K, E, V>) -> Self {
        Self {
            index: 0,
            list,
        }
    }
}
impl<'a, K, E, V> Iterator for OrderedIterator<'a, K, E, V> {
    type Item = &'a V;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.list.buffer.len() {
            return None;
        }
        let item = &self.list.buffer[self.index].val;
        self.index += 1;
        Some(item)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.list.buffer.len(), Some(self.list.buffer.len()))
    }
}

impl<K: ExpiredKey<E>, E: Expiration, V: Copy> KeyExpList<K, E, V> {

    #[inline]
    pub fn ordered_values(&self) -> OrderedIterator<K, E, V> {
        OrderedIterator::new(self)
    }

}