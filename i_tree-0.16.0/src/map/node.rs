use crate::map::entity::Entity;

#[derive(PartialEq, Clone, Copy)]
pub(super) enum Color {
    Red,
    Black,
}

#[derive(Clone)]
pub(super) struct Node<K, V> {
    pub(super) parent: u32,
    pub(super) left: u32,
    pub(super) right: u32,
    pub(super) color: Color,
    pub(super) entity: Entity<K, V>,
}

impl<K: Copy + Default, V: Clone + Default> Default for Node<K, V> {
    #[inline]
    fn default() -> Self {
        Self {
            parent: 0,
            left: 0,
            right: 0,
            color: Color::Red,
            entity: Entity::new(K::default(), V::default()),
        }
    }
}
