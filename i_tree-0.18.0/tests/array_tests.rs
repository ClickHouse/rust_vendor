#[cfg(test)]
mod tests {
    use i_tree::ExpiredKey;
    use i_tree::key::array::IntoArray;
    use i_tree::key::exp::KeyExpCollection;
    use i_tree::key::list::KeyExpList;
    use i_tree::key::tree::KeyExpTree;
    use std::cmp::Ordering;

    #[derive(Debug, Clone, Copy)]
    struct Key {
        key: i32,
        exp: i32,
    }

    impl Key {
        fn new(key: i32, exp: i32) -> Self {
            Self { key, exp }
        }
    }

    impl Ord for Key {
        fn cmp(&self, other: &Self) -> Ordering {
            self.key.cmp(&other.key)
        }
    }

    impl Eq for Key {}

    impl PartialEq<Self> for Key {
        fn eq(&self, other: &Self) -> bool {
            self.key.eq(&other.key)
        }
    }

    impl PartialOrd<Self> for Key {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            self.key.partial_cmp(&other.key)
        }
    }

    impl ExpiredKey<i32> for Key {
        fn expiration(&self) -> i32 {
            self.exp
        }
    }

    #[test]
    fn test_00() {
        let vals = vec![0, 3, 5, 6];
        let keys = vals.iter().map(|&a| Key::new(a, 10));

        let mut tree = KeyExpTree::new(8);

        for key in keys {
            tree.insert(key, key.key, 0);
        }

        assert_eq!(tree.into_ordered_vec(0), vals);
    }

    #[test]
    fn test_01() {
        let vals = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let keys = vals.iter().map(|&a| Key::new(a, 10));

        let mut tree = KeyExpTree::new(8);

        for key in keys {
            tree.insert(key, key.key, 0);
        }

        assert_eq!(tree.into_ordered_vec(0), vals);
    }

    #[test]
    fn test_dynamic_00() {
        let mut array = Vec::with_capacity(100);
        for n in 1..1000i32 {
            let mut tree = KeyExpTree::new(8);
            let mut list = KeyExpList::new(n as usize);
            for i in 1..n {
                array.push(i);
                tree.insert(Key::new(i, 10), i, 0);
                list.insert(Key::new(i, 10), i, 0);
            }

            assert_eq!(tree.into_ordered_vec(0), array);
            assert_eq!(list.into_ordered_vec(0), array);
            array.clear();
        }
    }
}
