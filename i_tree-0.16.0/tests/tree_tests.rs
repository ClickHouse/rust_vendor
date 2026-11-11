#[cfg(test)]
mod tests {
    use rand::prelude::SliceRandom;
    use rand::{Rng, rng};
    use std::cmp::Ordering;
    use i_tree::ExpiredKey;
    use i_tree::key::array::IntoArray;
    use i_tree::key::exp::KeyExpCollection;
    use i_tree::key::list::KeyExpList;
    use i_tree::key::tree::KeyExpTree;

    struct Task {
        time: i32,
        val: i32,
        exp: i32,
    }

    impl Task {
        fn new(time: i32, val: i32, exp: i32) -> Self {
            Self { time, val, exp }
        }
    }

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
        let mut list = KeyExpList::new(1);
        let mut tree = KeyExpTree::new(8);
        let k0 = Key::new(10, 10);
        list.insert(k0, 1, 0);
        tree.insert(k0, 1, 0);
        let t0 = tree.get_value(0, k0).unwrap();
        let l0 = list.get_value(0, k0).unwrap();

        assert_eq!(t0, 1);
        assert_eq!(l0, 1);
    }

    #[test]
    fn test_01() {
        let mut list = KeyExpList::new(3);
        let mut tree = KeyExpTree::new(8);

        let k0 = Key::new(10, 10);
        tree.insert(k0, 1, 0);
        list.insert(k0, 1, 0);

        let t0 = tree.get_value(0, Key::new(0, 10));
        let l0 = list.get_value(0, Key::new(0, 10));

        let t1 = tree.get_value(0, Key::new(10, 10));
        let l1 = list.get_value(0, Key::new(10, 10));

        let t2 = tree.get_value(0, Key::new(20, 10));
        let l2 = list.get_value(0, Key::new(20, 10));

        assert!(t0.is_none());
        assert!(l0.is_none());
        assert_eq!(t1.unwrap(), 1);
        assert_eq!(l1.unwrap(), 1);
        assert!(t2.is_none());
        assert!(l2.is_none());
    }

    #[test]
    fn test_02() {
        let mut list = KeyExpList::new(3);
        let mut tree = KeyExpTree::new(8);

        let k0 = Key::new(10, 10);

        list.insert(k0, 1, 0);
        tree.insert(k0, 1, 0);

        let t0 = tree.get_value(11, Key::new(0, 10));
        let l0 = list.get_value(11, Key::new(0, 10));
        let t1 = tree.get_value(0, Key::new(10, 10));
        let l1 = list.get_value(0, Key::new(10, 10));
        let t2 = tree.get_value(0, Key::new(20, 10));
        let l2 = list.get_value(0, Key::new(20, 10));

        assert!(t0.is_none());
        assert!(l0.is_none());
        assert!(t1.is_none());
        assert!(l1.is_none());
        assert!(t2.is_none());
        assert!(l2.is_none());
    }

    #[test]
    fn test_03() {
        let mut list = KeyExpList::new(3);
        let mut tree = KeyExpTree::new(8);

        let k0 = Key::new(10, 10);

        list.insert(k0, 1, 0);
        tree.insert(k0, 1, 0);

        let t0 = tree.first_less_or_equal_by(0, -1, |key| key.key.cmp(&1));
        let l0 = list.first_less_or_equal_by(0, -1, |key| key.key.cmp(&1));
        let t1 = tree.first_less_or_equal_by(0, -1, |key| key.key.cmp(&2));
        let l1 = list.first_less_or_equal_by(0, -1, |key| key.key.cmp(&2));
        let t2 = tree.first_less_or_equal_by(0, -1, |key| key.key.cmp(&3));
        let l2 = list.first_less_or_equal_by(0, -1, |key| key.key.cmp(&3));

        assert_eq!(t0, -1);
        assert_eq!(l0, -1);
        assert_eq!(t1, -1);
        assert_eq!(l1, -1);
        assert_eq!(t2, -1);
        assert_eq!(l2, -1);
    }

    #[test]
    fn test_04() {
        let mut vals = vec![3, 1, 2];
        let keys = vals.iter().map(|&a| Key::new(a, 10));

        let mut tree = KeyExpTree::new(8);
        let mut list = KeyExpList::new(keys.len());

        for key in keys {
            tree.insert(key, key.key, 0);
            list.insert(key, key.key, 0);
        }

        vals.sort_unstable();

        assert_eq!(tree.into_ordered_vec(0), vals);
        assert_eq!(list.into_ordered_vec(0), vals);
    }

    #[test]
    fn test_05() {
        let vals = vec![0, 3, 5, 6];
        let keys = vals.iter().map(|&a| Key::new(a, 10));

        let mut tree = KeyExpTree::new(8);
        let mut list = KeyExpList::new(keys.len());

        for key in keys {
            tree.insert(key, key.key, 0);
            list.insert(key, key.key, 0);
        }

        for i in 0..10 {
            let l = list.first_less_or_equal_by(1, -1, |k| k.key.cmp(&i));
            let t = tree.first_less_or_equal_by(1, -1, |k| k.key.cmp(&i));
            assert_eq!(l, t);
        }
    }

    #[test]
    fn test_06() {
        let vals = vec![0, 1, 2, 3];
        let keys = vals.iter().map(|&a| Key::new(a, 10));

        let mut tree = KeyExpTree::new(8);
        let mut list = KeyExpList::new(keys.len());

        for key in keys {
            tree.insert(key, key.key, 0);
            list.insert(key, key.key, 0);
        }

        assert_eq!(tree.into_ordered_vec(0), vals);
        assert_eq!(list.into_ordered_vec(0), vals);
    }

    #[test]
    fn test_077() {
        let vals = vec![0, 1, 2, 3, 4, 5];
        let keys = vals.iter().map(|&a| Key::new(a, 10));

        let mut tree = KeyExpTree::new(8);
        let mut list = KeyExpList::new(keys.len());

        for key in keys {
            tree.insert(key, key.key, 0);
            list.insert(key, key.key, 0);
        }

        assert_eq!(tree.into_ordered_vec(0), vals);
        assert_eq!(list.into_ordered_vec(0), vals);
    }

    #[test]
    fn test_07() {
        let vals = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let keys = vals.iter().map(|&a| Key::new(a, 10));

        let mut tree = KeyExpTree::new(8);
        let mut list = KeyExpList::new(keys.len());

        for key in keys {
            tree.insert(key, key.key, 0);
            list.insert(key, key.key, 0);
        }

        assert_eq!(tree.into_ordered_vec(0), vals);
        assert_eq!(list.into_ordered_vec(0), vals);
    }

    #[test]
    fn test_08() {
        let vals = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let keys = vals.iter().rev().map(|&a| Key::new(a, 10));

        let mut tree = KeyExpTree::new(8);
        let mut list = KeyExpList::new(keys.len());

        for key in keys {
            tree.insert(key, key.key, 0);
            list.insert(key, key.key, 0);
        }

        assert_eq!(tree.into_ordered_vec(0), vals);
        assert_eq!(list.into_ordered_vec(0), vals);
    }

    #[test]
    fn test_09() {
        let mut vals = vec![2, 8, 9, 7, 5, 1, 4, 6, 3];
        let keys = vals.iter().rev().map(|&a| Key::new(a, 10));

        let mut tree = KeyExpTree::new(8);
        let mut list = KeyExpList::new(keys.len());

        for key in keys {
            tree.insert(key, key.key, 0);
            list.insert(key, key.key, 0);
        }

        vals.sort_unstable();

        assert_eq!(tree.into_ordered_vec(0), vals);
        assert_eq!(list.into_ordered_vec(0), vals);
    }

    #[test]
    fn test_10() {
        let vals0 = vec![2, 18, 16, 4, 14, 8, 6, 10, 12];
        let mut vals1 = vec![7, 9, 15, 11, 13, 1, 17, 19, 3, 5];

        let keys0 = vals0.iter().rev().map(|&a| Key::new(a, 10));
        let keys1 = vals1.iter().rev().map(|&a| Key::new(a, 20));

        let mut tree = KeyExpTree::new(8);
        let mut list = KeyExpList::new(keys0.len() + keys1.len());

        for key in keys0 {
            tree.insert(key, key.key, 0);
            list.insert(key, key.key, 0);
        }
        for key in keys1 {
            tree.insert(key, key.key, 0);
            list.insert(key, key.key, 0);
        }

        vals1.sort_unstable();

        assert_eq!(tree.into_ordered_vec(11), vals1);
        assert_eq!(list.into_ordered_vec(11), vals1);
    }

    #[test]
    fn test_11() {
        let vals0 = vec![12, 16, 4, 6, 8, 18, 2, 10, 14];
        let mut vals1 = vec![3, 15, 13, 5, 17, 1, 7, 19, 11, 9];

        let keys0 = vals0.iter().rev().map(|&a| Key::new(a, 10));
        let keys1 = vals1.iter().rev().map(|&a| Key::new(a, 20));

        let mut tree = KeyExpTree::new(8);
        let mut list = KeyExpList::new(keys0.len() + keys1.len());

        for key in keys0 {
            tree.insert(key, key.key, 0);
            list.insert(key, key.key, 0);
        }
        for key in keys1 {
            tree.insert(key, key.key, 0);
            list.insert(key, key.key, 0);
        }

        vals1.sort_unstable();

        assert_eq!(tree.into_ordered_vec(11), vals1);
        assert_eq!(list.into_ordered_vec(11), vals1);
    }

    #[test]
    fn test_12() {
        let vals0 = vec![8, 4, 2, 16, 18, 12, 10, 6, 14];
        let mut vals1 = vec![5, 3, 11, 13, 19, 7, 1, 15, 17, 9];

        let keys0 = vals0.iter().rev().map(|&a| Key::new(a, 10));
        let keys1 = vals1.iter().rev().map(|&a| Key::new(a, 20));

        let mut tree = KeyExpTree::new(8);
        let mut list = KeyExpList::new(keys0.len() + keys1.len());

        for key in keys0 {
            tree.insert(key, key.key, 0);
            list.insert(key, key.key, 0);
        }
        for key in keys1 {
            tree.insert(key, key.key, 0);
            list.insert(key, key.key, 0);
        }

        vals1.sort_unstable();

        assert_eq!(tree.into_ordered_vec(11), vals1);
        assert_eq!(list.into_ordered_vec(11), vals1);
    }

    #[test]
    fn test_13() {
        let vals0 = vec![16, 14, 18, 12, 6, 4, 10, 8, 2];
        let mut vals1 = vec![15, 13, 9, 7, 1, 17, 5, 11, 3];

        let keys0 = vals0.iter().rev().map(|&a| Key::new(a, 10));
        let keys1 = vals1.iter().rev().map(|&a| Key::new(a, 20));

        let mut tree = KeyExpTree::new(8);
        let mut list = KeyExpList::new(keys0.len() + keys1.len());

        for key in keys0 {
            tree.insert(key, key.key, 0);
            list.insert(key, key.key, 0);
        }
        for key in keys1 {
            tree.insert(key, key.key, 0);
            list.insert(key, key.key, 0);
        }

        vals1.sort_unstable();

        assert_eq!(tree.into_ordered_vec(11), vals1);
        assert_eq!(list.into_ordered_vec(11), vals1);
    }

    #[test]
    fn test_14() {
        let vals0 = vec![
            32, 34, 2, 4, 16, 10, 28, 24, 12, 30, 18, 6, 36, 22, 14, 20, 8, 26,
        ];
        let mut vals1 = vec![
            27, 1, 17, 13, 23, 29, 21, 37, 15, 7, 9, 35, 5, 3, 25, 31, 19, 11, 33,
        ];

        let keys0 = vals0.iter().rev().map(|&a| Key::new(a, 10));
        let keys1 = vals1.iter().rev().map(|&a| Key::new(a, 20));

        let mut tree = KeyExpTree::new(8);
        let mut list = KeyExpList::new(keys0.len() + keys1.len());

        for key in keys0 {
            tree.insert(key, key.key, 0);
            list.insert(key, key.key, 0);
        }
        for key in keys1 {
            tree.insert(key, key.key, 0);
            list.insert(key, key.key, 0);
        }

        vals1.sort_unstable();

        assert_eq!(tree.into_ordered_vec(11), vals1);
        assert_eq!(list.into_ordered_vec(11), vals1);
    }

    #[test]
    fn test_15() {
        let mut tree = KeyExpTree::new(8);
        let mut list = KeyExpList::new(20);

        let keys = vec![
            Key::new(8, 4),
            Key::new(2, 4),
            Key::new(5, 3),
            Key::new(0, 5),
        ];

        for &key in keys.iter() {
            tree.insert(key, key.key, 0);
            list.insert(key, key.key, 0);
        }

        let l0 = list.first_less_or_equal_by(3, -1, |k| k.key.cmp(&2));
        let t0 = tree.first_less_or_equal_by(3, -1, |k| k.key.cmp(&2));
        assert_eq!(l0, t0);

        tree.insert(Key::new(1, 5), 1, 3);
        list.insert(Key::new(1, 5), 1, 3);

        let l1 = list.first_less_or_equal_by(4, -1, |k| k.key.cmp(&2));
        let t1 = tree.first_less_or_equal_by(4, -1, |k| k.key.cmp(&2));
        assert_eq!(l1, t1);
    }

    #[test]
    fn test_16() {
        let mut tree = KeyExpTree::new(8);
        let mut list = KeyExpList::new(20);

        let keys = vec![Key::new(0, 2), Key::new(1, 1)];

        for &key in keys.iter() {
            tree.insert(key, key.key, 0);
            list.insert(key, key.key, 0);
        }

        let l0 = list.first_less_or_equal_by(1, -1, |k| k.key.cmp(&4));
        let t0 = tree.first_less_or_equal_by(1, -1, |k| k.key.cmp(&4));
        assert_eq!(l0, t0);

        tree.insert(Key::new(4, 5), 4, 1);
        list.insert(Key::new(4, 5), 4, 1);

        let l1 = list.first_less_or_equal_by(1, -1, |k| k.key.cmp(&1));
        let t1 = tree.first_less_or_equal_by(1, -1, |k| k.key.cmp(&1));
        assert_eq!(l1, t1);

        let l2 = list.first_less_or_equal_by(2, -1, |k| k.key.cmp(&0));
        let t2 = tree.first_less_or_equal_by(2, -1, |k| k.key.cmp(&0));
        assert_eq!(l2, t2);
    }

    #[test]
    fn test_17() {
        let mut tree = KeyExpTree::new(8);
        let mut list = KeyExpList::new(20);

        let key = Key::new(6, 1);
        tree.insert(key, key.key, 0);
        list.insert(key, key.key, 0);

        let l = list.first_less_or_equal_by(0, -1, |k| k.key.cmp(&5));
        let t = tree.first_less_or_equal_by(0, -1, |k| k.key.cmp(&5));
        assert_eq!(l, t);

        let key = Key::new(5, 1);
        tree.insert(key, key.key, 0);
        list.insert(key, key.key, 0);

        let l = list.first_less_or_equal_by(0, -1, |k| k.key.cmp(&4));
        let t = tree.first_less_or_equal_by(0, -1, |k| k.key.cmp(&4));
        assert_eq!(l, t);

        let key = Key::new(4, 1);
        tree.insert(key, key.key, 0);
        list.insert(key, key.key, 0);

        let l = list.first_less_or_equal_by(1, -1, |k| k.key.cmp(&8));
        let t = tree.first_less_or_equal_by(1, -1, |k| k.key.cmp(&8));
        assert_eq!(l, t);

        let key = Key::new(8, 2);
        tree.insert(key, key.key, 1);
        list.insert(key, key.key, 1);

        let l = list.first_less_or_equal_by(1, -1, |k| k.key.cmp(&0));
        let t = tree.first_less_or_equal_by(1, -1, |k| k.key.cmp(&0));
        assert_eq!(l, t);

        let key = Key::new(0, 3);
        tree.insert(key, key.key, 1);
        list.insert(key, key.key, 1);

        let l = list.first_less_or_equal_by(2, -1, |k| k.key.cmp(&7));
        let t = tree.first_less_or_equal_by(2, -1, |k| k.key.cmp(&7));
        assert_eq!(l, t);

        let key = Key::new(7, 4);
        tree.insert(key, key.key, 2);
        list.insert(key, key.key, 2);

        let l = list.first_less_or_equal_by(3, -1, |k| k.key.cmp(&2));
        let t = tree.first_less_or_equal_by(3, -1, |k| k.key.cmp(&2));
        assert_eq!(l, t);

        let key = Key::new(2, 5);
        tree.insert(key, key.key, 3);
        list.insert(key, key.key, 3);

        let l = list.first_less_or_equal_by(3, -1, |k| k.key.cmp(&6));
        let t = tree.first_less_or_equal_by(3, -1, |k| k.key.cmp(&6));
        assert_eq!(l, t);
    }

    #[test]
    fn test_18() {
        let tasks = vec![
            Task::new(0, 3, 0),
            Task::new(0, 3, 42),
            Task::new(1, 2, 0),
            Task::new(1, 2, 20),
            Task::new(2, 8, 0),
            Task::new(2, 8, 22),
            Task::new(2, 3, 0),
            Task::new(3, 22, 0),
            Task::new(3, 22, 15),
            Task::new(4, 29, 0),
            Task::new(4, 29, 19),
            Task::new(5, 14, 0),
            Task::new(5, 14, 20),
            Task::new(5, 14, 0),
            Task::new(6, 25, 0),
            Task::new(6, 25, 46),
            Task::new(6, 26, 0),
            Task::new(6, 26, 18),
            Task::new(7, 6, 0),
            Task::new(7, 6, 35),
            Task::new(8, 26, 0),
            Task::new(9, 4, 0),
            Task::new(9, 4, 46),
            Task::new(9, 13, 0),
            Task::new(9, 13, 32),
            Task::new(10, 2, 0),
            Task::new(10, 19, 0),
            Task::new(10, 19, 46),
            Task::new(11, 13, 0),
            Task::new(12, 10, 0),
            Task::new(12, 10, 32),
            Task::new(12, 22, 0),
            Task::new(13, 2, 0),
            Task::new(14, 17, 0),
            Task::new(14, 17, 63),
            Task::new(14, 25, 0),
            Task::new(15, 24, 0),
            Task::new(15, 24, 41),
            Task::new(16, 15, 0),
            Task::new(16, 15, 38),
            Task::new(17, 27, 0),
            Task::new(17, 27, 43),
            Task::new(17, 10, 0),
            Task::new(18, 22, 0),
            Task::new(18, 22, 35),
            Task::new(19, 3, 0),
        ];

        let mut tree: KeyExpTree<Key, i32, i32> = KeyExpTree::new(8);
        let mut list: KeyExpList<Key, i32, i32> = KeyExpList::new(200);

        for i in 0..tasks.len() - 1 {
            let task = &tasks[i];
            if task.exp == 0 {
                let list_result = list.first_less_or_equal_by(task.time, -1, |k| k.key.cmp(&task.val));
                let tree_result = tree.first_less_or_equal_by(task.time, -1, |k| k.key.cmp(&task.val));
                assert_eq!(list_result, tree_result);
            } else {
                tree.insert(Key::new(task.val, task.exp), task.val, task.time);
                list.insert(Key::new(task.val, task.exp), task.val, task.time);
            }
        }

        // println!("list: {:?}", &list.into_ordered_vec(73));

        let task = &tasks.last().unwrap();
        let list_result = list.first_less_or_equal_by(task.time, -1, |k| k.key.cmp(&task.val));
        let tree_result = tree.first_less_or_equal_by(task.time, -1, |k| k.key.cmp(&task.val));
        assert_eq!(list_result, tree_result);
    }

    #[test]
    fn test_19() {
        let tasks = vec![
            Task::new(0, 0, 0),
            Task::new(0, 0, 25),
            Task::new(0, 6, 0),
            Task::new(0, 6, 22),
            Task::new(1, 15, 0),
            Task::new(1, 15, 4),
            Task::new(2, 11, 0),
            Task::new(2, 11, 14),
            Task::new(3, 3, 0),
            Task::new(3, 3, 28),
        ];

        let mut tree: KeyExpTree<Key, i32, i32> = KeyExpTree::new(8);
        let mut list: KeyExpList<Key, i32, i32> = KeyExpList::new(200);

        for task in tasks {
            if task.exp == 0 {
                let list_result = list.first_less_or_equal_by(task.time, -1, |k| k.key.cmp(&task.val));
                let tree_result = tree.first_less_or_equal_by(task.time, -1, |k| k.key.cmp(&task.val));
                assert_eq!(list_result, tree_result);
            } else {
                tree.insert(Key::new(task.val, task.exp), task.val, task.time);
                list.insert(Key::new(task.val, task.exp), task.val, task.time);
            }
        }

        // println!("list: {:?}", &list.into_ordered_vec(73));

        let list_result = list.first_less_or_equal_by(4, -1, |k| k.key.cmp(&16));
        let tree_result = tree.first_less_or_equal_by(4, -1, |k| k.key.cmp(&16));
        assert_eq!(list_result, tree_result);
    }

    #[test]
    fn test_20() {
        let mut list = KeyExpList::new(3);
        let mut tree = KeyExpTree::new(8);

        let k0 = Key::new(0, 10);
        let k1 = Key::new(1, 10);
        let k2 = Key::new(2, 10);

        list.insert(k0, k0.key, 0);
        list.insert(k1, k1.key, 0);
        list.insert(k1, k1.key, 0);
        list.insert(k1, k1.key, 0);
        list.insert(k2, k2.key, 0);

        tree.insert(k0, k0.key, 0);
        tree.insert(k1, k1.key, 0);
        tree.insert(k1, k1.key, 0);
        tree.insert(k1, k1.key, 0);
        tree.insert(k2, k2.key, 0);


        assert_eq!(tree.into_ordered_vec(0), vec![0, 1, 1, 1, 2]);
        assert_eq!(list.into_ordered_vec(0), vec![0, 1, 1, 1, 2]);
    }

    #[test]
    fn test_random_00() {
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

    #[test]
    fn test_random_01() {
        let mut array = Vec::with_capacity(100);
        for n in 1..1000i32 {
            let mut tree = KeyExpTree::new(8);
            let mut list = KeyExpList::new(n as usize);
            for i in 1..n {
                array.push(i);
                let j = n - i;
                tree.insert(Key::new(j, 10), j, 0);
                list.insert(Key::new(i, 10), i, 0);
            }

            assert_eq!(tree.into_ordered_vec(0), array);
            assert_eq!(list.into_ordered_vec(0), array);
            array.clear();
        }
    }

    #[test]
    fn test_random_02() {
        let template: Vec<i32> = (1..300).collect();
        let mut array: Vec<i32> = template.clone();
        let mut rng = rng();
        for _ in 1..300 {
            array.shuffle(&mut rng);
            let mut tree = KeyExpTree::new(8);
            let mut list = KeyExpList::new(array.len());
            for &i in array.iter() {
                tree.insert(Key::new(i, 10), i, 0);
                list.insert(Key::new(i, 10), i, 0);
            }

            assert_eq!(tree.into_ordered_vec(0), template);
            assert_eq!(list.into_ordered_vec(0), template);
        }
    }

    #[test]
    fn test_random_03() {
        let r = 1..100;
        let mut evens: Vec<i32> = r.clone().filter(|x| x % 2 == 0).collect();
        let mut odds: Vec<i32> = r.filter(|x| x % 2 != 0).collect();
        let template = odds.clone();
        let mut rng = rng();
        for _ in 1..1000 {
            let mut tree = KeyExpTree::new(8);
            let mut list = KeyExpList::new(evens.len() + odds.len());
            evens.shuffle(&mut rng);
            odds.shuffle(&mut rng);
            for &o in odds.iter() {
                tree.insert(Key::new(o, 20), o, 0);
                list.insert(Key::new(o, 20), o, 0);
            }
            for &e in evens.iter() {
                tree.insert(Key::new(e, 10), e, 0);
                list.insert(Key::new(e, 10), e, 0);
            }

            assert_eq!(tree.into_ordered_vec(11), template);
            assert_eq!(list.into_ordered_vec(11), template);
        }
    }

    #[test]
    fn test_random_04() {
        let n = 100;

        let mut rng = rng();
        let mut task = Vec::new();

        let mut list: KeyExpList<Key, i32, i32> = KeyExpList::new(n);
        let mut tree: KeyExpTree<Key, i32, i32> = KeyExpTree::new(n);
        for _ in 0..1000 {
            let mut t = 0.0;
            let mut numbers = vec![-1i32; n];
            while t < 1000.0 {
                let time = t as i32;
                let index = rng.random_range(0..n);
                let old_time = numbers[index];
                let val = index as i32;

                let list_result = list.first_less_or_equal_by(time, -1, |k| k.key.cmp(&val));
                let tree_result = tree.first_less_or_equal_by(time, -1, |k| k.key.cmp(&val));

                task.push(Task { time, val, exp: 0 });

                assert_eq!(list_result, tree_result);
                if old_time < time {
                    let exp = (t + rng.random_range(1.0..50.0)) as i32;
                    tree.insert(Key::new(val, exp), val, time);
                    list.insert(Key::new(val, exp), val, time);
                    numbers[index] = exp;

                    task.push(Task { time, val, exp });
                }

                t += rng.random_range(0.5..5.0);
            }
            task.clear();
            tree.clear();
            list.clear();
        }
    }
}
