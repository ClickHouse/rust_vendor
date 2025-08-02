use super::*;

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

#[test]
fn new_works() {
    let mut stash = <MultiStash<char>>::new();
    assert!(stash.is_empty());
    assert_eq!(stash.len(), 0);
    assert_eq!(stash.len_items(), 0);
    assert_eq!(stash.get(Key(0)), None);
    assert_eq!(stash.get(Key(9999)), None);
    assert_eq!(stash.get_mut(Key(0)), None);
    assert_eq!(stash.get_mut(Key(9999)), None);
}

#[test]
fn put_works() {
    let mut stash = <MultiStash<char>>::new();
    assert_eq!(stash.put(nz(3), 'A'), Key(0));
    assert!(!stash.is_empty());
    assert_eq!(stash.len(), 1);
    assert_eq!(stash.len_items(), 3);
    assert_eq!(stash.put(nz(2), 'B'), Key(1));
    assert_eq!(stash.len(), 2);
    assert_eq!(stash.len_items(), 5);
}

#[test]
fn put_after_take_works() {
    let mut stash = <MultiStash<char>>::new();
    assert_eq!(stash.put(nz(3), 'A'), Key(0));
    assert_eq!(stash.put(nz(2), 'B'), Key(1));
    assert_eq!(stash.put(nz(4), 'C'), Key(2));
    assert_eq!(stash.len(), 3);
    assert_eq!(stash.len_items(), 9);
    assert_eq!(stash.take_one(Key(1)), Some((1, 'B')));
    assert_eq!(stash.len(), 3);
    assert_eq!(stash.len_items(), 8);
    assert_eq!(stash.take_one(Key(1)), Some((0, 'B')));
    assert_eq!(stash.len(), 2);
    assert_eq!(stash.len_items(), 7);
    assert_eq!(stash.put(nz(3), 'D'), Key(1));
    assert_eq!(stash.len(), 3);
    assert_eq!(stash.len_items(), 10);
    assert_eq!(stash.put(nz(1), 'E'), Key(3));
    assert_eq!(stash.len(), 4);
    assert_eq!(stash.len_items(), 11);
}

#[test]
fn take_reverse() {
    let mut stash = <MultiStash<char>>::new();
    stash.extend([
        (nz(2), 'A'),
        (nz(3), 'B'),
        (nz(1), 'C'),
        (nz(5), 'D'),
        (nz(1), 'E'),
    ]);

    assert_eq!(stash.take_one(Key(4)), Some((0, 'E')));
    assert_eq!(stash.take_one(Key(3)), Some((4, 'D')));
    assert_eq!(stash.take_one(Key(3)), Some((3, 'D')));
    assert_eq!(stash.take_one(Key(3)), Some((2, 'D')));
    assert_eq!(stash.take_one(Key(3)), Some((1, 'D')));
    assert_eq!(stash.take_one(Key(3)), Some((0, 'D')));
    assert_eq!(stash.take_one(Key(2)), Some((0, 'C')));
    assert_eq!(stash.take_one(Key(1)), Some((2, 'B')));
    assert_eq!(stash.take_one(Key(1)), Some((1, 'B')));
    assert_eq!(stash.take_one(Key(1)), Some((0, 'B')));
    assert_eq!(stash.take_one(Key(0)), Some((1, 'A')));
    assert_eq!(stash.take_one(Key(0)), Some((0, 'A')));

    assert!(stash.is_empty());
}

#[test]
fn take_ascending() {
    let mut stash = <MultiStash<char>>::new();
    stash.extend([
        (nz(2), 'A'),
        (nz(3), 'B'),
        (nz(1), 'C'),
        (nz(5), 'D'),
        (nz(1), 'E'),
    ]);

    assert_eq!(stash.take_one(Key(0)), Some((1, 'A')));
    assert_eq!(stash.take_one(Key(0)), Some((0, 'A')));
    assert_eq!(stash.take_one(Key(1)), Some((2, 'B')));
    assert_eq!(stash.take_one(Key(1)), Some((1, 'B')));
    assert_eq!(stash.take_one(Key(1)), Some((0, 'B')));
    assert_eq!(stash.take_one(Key(2)), Some((0, 'C')));
    assert_eq!(stash.take_one(Key(3)), Some((4, 'D')));
    assert_eq!(stash.take_one(Key(3)), Some((3, 'D')));
    assert_eq!(stash.take_one(Key(3)), Some((2, 'D')));
    assert_eq!(stash.take_one(Key(3)), Some((1, 'D')));
    assert_eq!(stash.take_one(Key(3)), Some((0, 'D')));
    assert_eq!(stash.take_one(Key(4)), Some((0, 'E')));

    assert!(stash.is_empty());

    assert_eq!(stash.put(nz(4), 'F'), Key(0));
}

#[test]
fn take_all_reverse() {
    let mut stash = <MultiStash<char>>::new();
    stash.extend([
        (nz(2), 'A'),
        (nz(3), 'B'),
        (nz(1), 'C'),
        (nz(5), 'D'),
        (nz(1), 'E'),
    ]);
    assert_eq!(stash.take_all(Key(4)), Some((1, 'E')));
    assert_eq!(stash.take_all(Key(3)), Some((5, 'D')));
    assert_eq!(stash.take_all(Key(2)), Some((1, 'C')));
    assert_eq!(stash.take_all(Key(1)), Some((3, 'B')));
    assert_eq!(stash.take_all(Key(0)), Some((2, 'A')));

    // Since we clear stash if it is empty after take we
    // can observe key(0) for our next insert instead of
    // key(4) which we would get without the reset.
    assert_eq!(stash.put(nz(4), 'F'), Key(0));
}

#[test]
fn take_all_but_one_then_refill() {
    let mut stash = <MultiStash<char>>::new();
    stash.extend([
        (nz(2), 'A'),
        (nz(3), 'B'),
        (nz(1), 'C'),
        (nz(5), 'D'),
        (nz(1), 'E'),
    ]);
    assert_eq!(stash.take_all(Key(0)), Some((2, 'A')));
    assert_eq!(stash.take_all(Key(1)), Some((3, 'B')));
    // key(2) not taken!
    assert_eq!(stash.take_all(Key(3)), Some((5, 'D')));
    assert_eq!(stash.take_all(Key(4)), Some((1, 'E')));

    assert_eq!(stash.put(nz(4), 'F'), Key(4));
    assert_eq!(stash.put(nz(5), 'G'), Key(3));
    assert_eq!(stash.put(nz(6), 'H'), Key(1));
    assert_eq!(stash.put(nz(7), 'H'), Key(0));
    // Now we fill stash from the back again:
    assert_eq!(stash.put(nz(8), 'I'), Key(5));
}

#[test]
#[should_panic]
fn put_fails_0() {
    let mut stash = <MultiStash<char>>::new();
    assert_eq!(stash.put(nz(usize::MAX), 'A'), Key(0));
    stash.put(nz(1), 'B');
}

#[test]
#[should_panic]
fn put_fails_1() {
    let mut stash = <MultiStash<char>>::new();
    assert_eq!(stash.put(nz(1), 'A'), Key(0));
    stash.put(nz(usize::MAX), 'B');
}

#[test]
fn bump_works() {
    let mut stash = <MultiStash<char>>::new();
    assert_eq!(stash.put(nz(1), 'A'), Key(0));
    assert_eq!(stash.put(nz(2), 'B'), Key(1));
    assert_eq!(stash.put(nz(3), 'C'), Key(2));
    assert_eq!(stash.bump(Key(2), 0), Some(3));
    assert_eq!(stash.bump(Key(2), 1), Some(3));
    assert_eq!(stash.bump(Key(2), 2), Some(4));
    assert_eq!(stash.bump(Key(0), 10), Some(1));
    assert_eq!(stash.bump(Key(1), 100), Some(2));
    assert_eq!(stash.get(Key(0)), Some((11, &'A')));
    assert_eq!(stash.get(Key(1)), Some((102, &'B')));
    assert_eq!(stash.get(Key(2)), Some((6, &'C')));
}

#[test]
#[should_panic]
fn bump_fails_0() {
    let mut stash = <MultiStash<char>>::new();
    assert_eq!(stash.put(nz(usize::MAX), 'A'), Key(0));
    stash.bump(Key(0), 1);
}

#[test]
#[should_panic]
fn bump_fails_1() {
    let mut stash = <MultiStash<char>>::new();
    assert_eq!(stash.put(nz(1), 'A'), Key(0));
    stash.bump(Key(0), usize::MAX);
}

#[test]
#[should_panic]
fn bump_fails_2() {
    let mut stash = <MultiStash<char>>::new();
    assert_eq!(stash.put(nz(1000), 'A'), Key(0));
    assert_eq!(stash.put(nz(1000), 'B'), Key(0));
    stash.bump(Key(0), usize::MAX / 2);
    stash.bump(Key(1), usize::MAX / 2);
}
