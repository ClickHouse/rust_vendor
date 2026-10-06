use async_cell::sync::AsyncCell;
mod utils;
use loom::thread::yield_now;
use std::mem::size_of;
use utils::{root, spawn, tick_future};

#[test]
fn set_wakes() {
    let has_woken = AsyncCell::shared();

    let to_wake = AsyncCell::<u32>::new();
    tick_future(to_wake.get(), has_woken.clone());

    assert!(!has_woken.is_set());
    to_wake.set(42);
    assert!(has_woken.is_set());
}

#[test]
fn or_set_wakes() {
    let has_woken = AsyncCell::shared();

    let to_wake = AsyncCell::<u32>::new();
    tick_future(to_wake.get(), has_woken.clone());

    assert!(!has_woken.is_set());
    to_wake.or_set(42);
    assert!(has_woken.is_set());
}

#[test]
fn notify_wakes() {
    let has_woken = AsyncCell::shared();

    let to_wake = AsyncCell::new();
    tick_future(to_wake.get(), has_woken.clone());

    assert!(!has_woken.is_set());
    to_wake.notify();
    assert!(has_woken.is_set());
}

#[test]
fn update_wakes() {
    let has_woken = AsyncCell::shared();

    let to_wake = AsyncCell::new();
    tick_future(to_wake.get(), has_woken.clone());

    assert!(!has_woken.is_set());
    to_wake.update(|_| Some(42));
    assert!(has_woken.is_set());
}

#[test]
fn set_changes() {
    static A: AsyncCell<i32> = AsyncCell::new();

    assert_eq!(A.try_get(), None);
    A.set(1);
    assert_eq!(A.try_get(), Some(1));
    A.set(2);
    assert_eq!(A.try_get(), Some(2));
}

#[test]
fn update_changes() {
    static A: AsyncCell<i32> = AsyncCell::new();

    A.update(|prev| {
        assert_eq!(prev, None);
        Some(1)
    });
    assert_eq!(A.try_get(), Some(1));
    A.update(|prev| {
        assert_eq!(prev, Some(1));
        Some(2)
    });
    assert_eq!(A.try_get(), Some(2));
    A.update(|prev| {
        assert_eq!(prev, Some(2));
        None
    });
    assert_eq!(A.try_get(), None);
}

#[test]
fn or_set_changes() {
    static A: AsyncCell<i32> = AsyncCell::new();

    assert_eq!(A.try_take(), None);

    A.or_set(1);
    assert_eq!(A.try_take(), Some(1));

    A.or_set(2);
    assert_eq!(A.try_get(), Some(2));

    A.or_set(3);
    assert_eq!(A.try_get(), Some(2));

    A.set(4);
    assert_eq!(A.try_get(), Some(4));

    A.or_set(5);
    assert_eq!(A.try_get(), Some(4));
}

#[test]
fn drop_wakes() {
    let has_dropped = AsyncCell::shared();

    {
        let to_drop = AsyncCell::<u32>::shared();
        tick_future(to_drop.get(), has_dropped.clone());
    }

    assert!(has_dropped.is_set());
}

#[test]
fn can_wake_multiple_takes() {
    root(|| async {
        let x0 = AsyncCell::shared();
        let x1 = x0.clone();
        let x2 = x0.clone();
        let x3 = x0.clone();
        let a0 = AsyncCell::shared();
        let a1 = a0.clone();
        let b0 = AsyncCell::shared();
        let b1 = b0.clone();
        let c0 = AsyncCell::shared();
        let c1 = c0.clone();

        spawn(async move {
            let v = x1.take().await;
            x1.set(v + 1);
            a1.set(v);
        });
        spawn(async move {
            let v = x2.take().await;
            x2.set(v + 1);
            b1.set(v);
        });
        spawn(async move {
            let v = x3.take().await;
            x3.set(v + 1);
            c1.set(v);
        });

        yield_now();
        x0.set(1);
        let mut vs = [a0.get().await, b0.get().await, c0.get().await];
        vs.sort();
        assert_eq!(vs, [1, 2, 3]);
    });
}

#[test]
fn wakes_multiple_gets() {
    let x = AsyncCell::shared();
    let a = AsyncCell::shared();
    let b = AsyncCell::shared();
    let c = AsyncCell::shared();

    tick_future(x.get(), a.clone());
    tick_future(x.get(), b.clone());
    tick_future(x.get(), c.clone());

    x.set(42);
    assert!(a.is_set());
    assert!(b.is_set());
    assert!(c.is_set());
}

#[test]
fn new_into_inner() {
    assert_eq!(AsyncCell::<i32>::new().into_inner(), None);
    assert_eq!(AsyncCell::<i32>::new_with(0).into_inner(), Some(0));
}

#[test]
fn debug_text() {
    let cell = AsyncCell::new();
    assert_eq!(format!("{cell:?}"), "AsyncCell::Empty");
    tick_future(cell.get(), AsyncCell::shared());
    assert_eq!(format!("{cell:?}"), "AsyncCell::Pending");
    cell.set(0);
    assert_eq!(format!("{cell:?}"), "AsyncCell::Full(0)");
}

#[test]
#[ignore]
fn cell_size() {
    let size = size_of::<usize>();
    assert_eq!(size_of::<AsyncCell<()>>(), size * 3);
    assert_eq!(size_of::<AsyncCell<u8>>(), size * 3);
    assert_eq!(size_of::<AsyncCell<[usize; 1]>>(), size * 3);
    assert_eq!(size_of::<AsyncCell<[usize; 2]>>(), size * 3);
    assert_eq!(size_of::<AsyncCell<[usize; 3]>>(), size * 4);
}
