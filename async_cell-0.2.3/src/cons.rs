//! A waker cons cell.

use alloc::sync::Arc;
use alloc::task::Wake;
use core::task::Waker;

pub struct Cons(pub Waker, pub Waker);

impl Wake for Cons {
    fn wake(self: Arc<Self>) {
        match Arc::try_unwrap(self) {
            Ok(Cons(a, b)) => {
                a.wake();
                b.wake();
            }
            Err(arc) => Cons::wake_by_ref(&arc),
        }
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.wake_by_ref();
        self.1.wake_by_ref();
    }
}

pub fn wake_both(a: Waker, b: Waker) -> Waker {
    Arc::new(Cons(a, b)).into()
}
