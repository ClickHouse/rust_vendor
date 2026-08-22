#[cfg(feature = "std")]
use crate::thread;
#[cfg(feature = "async")]
use core::task;

pub enum ReceiverWaker {
    /// The receiver is waiting synchronously. Its thread is parked.
    #[cfg(feature = "std")]
    Thread(thread::Thread),
    /// The receiver is waiting asynchronously. Its task can be woken up with this `Waker`.
    #[cfg(feature = "async")]
    Task(task::Waker),
    /// A little hack to not make this enum an uninhibitable type when no features are enabled.
    #[cfg(not(any(feature = "async", feature = "std")))]
    _Uninhabited,
}

impl ReceiverWaker {
    #[cfg(feature = "std")]
    pub fn current_thread() -> Self {
        Self::Thread(thread::current())
    }

    #[cfg(feature = "async")]
    pub fn task_waker(cx: &task::Context<'_>) -> Self {
        Self::Task(cx.waker().clone())
    }

    pub fn unpark(self) {
        match self {
            #[cfg(feature = "std")]
            ReceiverWaker::Thread(thread) => thread.unpark(),
            #[cfg(feature = "async")]
            ReceiverWaker::Task(waker) => waker.wake(),
            #[cfg(not(any(feature = "async", feature = "std")))]
            ReceiverWaker::_Uninhabited => unreachable!(),
        }
    }
}

#[cfg(not(oneshot_loom))]
#[test]
#[ignore = "Unstable test. Different Rust versions have different sizes for Thread"]
fn receiver_waker_size() {
    let expected: usize = match (cfg!(feature = "std"), cfg!(feature = "async")) {
        (false, false) => 0,
        (false, true) => 16,
        (true, false) => 16,
        (true, true) => 24,
    };
    assert_eq!(core::mem::size_of::<ReceiverWaker>(), expected);
}
