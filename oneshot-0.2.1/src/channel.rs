#[cfg(not(oneshot_loom))]
use core::cell::UnsafeCell;
#[cfg(oneshot_loom)]
use loom::cell::UnsafeCell;

use core::mem::MaybeUninit;
use core::ptr;
#[cfg(feature = "async")]
use core::task::{self, Poll};

use crate::atomic::AtomicU8;
#[cfg(feature = "async")]
use crate::atomic::{Ordering::*, fence};
#[cfg(feature = "async")]
use crate::states::{DISCONNECTED, EMPTY, MESSAGE, RECEIVING};
use crate::waker::ReceiverWaker;

#[cfg(feature = "async")]
use crate::RecvError;

/// Internal channel data structure. The `channel` method allocates and puts one instance
/// of this struct on the heap for each oneshot channel instance.
pub struct Channel<T> {
    /// The current state of the channel. This is initialized to EMPTY, and always has the value
    /// of one of the constants in the `states` module. This atomic field is what allows the
    /// `Sender` and `Receiver` to communicate with each other and coordinate their actions
    /// in a thread safe manner.
    state: AtomicU8,

    /// The message in the channel. This memory is uninitialized until the message is sent.
    ///
    /// This field is wrapped in an `UnsafeCell` since interior mutability is required.
    /// Both ends of the channel will access this field mutably through a shared reference.
    message: UnsafeCell<MaybeUninit<T>>,

    /// The waker instance for the thread or task that is currently receiving on this channel.
    /// This memory is uninitialized until the receiver starts receiving.
    ///
    /// This field is wrapped in an `UnsafeCell` since interior mutability is required.
    /// Both ends of the channel will access this field mutably through a shared reference.
    waker: UnsafeCell<MaybeUninit<ReceiverWaker>>,
}

impl<T> Channel<T> {
    pub fn new() -> Self {
        Self {
            state: AtomicU8::new(crate::states::EMPTY),
            message: UnsafeCell::new(MaybeUninit::uninit()),
            waker: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }

    /// Returns a shared reference to the atomic state
    #[inline(always)]
    pub fn state(&self) -> &AtomicU8 {
        &self.state
    }

    /// Returns a reference to the message
    ///
    /// # Safety
    ///
    /// Must be called only when the caller can guarantee the message has been initialized, and
    /// no other thread will access the message field for the lifetime of the returned reference.
    #[inline(always)]
    pub unsafe fn message(&self) -> &T {
        // SAFETY: The caller guarantees that no other thread will access the message field.
        let message_container = unsafe {
            #[cfg(oneshot_loom)]
            {
                self.message.with(|ptr| &*ptr)
            }

            #[cfg(not(oneshot_loom))]
            {
                &*self.message.get()
            }
        };

        // SAFETY: The caller guarantees that the message has been initialized.
        unsafe { message_container.assume_init_ref() }
    }

    /// Runs a closure with mutable access to the message field in the channel.
    ///
    /// # Safety
    ///
    /// This uses interior mutability to provide mutable access via a shared reference to
    /// the channel. As a result, the caller must guarantee exclusive access to the message
    /// field during this call.
    #[inline(always)]
    unsafe fn with_message_mut<F>(&self, op: F)
    where
        F: FnOnce(&mut MaybeUninit<T>),
    {
        // SAFETY: The caller guarantees exclusive access to the message field.
        #[cfg(oneshot_loom)]
        unsafe {
            self.message.with_mut(|ptr| op(&mut *ptr))
        }

        // SAFETY: The caller guarantees exclusive access to the message field.
        #[cfg(not(oneshot_loom))]
        op(unsafe { &mut *self.message.get() })
    }

    /// Runs a closure with mutable access to the waker field in the channel.
    ///
    /// # Safety
    ///
    /// This uses interior mutability to provide mutable access via a shared reference to
    /// the channel. As a result, the caller must guarantee exclusive access to the waker
    /// field during this call.
    #[inline(always)]
    #[cfg(any(feature = "std", feature = "async"))]
    unsafe fn with_waker_mut<F>(&self, op: F)
    where
        F: FnOnce(&mut MaybeUninit<ReceiverWaker>),
    {
        #[cfg(oneshot_loom)]
        {
            // SAFETY: The caller guarantees exclusive access to the waker field.
            self.waker.with_mut(|ptr| op(unsafe { &mut *ptr }))
        }

        #[cfg(not(oneshot_loom))]
        {
            // SAFETY: The caller guarantees exclusive access to the waker field.
            op(unsafe { &mut *self.waker.get() })
        }
    }

    /// Writes a message to the message field in the channel. Will overwrite whatever
    /// is currently stored in the field. To avoid potential memory leaks, the caller
    /// should ensure that the waker field does not currently have any initialized
    /// waker in it before calling this function.
    ///
    /// # Safety
    ///
    /// Caller must guarantee exclusive access to the message field during this call.
    #[inline(always)]
    pub unsafe fn write_message(&self, message: T) {
        // SAFETY: The caller guarantees exclusive access to the message field.
        unsafe {
            self.with_message_mut(|slot| slot.as_mut_ptr().write(message));
        }
    }

    /// Reads the message from the channel and returns it.
    ///
    /// # Safety
    ///
    /// Must only be called after having observed the MESSAGE state with an acquire
    /// memory ordering to synchronize with the other thread's write of the message.
    #[inline(always)]
    pub unsafe fn take_message(&self) -> T {
        // SAFETY: The caller guarantees that no other thread will access the message field.
        let message_container = unsafe {
            #[cfg(oneshot_loom)]
            {
                self.message.with(|ptr| ptr::read(ptr))
            }
            #[cfg(not(oneshot_loom))]
            {
                ptr::read(self.message.get())
            }
        };

        // SAFETY: The caller guarantees that the message has been initialized.
        unsafe { message_container.assume_init() }
    }

    /// # Safety
    ///
    /// Must only be called after having observed the MESSAGE state with an acquire
    /// memory ordering to synchronize with the other thread's write of the message.
    #[inline(always)]
    pub unsafe fn drop_message(&self) {
        // SAFETY: The caller guarantees that the message has been initialized and that
        // we have exclusive access for the duration of this call.
        unsafe {
            self.with_message_mut(|slot| slot.assume_init_drop());
        }
    }

    /// Writes a waker to the waker field in the channel. Will overwrite whatever
    /// is currently stored in the field. To avoid potential memory leaks, the caller
    /// should ensure that the waker field does not currently have any initialized
    /// waker in it before calling this function.
    ///
    /// # Safety
    ///
    /// Caller must guarantee exclusive access to the waker field during this call.
    #[cfg(any(feature = "std", feature = "async"))]
    #[inline(always)]
    pub unsafe fn write_waker(&self, waker: ReceiverWaker) {
        // SAFETY: The caller guarantees exclusive access to the waker field.
        unsafe { self.with_waker_mut(|slot| slot.as_mut_ptr().write(waker)) };
    }

    /// # Safety
    ///
    /// Must be called only when the caller can guarantee the waker has been initialized (and
    /// not already dropped), and no other thread will access the waker field during this call.
    #[inline(always)]
    pub unsafe fn take_waker(&self) -> ReceiverWaker {
        // SAFETY: The caller guarantees that a waker has been initialized, and
        // that no other thread will access the waker field during this call.
        unsafe {
            #[cfg(oneshot_loom)]
            {
                self.waker.with(|ptr| ptr::read(ptr)).assume_init()
            }

            #[cfg(not(oneshot_loom))]
            {
                ptr::read(self.waker.get()).assume_init()
            }
        }
    }

    /// Runs the `Drop` implementation on the channel waker.
    ///
    /// # Safety
    ///
    /// Must be called only when the caller can guarantee the waker has been initialized (and
    /// not already dropped), and no other thread will access the waker field during this call.
    #[cfg(any(feature = "std", feature = "async"))]
    #[inline(always)]
    pub unsafe fn drop_waker(&self) {
        // SAFETY: The caller guarantees that a waker has been initialized, and that
        // we have exclusive access while this method runs.
        unsafe { self.with_waker_mut(|slot| slot.assume_init_drop()) };
    }

    /// # Safety
    ///
    /// * `Channel::waker` must not have a waker stored in it when calling this method.
    /// * Channel state must not be RECEIVING or UNPARKING when calling this method.
    #[cfg(feature = "async")]
    pub unsafe fn write_async_waker(
        &self,
        cx: &mut task::Context<'_>,
    ) -> Poll<Result<T, RecvError>> {
        // SAFETY: we are not yet in the RECEIVING state, meaning that the sender will not
        // try to access the waker until it sees the state set to RECEIVING below
        unsafe { self.write_waker(ReceiverWaker::task_waker(cx)) };

        // ORDERING: we use release ordering on success so the sender can synchronize with
        // our write of the waker. We use relaxed ordering on failure since the sender does
        // not need to synchronize with our write and the individual match arms handle any
        // additional synchronization
        match self
            .state
            .compare_exchange(EMPTY, RECEIVING, Release, Relaxed)
        {
            // We stored our waker, now we return and let the sender wake us up
            Ok(_) => Poll::Pending,
            // The sender sent the message while we prepared to park.
            // We take the message and mark the channel disconnected.
            Err(MESSAGE) => {
                // SAFETY: We wrote a waker above. The sender cannot have observed the
                // RECEIVING state, so it has not accessed the waker. We must drop it.
                unsafe { self.drop_waker() };

                // ORDERING: sender does not exist, so this update only needs to be visible to us
                self.state.store(DISCONNECTED, Relaxed);

                // ORDERING: Synchronize with the write of the message. This branch is
                // unlikely to be taken, so it's likely more efficient to use a fence here
                // instead of AcqRel ordering on the compare_exchange operation
                fence(Acquire);

                // SAFETY: The MESSAGE state + acquire ordering guarantees initialized message
                Poll::Ready(Ok(unsafe { self.take_message() }))
            }
            // The sender was dropped before sending anything while we prepared to park.
            Err(DISCONNECTED) => {
                // SAFETY: We wrote a waker above. The sender cannot have observed the
                // RECEIVING state, so it has not accessed the waker. We must drop it.
                unsafe { self.drop_waker() };

                Poll::Ready(Err(RecvError))
            }
            _ => unreachable!(),
        }
    }
}
