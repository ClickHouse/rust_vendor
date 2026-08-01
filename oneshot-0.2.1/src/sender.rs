use core::{marker::PhantomData, mem, ptr::NonNull};

use crate::Channel;
use crate::SendError;
use crate::atomic::Ordering::*;
use crate::dealloc;
use crate::states::{DISCONNECTED, EMPTY, MESSAGE, RECEIVING};

/// Sending end of a oneshot channel.
///
/// Created and returned from the [`channel`](crate::channel) function.
///
/// Can be used to send a message to the corresponding [`Receiver`](crate::Receiver).
#[derive(Debug)]
pub struct Sender<T> {
    channel_ptr: NonNull<Channel<T>>,
    // In reality we want contravariance, however we can't obtain that.
    //
    // Consider the following scenario:
    // ```
    // let (mut tx, rx) = channel::<&'short u8>();
    // let (tx2, rx2) = channel::<&'long u8>();
    //
    // tx = tx2;
    //
    // // Pretend short_ref is some &'short u8
    // tx.send(short_ref).unwrap();
    // let long_ref = rx2.recv().unwrap();
    // ```
    //
    // If this type were covariant then we could safely extend lifetimes, which is not okay.
    // Hence, we enforce invariance.
    _invariant: PhantomData<fn(T) -> T>,
}

// SAFETY: The core functionality of this library is to be able to pass channel ends to different
// threads to then be able to pass messages between threads or tasks.
// The sender only contains a pointer to the channel, and the entire library revolves around
// making sure the access to that channel object is properly synchronized
unsafe impl<T: Send> Send for Sender<T> {}

// SAFETY: The only methods that assumes there is only a single reference to the sender
// takes `self` by value, guaranteeing that there is only one reference to the sender at
// the time it is called.
unsafe impl<T: Sync> Sync for Sender<T> {}

impl<T> Sender<T> {
    /// # Safety
    ///
    /// * The pointer must be valid and point to a Channel<T>.
    /// * At most one [Sender] must exist for a channel at any point in time.
    pub(crate) unsafe fn new(channel_ptr: NonNull<Channel<T>>) -> Self {
        Self {
            channel_ptr,
            _invariant: PhantomData,
        }
    }

    /// Sends `message` over the channel to the corresponding [`Receiver`](crate::Receiver).
    ///
    /// Returns an error if the receiver has already been dropped. The message can
    /// be extracted from the error.
    ///
    /// This method is lock-free and wait-free when sending on a channel that the
    /// receiver is currently not receiving on. If the receiver is receiving during the send
    /// operation this method includes waking up the thread/task. Unparking a thread involves
    /// a mutex in Rust's standard library at the time of writing this.
    /// How lock-free waking up an async task is
    /// depends on your executor. If this method returns a `SendError`, please mind that dropping
    /// the error involves running any drop implementation on the message type, and freeing the
    /// channel's heap allocation, which might or might not be lock-free.
    ///
    /// This send call has a happens-before relationship with the various ways the receiver
    /// can obtain the message on the other side. The sending and receiving is synchronized in
    /// such a way that all operations and memory modifications before the send call is guaranteed
    /// to be visible to the receiving thread when in receives the message on the channel.
    pub fn send(self, message: T) -> Result<(), SendError<T>> {
        let channel_ptr = self.channel_ptr;

        // Don't run our Drop implementation if send was called, any cleanup now happens here
        mem::forget(self);

        // SAFETY: The channel exists on the heap for the entire duration of this method and we
        // only ever acquire shared references to it. Note that if the receiver disconnects it
        // does not free the channel.
        let channel = unsafe { channel_ptr.as_ref() };

        // Write the message into the channel on the heap.
        // SAFETY: The receiver only ever accesses this memory location if we are in the MESSAGE
        // state, and since we're responsible for setting that state, we can guarantee that we have
        // exclusive access to this memory location to perform this write.
        unsafe { channel.write_message(message) };

        // Set the state to signal there is a message on the channel.
        // ORDERING: we need release ordering to allow the receiver to synchronize with our write
        // of the message (and with our final write of the state, in the case where the receiver
        // becomes responsible for freeing the channel.)
        // We need Acquire ordering in the RECEIVING and DISCONNECTED branches as explained
        // further down.
        //
        // EMPTY + 1 = MESSAGE
        // RECEIVING + 1 = UNPARKING
        // DISCONNECTED + 1 = invalid, however this state is never observed
        match channel.state().fetch_add(1, AcqRel) {
            // The receiver is alive and has not started waiting. Send done.
            EMPTY => Ok(()),
            // The receiver is waiting. Wake it up so it can return the message.
            // We transitioned into the UNPARKING state. If the receiver observes this state
            // it will busy loop until it has observed the sender transitioning into another
            // state. As a result we do not have to worry about the receiver making changes
            // to the channel in this branch. But we must also be quick since the receiver
            // might be stuck in a loop.
            RECEIVING => {
                // Take the waker, but critically do not unpark it. If we unparked now, then the
                // receiving thread could still observe the UNPARKING state and re-park, meaning
                // that after we change to the MESSAGE state, it would remain parked indefinitely
                // or until a spurious wakeup.
                // SAFETY: at this point we are in the UNPARKING state, and the receiving thread
                // does not access the waker while in this state, nor does it free the channel
                // allocation in this state. The acquire ordering above establishes a happens-before
                // relationship with the writing of the waker.
                let waker = unsafe { channel.take_waker() };

                // ORDERING: this ordering serves two-fold: The release store synchronizes with
                // the acquire load in the receiving thread, ensuring that both our read of the
                // waker and write of the message happen-before the taking of the message and
                // freeing of the channel.
                // Furthermore, we need acquire ordering to ensure the unparking below
                // happens after the channel state is updated.
                //
                // We do not need to observe and act on the state that was replaced here.
                // in the UNPARKING state, the receiver must just wait for us to set a final state
                channel.state().swap(MESSAGE, AcqRel);

                // Note: it is possible that between the store above and this statement
                // the receiving thread is spuriously unparked, takes the message, and frees
                // the channel. However, we took ownership of the waker out of the channel,
                // and freeing the channel does not drop the waker since the
                // waker is wrapped in MaybeUninit. Therefore this data is valid regardless of
                // whether or not the receive has completed by this point.
                waker.unpark();

                Ok(())
            }
            // The receiver was already dropped. The `SendError` is responsible for freeing the
            // channel.
            // SAFETY: The acquire ordering in the fetch_add above synchronizes with the receiver's
            // write of the DISCONNECTED state. Since the receiver disconnected it will no longer
            // access `channel_ptr`, so we can transfer exclusive ownership of the channel's
            // resources to the error. Moreover, since we just placed the message in the channel,
            // the channel contains a valid message.
            DISCONNECTED => Err(unsafe { SendError::new(channel_ptr) }),
            _ => unreachable!(),
        }
    }

    /// Returns true if the associated [`Receiver`](crate::Receiver) has been dropped.
    ///
    /// If true is returned, a future call to send is guaranteed to return an error.
    pub fn is_closed(&self) -> bool {
        // SAFETY: The channel exists on the heap for the entire duration of this method and we
        // only ever acquire shared references to it. Note that if the receiver disconnects it
        // does not free the channel.
        let channel = unsafe { self.channel_ptr.as_ref() };

        // ORDERING: We *chose* a Relaxed ordering here as it sufficient to
        // enforce the method's contract: "if true is returned, a future
        // call to send is guaranteed to return an error."
        channel.state().load(Relaxed) == DISCONNECTED
    }

    /// Consumes the Sender, returning a raw pointer to the channel on the heap.
    ///
    /// This is intended to simplify using oneshot channels with some FFI code. The only safe thing
    /// to do with the returned pointer is to later reconstruct the Sender with [Sender::from_raw].
    /// Memory will leak if the Sender is never reconstructed.
    pub fn into_raw(self) -> *mut () {
        let raw = self.channel_ptr.as_ptr() as *mut ();
        mem::forget(self);
        raw
    }

    /// Consumes a raw pointer from [Sender::into_raw], recreating the Sender.
    ///
    /// # Safety
    ///
    /// This pointer must have come from [`Sender<T>::into_raw`] with the same message type, `T`.
    /// At most one Sender must exist for a channel at any point in time.
    /// Constructing multiple Senders from the same raw pointer leads to undefined behavior.
    pub unsafe fn from_raw(raw: *mut ()) -> Self {
        // SAFETY: Method guarantee that the pointer is valid and points to a Channel<T>.
        let channel_ptr = unsafe { NonNull::new_unchecked(raw as *mut Channel<T>) };
        // SAFETY: The above guarantees the pointer is valid and there is only one Sender
        unsafe { Self::new(channel_ptr) }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        // SAFETY: The receiver only ever frees the channel if we are in the MESSAGE or
        // DISCONNECTED states. If we are in the MESSAGE state, then we called
        // mem::forget(self), so we should not be in this function call. If we are in the
        // DISCONNECTED state, then the receiver either received a MESSAGE so this statement is
        // unreachable, or was dropped and observed that our side was still alive, and thus didn't
        // free the channel.
        let channel = unsafe { self.channel_ptr.as_ref() };

        // Set the channel state to disconnected and read what state the channel was in
        // ORDERING: Release is required so that in the states where the receiver becomes
        // responsible for deallocating the channel, they can synchronize with this final state
        // write from us.
        // Acquire is required by most branches to synchronize with writes in the sender.
        // See each branch for details.

        // ORDERING: we don't need release ordering here since there are no modifications we
        // need to make visible to other thread, and the Err(RECEIVING) branch handles
        // synchronization independent of this cmpxchg
        //
        // EMPTY ^ 001 = DISCONNECTED
        // RECEIVING ^ 001 = UNPARKING
        // DISCONNECTED ^ 001 = EMPTY (invalid), but this state is never observed
        match channel.state().fetch_xor(0b001, AcqRel) {
            // The receiver is not waiting, nor is it dropped. The receiver is now
            // responsible for deallocating the channel.
            EMPTY => (),
            // The receiver is waiting. Wake it up so it can detect that the channel disconnected.
            // We transitioned into the UNPARKING state. If the receiver observes this state
            // it will busy loop until it has observed the sender transitioning into another
            // state. As a result we do not have to worry about the receiver making changes
            // to the channel in this branch. But we must also be quick since the receiver
            // might be stuck in a loop.
            RECEIVING => {
                // See comments in Sender::send

                // SAFETY: The RECEIVING state plus acquire ordering guarantees the receiver has
                // written a waker and that it has a happens-before relationship with this read.
                let waker = unsafe { channel.take_waker() };

                // ORDERING: Release ordering ensures our read of the waker happens
                // before this state swap. Release ordering is also required since we write a
                // state that allows the receiver to deallocate the channel, and it must
                // synchronize with our final write of the state.
                // Acquire ordering ensures the unparking of the receiver happens after this write.
                //
                // We do not need to observe and act on the state that was replaced here.
                // in the UNPARKING state, the receiver must just wait for us to set a final state
                channel.state().swap(DISCONNECTED, AcqRel);

                waker.unpark();
            }
            // The receiver was already dropped. We are responsible for freeing the channel.
            DISCONNECTED => {
                // SAFETY: when the receiver switches the state to DISCONNECTED they have received
                // the message or will no longer be trying to receive the message, and have
                // observed that the sender is still alive, meaning that we're responsible for
                // freeing the channel allocation.
                //
                // The acquire ordering of the fetch_xor above synchronize with the
                // receiver's final write of the state. So we can safely deallocate it.
                unsafe { dealloc(self.channel_ptr) };
            }
            _ => unreachable!(),
        }
    }
}
