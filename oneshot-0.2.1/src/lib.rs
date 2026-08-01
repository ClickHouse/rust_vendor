//! Oneshot spsc (single producer, single consumer) channel. Meaning each channel instance
//! can only transport a single message. This has a few nice outcomes. One thing is that
//! the implementation can be very efficient, utilizing the knowledge that there will
//! only be one message. But more importantly, it allows the API to be expressed in such
//! a way that certain edge cases that you don't want to care about when only sending a
//! single message on a channel does not exist. For example: The sender can't be copied
//! or cloned, and the send method takes ownership and consumes the sender.
//! So you are guaranteed, at the type level, that there can only be one message sent.
//!
//! The sender's send method is non-blocking, and potentially lock- and wait-free.
//! See documentation on [Sender::send] for situations where it might not be fully wait-free.
//! The receiver supports both lock- and wait-free `try_recv` as well as indefinite and time
//! limited thread blocking receive operations. The receiver also implements `IntoFuture` and
//! supports asynchronously awaiting the message.
//!
//!
//! # Examples
//!
//! This example sets up a background worker that processes requests coming in on a standard
//! mpsc channel and replies on a oneshot channel provided with each request. The worker can
//! be interacted with both from sync and async contexts since the oneshot receiver
//! can receive both blocking and async.
//!
//! ```rust
//! # #[cfg(not(feature = "loom"))] {
//! use std::sync::mpsc;
//! use std::thread;
//! use std::time::Duration;
//!
//! type Request = String;
//!
//! // Starts a background thread performing some computation on requests sent to it.
//! // Delivers the response back over a oneshot channel.
//! fn spawn_processing_thread() -> mpsc::Sender<(Request, oneshot::Sender<usize>)> {
//!     let (request_sender, request_receiver) = mpsc::channel::<(Request, oneshot::Sender<usize>)>();
//!     thread::spawn(move || {
//!         for (request_data, response_sender) in request_receiver.iter() {
//!             let compute_operation = || request_data.len();
//!             let _ = response_sender.send(compute_operation()); // <- Send on the oneshot channel
//!         }
//!     });
//!     request_sender
//! }
//!
//! let processor = spawn_processing_thread();
//!
//! // If compiled with `std` the library can receive messages with timeout on regular threads
//! #[cfg(feature = "std")] {
//!     let (response_sender, response_receiver) = oneshot::channel();
//!     let request = Request::from("data from sync thread");
//!
//!     processor.send((request, response_sender)).expect("Processor down");
//!     match response_receiver.recv_timeout(Duration::from_secs(1)) { // <- Receive on the oneshot channel
//!         Ok(result) => println!("Processor returned {}", result),
//!         Err(oneshot::RecvTimeoutError::Timeout) => eprintln!("Processor was too slow"),
//!         Err(oneshot::RecvTimeoutError::Disconnected) => panic!("Processor exited"),
//!     }
//! }
//!
//! // If compiled with the `async` feature, the `Receiver` can be awaited in an async context
//! #[cfg(feature = "async")] {
//!     tokio::runtime::Runtime::new()
//!         .unwrap()
//!         .block_on(async move {
//!             let (response_sender, response_receiver) = oneshot::channel();
//!             let request = Request::from("data from sync thread");
//!
//!             processor.send((request, response_sender)).expect("Processor down");
//!             match response_receiver.await { // <- Receive on the oneshot channel asynchronously
//!                 Ok(result) => println!("Processor returned {}", result),
//!                 Err(_e) => panic!("Processor exited"),
//!             }
//!         });
//! }
//! # }
//! ```
//!
//! # Send has happens-before relationship with receive
//!
//! All the various ways the `Receiver` can obtain the message out of the channel is synchronized
//! with the `Sender`s `send` method. This means any operations and memory modifications done in
//! the sender thread before the call to `Sender::send` are guaranteed to happen before any code
//! running after the message has been received in the receiver thread.
//!
//! # Sync vs async
//!
//! The main motivation for writing this library was that there were no (known to me) channel
//! implementations allowing you to seamlessly send messages between a normal thread and an async
//! task, or the other way around. If message passing is the way you are communicating, of course
//! that should work smoothly between the sync and async parts of the program!
//!
//! This library achieves that by having a fast and cheap send operation that can
//! be used in both regular threads and async tasks. The receiver has both thread blocking
//! receive methods for synchronous usage, and implements `IntoFuture` for asynchronous usage.
//!
//! The receiving endpoint of this channel implements Rust's `IntoFuture` trait and can be waited on
//! in an asynchronous task. This implementation is completely executor/runtime agnostic. It should
//! be possible to use this library with any executor, or even pass messages between tasks running
//! in different executors.
//!

// # Implementation description
//
// When a channel is created via the `channel` function, it creates a single heap allocation
// containing:
// * A one byte atomic integer that represents the current channel state,
// * Uninitialized memory to fit the message,
// * Uninitialized memory to fit the waker that can wake the receiving task or thread up.
//
// The size of the waker depends on which features are activated, it ranges from 0 to 24 bytes[1].
// So with all features enabled each channel allocates 25 bytes plus the size of the
// message, plus any padding needed to get correct memory alignment.
//
// The Sender and Receiver only holds a raw pointer to the heap channel object. The last endpoint
// to be consumed or dropped is responsible for freeing the heap memory. The first endpoint to
// be consumed or dropped signal via the state that it is gone. And the second one see this and
// frees the memory.
//
// ## Footnotes
//
// [1]: Mind that the waker only takes zero bytes when all features are disabled, making it
//      impossible to *wait* for the message. `try_recv` is the only available method in this scenario.

#![deny(rust_2018_idioms)]
#![cfg_attr(not(feature = "std"), no_std)]
// Enables this nightly only feature for the documentation build on docs.rs.
// To test this locally, build the docs with:
// `RUSTDOCFLAGS="--cfg docsrs" cargo +nightly doc --all-features`
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(not(oneshot_loom))]
extern crate alloc;

use core::ptr::NonNull;

mod atomic;

mod channel;
use channel::Channel;

mod sender;
pub use sender::Sender;

mod receiver;
#[cfg(feature = "async")]
pub use receiver::AsyncReceiver;
pub use receiver::Receiver;

mod states;

#[cfg(feature = "std")]
mod thread;

mod waker;

#[cfg(oneshot_loom)]
mod loombox;
#[cfg(not(oneshot_loom))]
use alloc::boxed::Box;
#[cfg(oneshot_loom)]
use loombox::Box;

mod errors;
// Wildcard imports are not nice. But since multiple errors have various conditional compilation,
// this is easier than doing three different imports.
pub use errors::*;

/// Creates a new oneshot channel and returns the two endpoints, [`Sender`] and [`Receiver`].
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    // Allocate the channel on the heap and get the pointer.
    // The last endpoint of the channel to be alive is responsible for freeing the channel
    // and dropping any object that might have been written to it.
    let channel_ptr = NonNull::from(Box::leak(Box::new(Channel::new())));

    (
        // SAFETY: The pointer is valid, and we only create a single `Sender`
        unsafe { Sender::new(channel_ptr) },
        // SAFETY: The pointer is valid, and we only create a single `Receiver`
        unsafe { Receiver::new(channel_ptr) },
    )
}

/// Ergonomic shorthand for creating a channel and immediately convert the [`Receiver`] into
/// a future.
///
/// This can be useful when you need to pass the receiver to a function that expects a
/// type implementing [`Future`] directly. Using this function is not necessary when
/// you are going to use `.await` on the receiver, as that will automatically call
/// [`IntoFuture::into_future`] in the background.
#[cfg(feature = "async")]
#[inline(always)]
pub fn async_channel<T>() -> (Sender<T>, AsyncReceiver<T>) {
    let (sender, receiver) = channel();
    let async_receiver = core::future::IntoFuture::into_future(receiver);
    (sender, async_receiver)
}

/// Deallocates the channel's heap allocation (created in `oneshot::channel()`).
///
/// # Safety
///
/// * `channel` must be a valid pointer to a `Channel<T>` originally coming from
///   `oneshot::channel()`.
/// * The thread calling this function must have properly synchronized with any other thread
///   that has used the channel (either the `Sender` or `Receiver`). This means having an
///   acquire memory barrier on or after the loading of `channel.state` that determined that
///   the other thread is fully done using the channel and we are responsible for freeing it.
#[inline]
pub(crate) unsafe fn dealloc<T>(channel: NonNull<Channel<T>>) {
    // SAFETY: Method guarantee that the pointer is valid and points to a Channel<T>.
    drop(unsafe { Box::from_raw(channel.as_ptr()) });
}
