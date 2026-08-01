//! This module contains all the u8 constants that the channel state can be in.
//!
//! These values are very explicitly chosen so that we can replace some cmpxchg calls with
//! fetch_* calls which can be cheaper.

/// The initial channel state. Active while both endpoints are still alive, no message has been
/// sent, and the receiver is not receiving.
pub const EMPTY: u8 = 0b011;

/// A message has been sent to the channel, but the receiver has not yet read it.
pub const MESSAGE: u8 = 0b100;

/// No message has yet been sent on the channel, but the receiver is currently receiving.
pub const RECEIVING: u8 = 0b000;

/// The `Sender` has observed the `RECEIVING` state and is currently reading out the waker
/// from the channel and preparing to unpark the receiver. During this state, the `Sender`
/// has exclusive access to the channel's waker field. If the `Receiver` observes this state,
/// it must wait for the `Sender` to finish, which it does by updating the state again, away
/// from `UNPARKING`.
#[cfg(any(feature = "std", feature = "async"))]
pub const UNPARKING: u8 = 0b001;

/// The channel has been closed. This means that either the sender or receiver has been dropped,
/// or the message sent to the channel has already been received. Since this is a oneshot
/// channel, it is disconnected after the one message it is supposed to hold has been
/// transmitted.
pub const DISCONNECTED: u8 = 0b010;
