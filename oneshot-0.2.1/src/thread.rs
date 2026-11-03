//! This module by default just re-exports needed items from `std::thread`.
//! But when loom testing is enabled, these items are instead swapped to
//! the equivalent from the `loom` crate.

#[cfg(not(oneshot_loom))]
pub use std::thread::{Thread, current, park, park_timeout};

#[cfg(oneshot_loom)]
pub use loom::thread::{Thread, current, park};

// loom does not support parking with a timeout. So we just
// yield. This means that the "park" will "spuriously" wake up
// way too early. But the code should properly handle this.
// One thing to note is that very short timeouts are needed
// when using loom, since otherwise the looping will cause
// an overflow in loom.
#[cfg(oneshot_loom)]
pub fn park_timeout(_timeout: std::time::Duration) {
    loom::thread::yield_now()
}
