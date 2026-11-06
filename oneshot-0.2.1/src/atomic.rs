#[cfg(not(oneshot_loom))]
pub use core::sync::atomic::{AtomicU8, Ordering, fence};
#[cfg(oneshot_loom)]
pub use loom::sync::atomic::{AtomicU8, Ordering, fence};
