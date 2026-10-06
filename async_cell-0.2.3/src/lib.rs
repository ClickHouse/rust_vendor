#![cfg_attr(feature = "no_std", no_std)]

//! The key type of this crate is [`AsyncCell`](sync::AsyncCell) which can be
//! found in both thread-safe and single-threaded variants. It is intended as a
//! useful async primitive which can replace more expensive channels in a fair
//! number of cases.
//!
//! > `AsyncCell<T>` behaves a lot like a `Cell<Option<T>>` that you can await
//! on.
//!
//! This is used to create futures from a callbacks:
//! ```
//! # #[path="../tests/utils.rs"] mod utils;
//! # use loom::thread;
//! # utils::root(|| async {
//! use async_cell::sync::AsyncCell;
//!
//! let cell = AsyncCell::shared();
//! let future = cell.take_shared();
//!
//! thread::spawn(move || cell.set("Hello, World!"));
//!
//! println!("{}", future.await);
//! # });
//! ```
//!
//! You can also use an async_cell for static variable initialization, wherever
//! the blocking behavior of a `OnceCell` is unacceptable:
//! ```
//! # #[path="../tests/utils.rs"] mod utils;
//! # use loom::thread;
//! # macro_rules! println {
//! # ("{}", $x:expr) => { assert_eq!($x, "Hello, World!\n"); };
//! # }
//! # utils::root(|| async {
//! use async_cell::sync::AsyncCell;
//!
//! // AsyncCell::new() is const!
//! static GREETING: AsyncCell<String> = AsyncCell::new();
//! # GREETING.try_take();
//!
//! // Read the file on a background thread,
//! // setting a placeholder value if the thread panics.
//! thread::spawn(|| {
//!     let greeting = GREETING.guard("ERROR".to_string());
//!     let hello = std::fs::read_to_string("tests/hello.txt").unwrap();
//!     greeting.set(hello);
//! });
//!
//! // Do some work while waiting for the file.
//!
//! // And greet the user!
//! println!("{}", &GREETING.get().await);
//! # assert!(true);
//! # });
//! ```
//!
//! Async cells can also be used to react to the latest value of a variable,
//! since the same cell can be reused as many times as desired. This is one
//! way `AsyncCell` differs from a one-shot channel:
//! ```
//! # #[path="../tests/utils.rs"] mod utils;
//! # use utils::spawn;
//! # utils::root(|| async {
//! use async_cell::sync::AsyncCell;
//!
//! // Allocate space for our timer.
//! let timer = AsyncCell::<i32>::shared();
//!
//! // Try to print out the time as fast as it updates.
//! // Some ticks will be skipped if this loop runs too slowly!
//! let watcher = timer.take_weak();
//! spawn(async move {
//!     while let Some(time) = (&watcher).await {
//!         println!("Launch in T-{} ticks!", time);
//!     }
//! });
//!
//! // Begin counting down!
//! for i in (0..60).rev() {
//!     timer.set(i);
//! }
//! # });
//! ```
//!
//! Although this crate contains a number of utility functions, you can often
//! make due with just [`AsyncCell::new`](sync::AsyncCell::new),
//! [`AsyncCell::set`](sync::AsyncCell::set), and
//! [`AsyncCell::take`](sync::AsyncCell::take).
//!
//! ## Limitations
//!
//! Cells are not channels! Channels will queue all sent values until a receiver
//! can process them. Readers of a cell will only ever see the most recently
//! written value. As an example, imagine a GUI with a text box. An `AsyncCell`
//! would be perfect to watch the text content of the box, since it is not
//! necessary to send the whole thing on every keystroke. But the keystrokes
//! themselves must be sent to the box via a channel to avoid any getting lost.
//!
//! Also avoid using `AsyncCell` in situations with high contention. Cells block
//! momentarily while cloning values, allocating async callbacks, etc.
//! As a rule of thumb, try to fill cells from one thread or task and empty from one other.
//! _Although multiple futures can wait on the same cell, that case is not highly optimized._

extern crate alloc;

mod cons;
mod internal;

macro_rules! impl_async_cell_part1 {
    ($cell_use:expr, $inner:ty, $inner_arg:ident, $shared:ident) => {
        use core::{fmt, future::Future, ops::Deref, pin::Pin, task::Context, task::Poll};

        /// Used to remove the value of a cell. Can be constructed directly or
        /// with [AsyncCell::take].
        ///
        /// Note that a single instance of `Take` can be used multiple times to
        /// consume a sequence of values. For example:
        /// ```
        /// # #[path="../tests/utils.rs"] mod utils;
        /// # utils::root(|| async {
        #[doc = $cell_use]
        ///
        /// let cell = AsyncCell::new();
        /// let taker = cell.take();
        ///
        /// cell.set(1);
        /// assert_eq!(taker.await, 1);
        ///
        /// cell.set(2);
        /// assert_eq!(taker.await, 2);
        ///
        /// # });
        /// ```
        #[repr(transparent)]
        #[must_use]
        #[derive(Clone, Copy)]
        pub struct Take<C>(pub C);

        /// A similar future to [Take], but resolves to None when the
        /// cell is dropped. Can be constructed directly or
        /// with [AsyncCell::take_weak].
        #[repr(transparent)]
        #[must_use]
        pub struct TakeWeak<T>(pub Weak<AsyncCell<T>>);

        impl<T> Clone for TakeWeak<T> {
            fn clone(&self) -> Self {
                Self(self.0.clone())
            }
        }

        /// A similar future to [TakeWeak], but uses the weakref crate.
        ///
        /// There is not currently a method to construct this, since
        /// it would need to be on [weakref::Own].
        #[repr(transparent)]
        #[must_use]
        #[cfg(feature = "weakref")]
        pub struct TakeRef<T>(pub weakref::Ref<AsyncCell<T>>);

        #[cfg(feature = "weakref")]
        impl<T> Clone for TakeRef<T> {
            fn clone(&self) -> Self {
                *self
            }
        }

        #[cfg(feature = "weakref")]
        impl<T> Copy for TakeRef<T> {}

        /// Used to clone the value of a cell. Can be constructed directly or
        /// with [AsyncCell::get].
        ///
        /// Note that a single instance of `Get` can be used multiple times to
        /// consume a sequence of values. For example:
        /// ```
        /// # #[path="../tests/utils.rs"] mod utils;
        /// # utils::root(|| async {
        #[doc = $cell_use]
        ///
        /// let cell = AsyncCell::new();
        /// let getter = cell.get();
        ///
        /// cell.set(1);
        /// assert_eq!(getter.await, 1);
        /// assert_eq!(getter.await, 1);
        ///
        /// # });
        /// ```
        #[repr(transparent)]
        #[must_use]
        #[derive(Clone, Copy)]
        pub struct Get<C>(pub C);

        /// A similar future to [Get], but resolves to None when the
        /// cell is dropped. Can be constructed directly or
        /// with [AsyncCell::get_weak].
        #[repr(transparent)]
        #[must_use]
        pub struct GetWeak<T>(pub Weak<AsyncCell<T>>);

        impl<T> Clone for GetWeak<T> {
            fn clone(&self) -> Self {
                Self(self.0.clone())
            }
        }

        /// A similar future to [GetWeak], but uses the weakref crate.
        ///
        /// There is not currently a method to construct this, since
        /// it would need to be on [weakref::Own].
        #[repr(transparent)]
        #[must_use]
        #[cfg(feature = "weakref")]
        pub struct GetRef<T>(pub weakref::Ref<AsyncCell<T>>);

        #[cfg(feature = "weakref")]
        impl<T> Clone for GetRef<T> {
            fn clone(&self) -> Self {
                *self
            }
        }

        #[cfg(feature = "weakref")]
        impl<T> Copy for GetRef<T> {}

        /// A utility wrapper to set a given `AsyncCell<T>` when dropped.
        pub struct GuardedCell<T, C: Deref<Target = AsyncCell<T>>> {
            inner: C,
            cancel: Option<T>,
        }

        /// An async primitive. Similar to a `Cell<Option<T>>` but awaitable.
        ///
        /// This type should generally have much lower overhead vs an equivalent
        /// channel although it may not handle heavy contention as well. In fact,
        /// it may only be a few bytes larger than `T` itself.
        pub struct AsyncCell<$inner_arg = ()> {
            cell: $inner,
        }
    };
}

macro_rules! impl_async_cell_part2 {
    ($cell_use:expr, $shared:ident) => {
        impl<T> AsyncCell<T> {
            /// Set the value of the cell. If it was previously empty, wake up a
            /// single arbitrary call [`take`](Self::take) and/or all calls
            /// to [`get`](Self::get).
            ///
            /// This is probably the most important function in the whole crate
            /// since it can be used to resolve some future X with some value Y
            /// at a distance.
            ///
            /// ```
            /// # #[path="../tests/utils.rs"] mod utils;
            /// # use utils::spawn;
            /// # utils::root(|| async {
            #[doc = $cell_use]
            ///
            /// let cell = AsyncCell::shared();
            /// let message = cell.take_shared();
            /// # let cell2 = AsyncCell::shared();
            /// # let cell3 = cell2.clone();
            ///
            /// spawn(async move {
            ///     println!("{}", message.await)
            /// # ; cell2.notify();
            /// });
            ///
            /// cell.set("Hello, World!");
            /// # cell3.take().await;
            /// # })
            /// ```
            pub fn set(&self, value: T) {
                crate::internal::set(&self.cell, value);
            }

            /// Once woken up with a value, remove it and resolve.
            ///
            /// ```
            /// # #[path="../tests/utils.rs"] mod utils;
            /// # use utils::spawn;
            /// # utils::root(|| async {
            #[doc = $cell_use]
            ///
            /// let cell1 = AsyncCell::shared();
            /// let cell2 = cell1.clone();
            ///
            /// spawn(async move {
            ///     cell1.set(vec![1i32, 2, 3]);
            /// });
            ///
            /// assert_eq!(&cell2.take().await, &[1, 2, 3]);
            ///
            /// // Awaiting again on the cell would block forever!
            /// // The value is now None.
            /// assert_eq!(cell2.try_take(), None);
            /// # });
            /// ```
            ///

            /// Technically, the returned type is just a reference to this cell.
            /// It must be driven by `await` to actually move the internal data.
            /// If borrowing this cell is unacceptable, consider directly
            /// constructing the [`Take`] wrapper around a smart-pointer of your
            /// own choosing.
            pub fn take(&self) -> Take<&Self> {
                Take(self)
            }

            /// Once woken up with a value, clone it and resolve.
            ///
            /// ```
            /// # #[path="../tests/utils.rs"] mod utils;
            /// # use utils::spawn;
            /// # utils::root(|| async {
            #[doc = $cell_use]
            ///
            /// let cell1 = AsyncCell::shared();
            /// let cell2 = cell1.clone();
            ///
            /// spawn(async move {
            ///     cell1.set(vec![1i32, 2, 3]);
            /// });
            ///
            /// assert_eq!(&cell2.get().await, &[1, 2, 3]);
            /// assert_eq!(&cell2.get().await, &[1, 2, 3]);
            /// # });
            /// ```
            ///

            /// Technically, the returned type is just a reference to this cell.
            /// It must be driven by `await` to actually move the internal data.
            /// If borrowing this cell is unacceptable, consider directly
            /// constructing the [`Get`] wrapper around a smart-pointer of your
            /// own choosing.
            pub fn get(&self) -> Get<&Self>
            where
                T: Clone,
            {
                Get(self)
            }

            /// If the cell currently has a value, remove it.
            ///
            /// ```
            #[doc = $cell_use]
            ///
            /// let cell = AsyncCell::new();
            ///
            /// cell.set(420);
            ///
            /// assert_eq!(cell.try_take(), Some(420));
            /// assert_eq!(cell.try_take(), None);
            /// ```
            pub fn try_take(&self) -> Option<T> {
                crate::internal::try_take(&self.cell)
            }

            /// Clones the current value of the cell.
            ///
            /// ```
            #[doc = $cell_use]
            ///
            /// let cell = AsyncCell::new();
            ///
            /// // Value starts out empty.
            /// assert_eq!(cell.try_get(), None);
            ///
            /// cell.set(420);
            ///
            /// // Value is now set.
            /// assert_eq!(cell.try_get(), Some(420));
            /// ```
            pub fn try_get(&self) -> Option<T>
            where
                T: Clone,
            {
                crate::internal::try_get(&self.cell)
            }

            /// Set the value of the cell _if it is empty_, waking up any
            /// attached futures.
            pub fn or_set(&self, value: T) {
                crate::internal::or_set(&self.cell, value);
            }

            /// Is the cell currently full?
            pub fn is_set(&self) -> bool {
                crate::internal::is_set(&self.cell)
            }

            /// Replace the value of the cell, returning the previous value. If
            /// the previous value is empty, wake up any attached futures.
            pub fn replace(&self, value: T) -> Option<T> {
                crate::internal::set(&self.cell, value)
            }

            /// Atomically update the value of this cell using the given
            /// function.
            ///
            /// If the value transitions from None to Some, it acts like a call
            /// to `set`. Otherwise it only effects the internals of the cell.
            ///
            /// Note: avoid doing anything time consuming in the passed
            /// function, since it will block any async runtime thread in the
            /// process of poll-ing a `Get` or `Take` future.
            pub fn update(&self, with: impl FnOnce(Option<T>) -> Option<T>) {
                crate::internal::update(&self.cell, with);
            }

            /// Atomically update the value of this cell if it is set, using the
            /// given function.
            ///
            /// Note: avoid doing anything time consuming in the passed
            /// function, since it will block any async runtime thread in the
            /// process of poll-ing a `Get` or `Take` future.
            pub fn update_some(&self, with: impl FnOnce(T) -> T) {
                crate::internal::update(&self.cell, |x| match x {
                    Some(x) => Some(with(x)),
                    None => None,
                });
            }

            /// Destroys this cell, returning the value inside.
            pub fn into_inner(self) -> Option<T> {
                crate::internal::into_inner(self.cell)
            }

            /// Used to ensure this cell is set to some value even when panicking
            /// or returning errors. This is useful in preventing deadlocks, signaling
            /// shutdown, etc.
            ///
            /// ```
            /// # #[path="../tests/utils.rs"] mod utils;
            /// # use utils::{spawn, Unordered};
            /// # fn send(_: u32) {}
            /// # utils::root(|| async {
            #[doc = $cell_use]
            ///
            /// let latest_val = AsyncCell::shared();
            ///
            /// let latest_val_ref = latest_val.clone();
            /// let read_content = move |path: &str| -> Option<u32> {
            ///     let latest_val = latest_val_ref.guard(Err(format!("{:?} is not an int", path)));
            ///     let text = std::fs::read_to_string(path).ok()?;
            ///     let val = text.parse().ok()?;
            ///     latest_val.set(Ok(val));
            ///     Some(val)
            /// };
            ///
            /// spawn(async move {
            ///     if let Some(val) = read_content("test/hello.txt") {
            ///         send(val);
            ///     }
            /// });
            ///
            /// match latest_val.get().await {
            ///     Ok(text) => println!("{}", text),
            ///     Err(text) => eprintln!("Error: {}", text),
            /// }
            /// # })
            /// ```
            pub fn guard(&self, cancel: T) -> GuardedCell<T, &Self> {
                GuardedCell {
                    inner: self,
                    cancel: Some(cancel),
                }
            }
        }

        /// These are trivial wrappers around [AsyncCell::set] and
        /// [AsyncCell::take] which are useful in the case where you want
        /// "waking up" functionality, without any actual data being sent.
        impl AsyncCell<()> {
            /// Wake up a single arbitrary call to [`wait`](Self::wait).
            pub fn notify(&self) {
                crate::internal::set(&self.cell, ());
            }

            /// Resolves once [`notify`](Self::notify) is called.
            pub fn wait(&self) -> Take<&AsyncCell> {
                Take(&self)
            }

            /// Resolves once [`notify`](Self::notify) is called.
            pub fn wait_shared(self: &$shared<Self>) -> Take<$shared<AsyncCell>> {
                Take(self.clone())
            }
        }

        impl<T> AsyncCell<T> {
            /// Create an empty AsyncCell which can be shared between locations with
            /// `.clone()`.
            pub fn shared() -> $shared<Self> {
                AsyncCell::new().into_shared()
            }

            /// Wraps an existing cell in a reference-counted pointer, so it can
            /// be shared between locations with `.clone()`.
            pub fn into_shared(self) -> $shared<Self> {
                $shared::new(self)
            }

            /// Like [`take`](Self::take) but doesn't borrow self.
            pub fn take_shared(self: &$shared<Self>) -> Take<$shared<Self>> {
                Take(self.clone())
            }

            /// Like [`get`](Self::get) but doesn't borrow self.
            pub fn get_shared(self: &$shared<Self>) -> Get<$shared<Self>>
            where
                T: Clone,
            {
                Get(self.clone())
            }

            /// Like [`guard`](Self::guard) but doesn't borrow self.
            pub fn guard_shared(self: &$shared<Self>, cancel: T) -> GuardedCell<T, $shared<Self>> {
                GuardedCell {
                    inner: self.clone(),
                    cancel: Some(cancel),
                }
            }

            /// Like [`take`](Self::take) but creates a weak reference to self.
            /// If all strong references to self are dropped, the returned future
            /// will resolve to None even if the cell was full at the time.
            ///
            /// ```
            /// # #[path="../tests/utils.rs"] mod utils;
            /// # use utils::spawn;
            /// # utils::root(|| async {
            #[doc = $cell_use]
            ///
            /// let cell = AsyncCell::shared();
            /// let taker = cell.take_weak();
            ///
            /// // The cell can be used normally
            /// cell.set(42);
            /// assert_eq!((&taker).await, Some(42));
            ///
            /// // Will resolve after being dropped
            /// spawn(async move {
            ///     cell.set(43); // any current value will be dropped
            ///     drop(cell);
            /// });
            /// assert_eq!(taker.await, None);
            /// # });
            /// ```
            pub fn take_weak(self: &$shared<Self>) -> TakeWeak<T> {
                TakeWeak($shared::downgrade(self))
            }

            /// Like [`get`](Self::get) but creates a weak reference to self.
            /// If self is dropped, the returned future will resolve to None.
            ///
            /// ```
            /// # #[path="../tests/utils.rs"] mod utils;
            /// # use utils::spawn;
            /// # utils::root(|| async {
            #[doc = $cell_use]
            ///
            /// let cell = AsyncCell::<i32>::shared();
            /// let getter = cell.get_weak();
            ///
            /// spawn(async move {
            ///     cell.set(43); // any current value will be dropped
            ///     drop(cell);
            /// });
            /// assert_eq!(getter.await, None);
            /// # });
            /// ```
            pub fn get_weak(self: &$shared<Self>) -> GetWeak<T>
            where
                T: Clone,
            {
                GetWeak($shared::downgrade(self))
            }
        }

        impl<T> Default for AsyncCell<T> {
            fn default() -> Self {
                AsyncCell::new()
            }
        }

        impl<T: fmt::Debug + Clone> fmt::Debug for AsyncCell<T> {
            fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
                crate::internal::debug_state(&self.cell, f)
            }
        }

        impl<T, C> Future for Take<C>
        where
            C: Deref<Target = AsyncCell<T>>,
        {
            type Output = T;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
                crate::internal::poll_take(&self.0.cell, cx.waker())
            }
        }

        impl<T, C> Future for &Take<C>
        where
            C: Deref<Target = AsyncCell<T>>,
        {
            type Output = T;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
                crate::internal::poll_take(&self.0.cell, cx.waker())
            }
        }

        impl<T> TakeWeak<T> {
            /// Returns a [Take] promise if the cell has not been dropped.
            pub fn upgrade(&self) -> Option<Take<$shared<AsyncCell<T>>>> {
                Some(Take(self.0.upgrade()?))
            }
        }

        impl<T> Future for TakeWeak<T> {
            type Output = Option<T>;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
                if let Some(cell) = self.0.upgrade() {
                    crate::internal::poll_take(&cell.cell, cx.waker()).map(Some)
                } else {
                    Poll::Ready(None)
                }
            }
        }

        impl<T> Future for &TakeWeak<T> {
            type Output = Option<T>;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
                if let Some(cell) = self.0.upgrade() {
                    crate::internal::poll_take(&cell.cell, cx.waker()).map(Some)
                } else {
                    Poll::Ready(None)
                }
            }
        }

        #[cfg(feature = "weakref")]
        impl<T> Future for TakeRef<T> {
            type Output = Option<T>;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
                let guard = &weakref::pin();
                if let Some(cell) = self.0.get(&guard) {
                    crate::internal::poll_take(&cell.cell, cx.waker()).map(Some)
                } else {
                    Poll::Ready(None)
                }
            }
        }

        #[cfg(feature = "weakref")]
        impl<T> Future for &TakeRef<T> {
            type Output = Option<T>;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
                let guard = &weakref::pin();
                if let Some(cell) = self.0.get(&guard) {
                    crate::internal::poll_take(&cell.cell, cx.waker()).map(Some)
                } else {
                    Poll::Ready(None)
                }
            }
        }

        impl<T, C> Future for Get<C>
        where
            C: Deref<Target = AsyncCell<T>>,
            T: Clone,
        {
            type Output = T;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
                crate::internal::poll_get(&self.0.cell, cx.waker())
            }
        }

        impl<T, C> Future for &Get<C>
        where
            C: Deref<Target = AsyncCell<T>>,
            T: Clone,
        {
            type Output = T;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
                crate::internal::poll_get(&self.0.cell, cx.waker())
            }
        }

        impl<T> GetWeak<T> {
            /// Returns a [Get] promise if the cell has not been dropped.
            pub fn upgrade(&self) -> Option<Get<$shared<AsyncCell<T>>>> {
                Some(Get(self.0.upgrade()?))
            }
        }

        impl<T: Clone> Future for GetWeak<T> {
            type Output = Option<T>;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
                if let Some(cell) = self.0.upgrade() {
                    crate::internal::poll_get(&cell.cell, cx.waker()).map(Some)
                } else {
                    Poll::Ready(None)
                }
            }
        }

        impl<T: Clone> Future for &GetWeak<T> {
            type Output = Option<T>;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
                if let Some(cell) = self.0.upgrade() {
                    crate::internal::poll_get(&cell.cell, cx.waker()).map(Some)
                } else {
                    Poll::Ready(None)
                }
            }
        }

        #[cfg(feature = "weakref")]
        impl<T: Clone> Future for GetRef<T> {
            type Output = Option<T>;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
                let guard = &weakref::pin();
                if let Some(cell) = self.0.get(&guard) {
                    crate::internal::poll_get(&cell.cell, cx.waker()).map(Some)
                } else {
                    Poll::Ready(None)
                }
            }
        }

        #[cfg(feature = "weakref")]
        impl<T: Clone> Future for &GetRef<T> {
            type Output = Option<T>;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
                let guard = &weakref::pin();
                if let Some(cell) = self.0.get(&guard) {
                    crate::internal::poll_get(&cell.cell, cx.waker()).map(Some)
                } else {
                    Poll::Ready(None)
                }
            }
        }

        impl<T, C: Deref<Target = AsyncCell<T>>> GuardedCell<T, C> {
            /// Drop this guard without setting the wrapped cell.
            pub fn release(mut self) {
                self.cancel = None;
            }

            /// Like [`AsyncCell::set`](AsyncCell::set) but also releases this guard.
            pub fn set(self, value: T) {
                self.inner.set(value);
                self.release();
            }

            /// Like [`AsyncCell::or_set`](AsyncCell::or_set) but also releases this guard.
            pub fn or_set(self, value: T) {
                self.inner.or_set(value);
                self.release();
            }
        }

        impl<T, C: Deref<Target = AsyncCell<T>>> Deref for GuardedCell<T, C> {
            type Target = AsyncCell<T>;

            fn deref(&self) -> &AsyncCell<T> {
                &*self.inner
            }
        }

        impl<T, C: Deref<Target = AsyncCell<T>>> Drop for GuardedCell<T, C> {
            fn drop(&mut self) {
                if let Some(cancel) = self.cancel.take() {
                    self.inner.or_set(cancel);
                }
            }
        }
    };
}

/// Types which can be shared across threads.
#[cfg(not(feature = "no_std"))]
pub mod sync {
    use std::sync::{Arc, Weak};

    #[cfg(feature = "parking_lot")]
    type Mutex<T> = parking_lot::Mutex<crate::internal::DropState<T>>;

    #[cfg(not(feature = "parking_lot"))]
    type Mutex<T> = std::sync::Mutex<crate::internal::DropState<T>>;

    impl_async_cell_part1!("use async_cell::sync::AsyncCell;", Mutex<T>, T, Arc);

    #[cfg(feature = "parking_lot")]
    impl<T> AsyncCell<T> {
        /// Create an empty AsyncCell.
        pub const fn new() -> Self {
            AsyncCell {
                cell: parking_lot::const_mutex(crate::internal::DropState::empty()),
            }
        }

        /// Create a filled AsyncCell.
        /// ```
        /// # use async_cell::sync::AsyncCell;
        /// let cell = AsyncCell::new_with(42);
        /// assert_eq!(cell.try_get(), Some(42));
        /// ```
        pub const fn new_with(value: T) -> Self {
            AsyncCell {
                cell: parking_lot::const_mutex(crate::internal::DropState::full(value)),
            }
        }
    }

    #[cfg(not(feature = "parking_lot"))]
    impl<T> AsyncCell<T> {
        /// Create an empty AsyncCell.
        pub const fn new() -> Self {
            AsyncCell {
                cell: std::sync::Mutex::new(crate::internal::DropState::empty()),
            }
        }

        /// Create a filled AsyncCell.
        /// ```
        /// # use async_cell::sync::AsyncCell;
        /// let cell = AsyncCell::new_with(42);
        /// assert_eq!(cell.try_get(), Some(42));
        /// ```
        pub const fn new_with(value: T) -> Self {
            AsyncCell {
                cell: std::sync::Mutex::new(crate::internal::DropState::full(value)),
            }
        }
    }

    impl_async_cell_part2!("use async_cell::sync::AsyncCell;", Arc);
}

/// Types for single-threaded and no_std use.
pub mod unsync {
    use alloc::rc::{Rc, Weak};

    impl_async_cell_part1!(
        "use async_cell::unsync::AsyncCell;",
        core::cell::Cell<crate::internal::DropState<T>>,
        T,
        Rc
    );

    impl<T> AsyncCell<T> {
        /// Create an empty AsyncCell.
        pub const fn new() -> Self {
            AsyncCell {
                cell: core::cell::Cell::new(crate::internal::DropState::empty()),
            }
        }

        /// Create a filled AsyncCell.
        /// ```
        /// # use async_cell::unsync::AsyncCell;
        /// let cell = AsyncCell::new_with(42);
        /// assert_eq!(cell.try_get(), Some(42));
        /// ```
        pub const fn new_with(value: T) -> Self {
            AsyncCell {
                cell: core::cell::Cell::new(crate::internal::DropState::full(value)),
            }
        }
    }

    impl_async_cell_part2!("use async_cell::unsync::AsyncCell;", Rc);
}
