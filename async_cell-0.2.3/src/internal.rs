//! This module is actually responsible for implementing AsyncCell. It uses
//! traits to share implementation details across different types of locking
//! primitive.

use crate::cons::wake_both;
use core::fmt;
#[allow(unused)]
use core::mem::{replace, take};
use core::{task::Poll, task::Waker};

pub enum State<T> {
    /// No data in the cell.
    Empty,
    /// A future waiting on data to enter the cell.
    Pending(Waker),
    /// The cell contains data.
    Full(T),
}

pub use State::*;

impl<T> Default for State<T> {
    #[inline(always)]
    fn default() -> Self {
        Empty
    }
}

#[repr(transparent)]
pub struct DropState<T> {
    pub state: State<T>,
}

impl<T> DropState<T> {
    pub const fn empty() -> Self {
        DropState { state: Empty }
    }

    pub const fn full(value: T) -> Self {
        DropState { state: Full(value) }
    }
}

impl<T> From<State<T>> for DropState<T> {
    #[inline]
    fn from(state: State<T>) -> Self {
        DropState { state }
    }
}

impl<T> From<DropState<T>> for State<T> {
    #[inline]
    fn from(mut wrapped: DropState<T>) -> Self {
        replace(&mut wrapped.state, Empty)
    }
}

impl<T> Default for DropState<T> {
    #[inline(always)]
    fn default() -> Self {
        Empty.into()
    }
}

impl<T> Drop for DropState<T> {
    fn drop(&mut self) {
        match replace(&mut self.state, Empty) {
            Pending(w) => w.wake(),
            _ => (),
        }
    }
}

pub trait CellInner {
    type Value;

    fn new(value: State<Self::Value>) -> Self;
    fn replace_state(&self, new: State<Self::Value>) -> State<Self::Value>;
    fn update_state<O>(
        &self,
        func: impl FnOnce(State<Self::Value>) -> (O, State<Self::Value>),
    ) -> O;
    fn into_inner(self) -> State<Self::Value>;
}

#[cfg(not(feature = "no_std"))]
impl<T> CellInner for std::sync::Mutex<DropState<T>> {
    type Value = T;

    fn new(value: State<T>) -> Self {
        std::sync::Mutex::new(value.into())
    }

    fn replace_state(&self, new: State<T>) -> State<T> {
        let mut g = match self.lock() {
            Ok(g) => g,
            Err(e) => e.into_inner(),
        };
        replace(&mut g.state, new)
    }

    fn update_state<O>(&self, func: impl FnOnce(State<T>) -> (O, State<T>)) -> O {
        let mut g = match self.lock() {
            Ok(g) => g,
            Err(e) => e.into_inner(),
        };
        let (out, new) = func(take(&mut g.state));
        g.state = new;
        out
    }

    fn into_inner(self) -> State<T> {
        match std::sync::Mutex::into_inner(self) {
            Ok(i) => i,
            Err(e) => e.into_inner(),
        }
        .into()
    }
}

#[cfg(not(feature = "no_std"))]
#[cfg(feature = "parking_lot")]
impl<T> CellInner for parking_lot::Mutex<DropState<T>> {
    type Value = T;

    fn new(value: State<T>) -> Self {
        parking_lot::Mutex::new(value.into())
    }

    fn replace_state(&self, new: State<T>) -> State<T> {
        replace(&mut self.lock().state, new)
    }

    fn update_state<O>(&self, func: impl FnOnce(State<T>) -> (O, State<T>)) -> O {
        let mut g = self.lock();
        let (out, new) = func(take(&mut g.state));
        g.state = new;
        out
    }

    fn into_inner(self) -> State<T> {
        parking_lot::Mutex::into_inner(self).into()
    }
}

impl<T> CellInner for core::cell::Cell<DropState<T>> {
    type Value = T;

    fn new(value: State<T>) -> Self {
        core::cell::Cell::new(value.into())
    }

    fn replace_state(&self, new: State<T>) -> State<T> {
        self.replace(new.into()).into()
    }

    fn update_state<O>(&self, func: impl FnOnce(State<T>) -> (O, State<T>)) -> O {
        let (out, new) = func(self.take().into());
        self.set(new.into());
        out
    }

    fn into_inner(self) -> State<T> {
        core::cell::Cell::into_inner(self).into()
    }
}

pub fn set<C: CellInner>(cell: &C, value: C::Value) -> Option<C::Value> {
    match cell.replace_state(Full(value)) {
        Empty => (),
        Pending(waker) => waker.wake(),
        Full(value) => return Some(value),
    }

    None
}

pub fn or_set<C: CellInner>(cell: &C, value: C::Value) {
    if let Some(waker) = cell.update_state(|s| match s {
        Full(x) => (None, Full(x)),
        Pending(w) => (Some(w), Full(value)),
        Empty => (None, Full(value)),
    }) {
        waker.wake();
    }
}

pub fn into_inner<C: CellInner>(cell: C) -> Option<C::Value> {
    match cell.into_inner() {
        Empty | Pending(_) => None,
        Full(v) => Some(v),
    }
}

pub fn update<C: CellInner>(cell: &C, f: impl FnOnce(Option<C::Value>) -> Option<C::Value>) {
    let f = |x, w| match (f(x), w) {
        (None, None) => (None, Empty),
        (None, Some(w)) => (None, Pending(w)),
        (Some(y), None) => (None, Full(y)),
        (Some(y), Some(w)) => (Some(w), Full(y)),
    };

    if let Some(waker) = cell.update_state(|s| match s {
        Full(x) => f(Some(x), None),
        Pending(w) => f(None, Some(w)),
        Empty => f(None, None),
    }) {
        waker.wake();
    }
}

pub fn is_set<C: CellInner>(cell: &C) -> bool {
    cell.update_state(|s| (matches!(s, Full(_)), s))
}

pub fn poll_take<C: CellInner>(cell: &C, waker: &Waker) -> Poll<C::Value> {
    cell.update_state(|s| {
        let waker = match s {
            Full(data) => return (Poll::Ready(data), Empty),
            Pending(old) if old.will_wake(waker) => old,
            Pending(old) => wake_both(old, waker.clone()),
            Empty => waker.clone(),
        };

        (Poll::Pending, Pending(waker))
    })
}

pub fn poll_get<C: CellInner>(cell: &C, waker: &Waker) -> Poll<C::Value>
where
    C::Value: Clone,
{
    cell.update_state(|s| {
        let waker = match s {
            Full(data) => return (Poll::Ready(data.clone()), Full(data)),
            Pending(old) if old.will_wake(waker) => old,
            Pending(old) => wake_both(old, waker.clone()),
            Empty => waker.clone(),
        };

        (Poll::Pending, Pending(waker))
    })
}

pub fn try_take<C: CellInner>(cell: &C) -> Option<C::Value> {
    if let Full(value) = cell.replace_state(State::Empty) {
        Some(value)
    } else {
        None
    }
}

pub fn try_get<C: CellInner>(cell: &C) -> Option<C::Value>
where
    C::Value: Clone,
{
    cell.update_state(|s| {
        if let Full(data) = s {
            (Some(data.clone()), Full(data))
        } else {
            (None, s)
        }
    })
}

pub fn debug_state<C: CellInner>(cell: &C, fmt: &mut fmt::Formatter) -> Result<(), fmt::Error>
where
    C::Value: Clone + fmt::Debug,
{
    let (name, value) = cell.update_state(|s| {
        (
            match &s {
                Empty => ("AsyncCell::Empty", None),
                Pending(_) => ("AsyncCell::Pending", None),
                Full(x) => ("AsyncCell::Full", Some(x.clone())),
            },
            s,
        )
    });

    if let Some(value) = value {
        fmt.debug_tuple(name).field(&value).finish()
    } else {
        fmt.write_str(name)
    }
}
