#![allow(unused)]

use async_cell::sync::AsyncCell;
use std::task::{Context, Poll, Wake, Waker};
use std::{future::Future, pin::Pin, sync::Arc};

struct LoomWaker(loom::sync::Notify);

impl Wake for LoomWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.notify();
    }
}

pub fn spawn(future: impl Future<Output = ()> + 'static) {
    loom::thread::spawn(|| block_on(future));
}

pub fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    let notify = Arc::new(LoomWaker(loom::sync::Notify::new()));
    let waker: Waker = notify.clone().into();
    loop {
        let mut cx = Context::from_waker(&waker);
        if let Poll::Ready(out) = future.as_mut().poll(&mut cx) {
            return out;
        }
        notify.0.wait();
    }
}

pub fn root<F: Future>(future: impl Fn() -> F + Sync + Send + 'static) {
    loom::model(move || {
        block_on(future());
    });
}

struct NotifyOnWake(Arc<AsyncCell>);

impl Wake for NotifyOnWake {
    fn wake(self: Arc<Self>) {
        self.0.notify();
    }
}

pub fn tick_future(future: impl Future, on_wake: Arc<AsyncCell>) {
    let mut future = Box::pin(future);
    tick_future_ref(future.as_mut(), on_wake);
}

#[allow(unused)]
pub fn tick_future_ref(future: Pin<&mut impl Future>, on_wake: Arc<AsyncCell>) {
    let waker = Arc::new(NotifyOnWake(on_wake)).into();
    let mut cx = Context::from_waker(&waker);
    let _ = future.poll(&mut cx);
}

pub struct Unordered<F: Future + Unpin>(pub Vec<F>);

impl<F: Future + Unpin> Unordered<F> {
    pub async fn next(&mut self) -> F::Output {
        Select(&mut self.0).await
    }
}

struct Select<'a, F: Future + Unpin>(&'a mut Vec<F>);

impl<'a, F: Future + Unpin> Future for Select<'a, F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        for i in 0..self.0.len() {
            if let Poll::Ready(x) = Pin::new(&mut self.0[i]).poll(cx) {
                self.0.remove(i);
                return Poll::Ready(x);
            }
        }
        Poll::Pending
    }
}
