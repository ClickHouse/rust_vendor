# async_cell

[![Build Status](https://gitlab.com/samsartor/async_cell/badges/master/pipeline.svg)](https://gitlab.com/samsartor/async_cell/commits/master)
[![Latest Version](https://img.shields.io/crates/v/async_cell.svg)](https://crates.io/crates/async_cell)
[![Documentation](https://img.shields.io/badge/docs.rs-async__cell-66c2a5)](https://docs.rs/async_cell)
---

The key type of this crate is [`AsyncCell`](sync::AsyncCell) which can be
found in both thread-safe and single-threaded variants. It is intended as a
useful async primitive which can replace more expensive channels in a fair
number of cases.

> `AsyncCell<T>` behaves a lot like a `Cell<Option<T>>` that you can await
on.

This is used to create futures from a callbacks:
```rust
use async_cell::sync::AsyncCell;

let cell = AsyncCell::shared();
let future = cell.take_shared();

thread::spawn(move || cell.set("Hello, World!"));

println!("{}", future.await);
```

You can also use an async_cell for static variable initialization, wherever
the blocking behavior of a `OnceCell` is unacceptable:
```rust
use async_cell::sync::AsyncCell;

// AsyncCell::new() is const!
static GREETING: AsyncCell<String> = AsyncCell::new();

// Read the file on a background thread,
// setting a placeholder value if the thread panics.
thread::spawn(|| {
    let greeting = GREETING.guard("ERROR".to_string());
    let hello = std::fs::read_to_string("tests/hello.txt").unwrap();
    greeting.set(hello);
});

// Do some work while waiting for the file.

// And greet the user!
println!("{}", &GREETING.get().await);
```

Async cells can also be used to react to the latest value of a variable,
since the same cell can be reused as many times as desired. This is one
way `AsyncCell` differs from a one-shot channel:
```rust
use async_cell::sync::AsyncCell;

// Allocate space for our timer.
let timer = AsyncCell::<i32>::shared();

// Try to print out the time as fast as it updates.
// Some ticks will be skipped if this loop runs too slowly!
let watcher = timer.take_weak();
spawn(async move {
    while let Some(time) = (&watcher).await {
        println!("Launch in T-{} ticks!", time);
    }
});

// Begin counting down!
for i in (0..60).rev() {
    timer.set(i);
}
```

Although this crate contains a number of utility functions, you can often
make due with just [`AsyncCell::new`](sync::AsyncCell::new),
[`AsyncCell::set`](sync::AsyncCell::set), and
[`AsyncCell::take`](sync::AsyncCell::take).

### Limitations

Cells are not channels! Channels will queue all sent values until a receiver
can process them. Readers of a cell will only ever see the most recently
written value. As an example, imagine a GUI with a text box. An `AsyncCell`
would be perfect to watch the text content of the box, since it is not
necessary to send the whole thing on every keystroke. But the keystrokes
themselves must be sent to the box via a channel to avoid any getting lost.

Also avoid using `AsyncCell` in situations with high contention. Cells block
momentarily while cloning values, allocating async callbacks, etc.
As a rule of thumb, try to fill cells from one thread or task and empty from one other.
_Although multiple futures can wait on the same cell, that case is not highly optimized._
