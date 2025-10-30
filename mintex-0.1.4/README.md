# mintex
A minimal Mutex

A drop-in for `std::sync::Mutex` which doesn't do poisoning. The lock will spin and yield if the lock isn't acquired. The implementation doesn't attempt to be "fair", but because it's so simple it is fast.

The main reason for it to exist, is because I know it does not allocate memory during execution and that's a desirable feature for some use cases.

I have run the tests under [miri](https://github.com/rust-lang/miri) and no issues are detected.

[![Crates.io](https://img.shields.io/crates/v/mintex.svg)](https://crates.io/crates/mintex)

[API Docs](https://docs.rs/mintex/latest/mintex)

## Installation

```toml
[dependencies]
mintex = "0.1"
```

## License

Apache 2.0 licensed. See LICENSE for details.
