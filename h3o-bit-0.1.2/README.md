# h3o-bit — bit twiddling for H3 indexes

[![Crates.io](https://img.shields.io/crates/v/h3o-bit.svg)](https://crates.io/crates/h3o-bit)
[![Docs.rs](https://docs.rs/h3o-bit/badge.svg)](https://docs.rs/h3o-bit)
[![CI Status](https://github.com/HydroniumLabs/h3o-bit/actions/workflows/ci.yml/badge.svg)](https://github.com/HydroniumLabs/h3o-bit/actions)
[![Coverage](https://img.shields.io/codecov/c/github/HydroniumLabs/h3o-bit)](https://app.codecov.io/gh/HydroniumLabs/h3o-bit)
[![License](https://img.shields.io/badge/license-BSD-green)](https://opensource.org/licenses/BSD-3-Clause)

This is a low-level library that assumes that you know what you're doing (there
are no guardrails: garbage-in, garbage-out).

Unless you're implementing algorithms/data structures for H3 indexes, you should
be using [h3o](https://github.com/HydroniumLabs/h3o).

This crate is mostly for sharing low-level primitives between the h3o crates,
thus the API is enriched on a need basis.

## License

[BSD 3-Clause](./LICENSE)
