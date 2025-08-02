# `MultiStash`

A vector-like data structure that is able to reuse slots for new elements.

Specifically allows for (armortized) O(1) instructions for:

- `MultiStash::put`
- `MultiStash::take_one`
- `MultiStash::take_all`
- `MultiStash::get`
- `MultiStash::get_mut`

## License

`multi-stash` is primarily distributed under the terms of both the MIT
license and the APACHE license (Version 2.0), at your choice.

See `LICENSE-APACHE` and `LICENSE-MIT` for details.
