test_normalize! {"
error[E0658]: `#[used(linker)]` is currently unstable
 --> tests/ui/used-linker.rs:6:5
  |
6 |     #![used(linker)]
  |     ^^^^^^^^^^^^^^^^
  |
  = note: see issue #93798 <https://github.com/rust-lang/rust/issues/93798> for more information
  = help: add `#![feature(used_with_arg)]` to the crate attributes to enable
  = note: this compiler was built on 2024-01-13; consider upgrading it if it is out of date
" "
error[E0658]: `#[used(linker)]` is currently unstable
 --> tests/ui/used-linker.rs:6:5
  |
6 |     #![used(linker)]
  |     ^^^^^^^^^^^^^^^^
  |
  = note: see issue #93798 <https://github.com/rust-lang/rust/issues/93798> for more information
  = help: add `#![feature(used_with_arg)]` to the crate attributes to enable
"}
