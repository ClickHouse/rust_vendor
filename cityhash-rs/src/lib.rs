#![no_std]
#![allow(clippy::many_single_char_names)]

#[cfg(test)]
#[macro_use]
extern crate std;

#[cfg(test)]
mod test_102;
#[cfg(test)]
mod test_103;
#[cfg(test)]
mod test_110;

mod base;

mod v102;
mod v103;
mod v110;
pub use v102::cityhash_102_128;
pub use v103::cityhash_103_128;
pub use v110::cityhash_110_128;
