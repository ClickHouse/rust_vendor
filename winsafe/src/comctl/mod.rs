#![cfg(feature = "comctl")]

mod aliases;
mod enums;
mod funcs;
mod handles;
mod structs;

pub(in crate::comctl) mod ffi;
pub(crate) mod privs;
pub mod co;
pub mod guard;
pub mod messages;

pub mod decl {
	pub use super::aliases::*;
	pub use super::enums::*;
	pub use super::handles::decl::*;
	pub use super::funcs::*;
	pub use super::structs::*;
}

pub mod traits {
	pub use super::handles::traits::*;
}
