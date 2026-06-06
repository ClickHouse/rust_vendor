#![no_std]
//! The `h3o-bit` library provides bit twiddling routines for H3 indexes.
//!
//! This is a low-level library that assumes that you know what you're doing
//! (there are no guardrails: garbage-in, garbage-out).
//!
//! Unless you're implementing algorithms/data structures for H3 indexes, you
//! should be using [h3o](https://github.com/HydroniumLabs/h3o).
//!
//! This crate is mostly for sharing low-level primitives between the h3o
//! crates, thus the API is enriched on a need basis.

// Lints {{{

#![deny(
    nonstandard_style,
    rust_2018_idioms,
    rust_2021_compatibility,
    future_incompatible,
    rustdoc::all,
    rustdoc::missing_crate_level_docs,
    missing_docs,
    unsafe_code,
    unused,
    unused_import_braces,
    unused_lifetimes,
    unused_qualifications,
    variant_size_differences,
    warnings,
    clippy::all,
    clippy::cargo,
    clippy::pedantic,
    clippy::allow_attributes_without_reason,
    clippy::as_underscore,
    clippy::branches_sharing_code,
    clippy::clone_on_ref_ptr,
    clippy::cognitive_complexity,
    clippy::create_dir,
    clippy::dbg_macro,
    clippy::debug_assert_with_mut_call,
    clippy::decimal_literal_representation,
    clippy::default_union_representation,
    clippy::derive_partial_eq_without_eq,
    clippy::empty_drop,
    clippy::empty_line_after_outer_attr,
    clippy::empty_structs_with_brackets,
    clippy::equatable_if_let,
    clippy::exhaustive_enums,
    clippy::exit,
    clippy::filetype_is_file,
    clippy::float_cmp_const,
    clippy::fn_to_numeric_cast_any,
    clippy::format_push_string,
    clippy::future_not_send,
    clippy::get_unwrap,
    clippy::if_then_some_else_none,
    clippy::imprecise_flops,
    clippy::iter_on_empty_collections,
    clippy::iter_on_single_items,
    clippy::iter_with_drain,
    clippy::large_include_file,
    clippy::let_underscore_must_use,
    clippy::lossy_float_literal,
    clippy::mem_forget,
    clippy::missing_const_for_fn,
    clippy::missing_inline_in_public_items,
    clippy::mixed_read_write_in_expression,
    clippy::multiple_inherent_impl,
    clippy::mutex_atomic,
    clippy::mutex_integer,
    clippy::needless_collect,
    clippy::non_send_fields_in_send_ty,
    clippy::nonstandard_macro_braces,
    clippy::option_if_let_else,
    clippy::or_fun_call,
    clippy::panic,
    clippy::path_buf_push_overwrite,
    clippy::pattern_type_mismatch,
    clippy::print_stderr,
    clippy::print_stdout,
    clippy::rc_buffer,
    clippy::rc_mutex,
    clippy::redundant_pub_crate,
    clippy::rest_pat_in_fully_bound_structs,
    clippy::same_name_method,
    clippy::self_named_module_files,
    clippy::significant_drop_in_scrutinee,
    clippy::str_to_string,
    clippy::string_add,
    clippy::string_lit_as_bytes,
    clippy::string_slice,
    clippy::string_to_string,
    clippy::suboptimal_flops,
    clippy::suspicious_operation_groupings,
    clippy::todo,
    clippy::trailing_empty_array,
    clippy::trait_duplication_in_bounds,
    clippy::transmute_undefined_repr,
    clippy::trivial_regex,
    clippy::try_err,
    clippy::type_repetition_in_bounds,
    clippy::undocumented_unsafe_blocks,
    clippy::unimplemented,
    clippy::unnecessary_self_imports,
    clippy::unneeded_field_pattern,
    clippy::unseparated_literal_suffix,
    clippy::unused_peekable,
    clippy::unused_rounding,
    clippy::unwrap_used,
    clippy::use_debug,
    clippy::use_self,
    clippy::useless_let_if_seq,
    clippy::verbose_file_reads
)]
#![allow(
    // The 90’s called and wanted their charset back.
    clippy::non_ascii_literal,
    // "It requires the user to type the module name twice."
    // => not true here since internal modules are hidden from the users.
    clippy::module_name_repetitions,
    // Usually yes, but not really applicable for most literals in this crate.
    clippy::unreadable_literal,
    reason = "allow some exceptions"
)]

// }}}

use core::debug_assert;

/// Default cell index (resolution 0, base cell 0).
pub const DEFAULT_CELL_INDEX: u64 = 0x8001fffffffffff;

/// Size, in bits, of a direction (range [0; 6].
pub const DIRECTION_BITSIZE: usize = 3;

/// Maximum supported H3 resolution.
pub const MAX_RESOLUTION: u8 = 15;

/// The bit offset of the resolution in an H3 index.
const RESOLUTION_OFFSET: u64 = 52;
/// Bitmask to select the resolution bits in an H3 index.
const RESOLUTION_MASK: u64 = 0b1111 << RESOLUTION_OFFSET;

/// Offset (in bits) of the base cell in an H3 index.
const BASE_CELL_OFFSET: u64 = 45;
// Bitmask to select the base cell bits in an H3 index.
const BASE_CELL_MASK: u64 = 0b111_1111 << BASE_CELL_OFFSET;

// ------------------------------------------------------------------------------

/// Returns the H3 index resolution.
#[must_use]
#[inline]
pub const fn get_resolution(bits: u64) -> u8 {
    ((bits & RESOLUTION_MASK) >> RESOLUTION_OFFSET) as u8
}

/// Clears the H3 index resolution bits.
#[must_use]
#[inline]
pub const fn clr_resolution(bits: u64) -> u64 {
    bits & !RESOLUTION_MASK
}

/// Sets the H3 index resolution bits.
#[must_use]
#[inline]
pub const fn set_resolution(bits: u64, resolution: u8) -> u64 {
    clr_resolution(bits) | ((resolution as u64) << RESOLUTION_OFFSET)
}

// ------------------------------------------------------------------------------

/// Returns the H3 index base cell bits.
#[must_use]
#[inline]
pub const fn get_base_cell(bits: u64) -> u8 {
    ((bits & BASE_CELL_MASK) >> BASE_CELL_OFFSET) as u8
}

/// Sets the H3 index base cell bits.
#[must_use]
#[inline]
pub const fn set_base_cell(bits: u64, cell: u8) -> u64 {
    (bits & !BASE_CELL_MASK) | ((cell as u64) << BASE_CELL_OFFSET)
}

// ------------------------------------------------------------------------------

/// Returns the H3 index direction bits at the given resolution.
#[allow(clippy::cast_possible_truncation, reason = "Cast safe thx to masking.")]
#[must_use]
#[inline]
pub const fn get_direction(bits: u64, resolution: u8) -> u8 {
    ((bits & direction_mask(resolution)) >> direction_offset(resolution)) as u8
}

/// Clears the H3 index direction at the given resolution.
#[must_use]
#[inline]
pub const fn clr_direction(bits: u64, resolution: u8) -> u64 {
    bits & !direction_mask(resolution)
}

/// Set the H3 index direction bits at the given resolution.
#[must_use]
#[inline]
pub const fn set_direction(bits: u64, resolution: u8, direction: u8) -> u64 {
    (bits & !direction_mask(resolution))
        | ((direction as u64) << direction_offset(resolution))
}

// ------------------------------------------------------------------------------

/// Returns the bit mask of the direction at this resolution in an H3 index.
#[must_use]
#[inline]
pub const fn direction_mask(resolution: u8) -> u64 {
    debug_assert!(resolution != 0, "res 0 means no directions");
    0b111 << direction_offset(resolution)
}

/// Returns the bit offset of the direction at this resolution in an H3 index.
#[must_use]
#[inline]
pub const fn direction_offset(resolution: u8) -> usize {
    (MAX_RESOLUTION - resolution) as usize * DIRECTION_BITSIZE
}
