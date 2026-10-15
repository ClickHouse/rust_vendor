// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! The one vocabulary for "these compressed buffers are malformed", plus the
//! panic helper that surfaces it.
//!
//! A [`ColumnView`](crate::ColumnView) / dictionary view is built from buffers a
//! consumer deserialized from storage, so its arrays may be corrupt.
//! [`InvalidColumn`] enumerates the structural violations that would otherwise
//! make a decoder read or write out of bounds. It is surfaced two ways:
//!
//! * **Recoverable** — the `validate` family returns a `Result` for buffers a
//!   consumer deserialized from storage. Two distinct roles:
//!   * [`CompactDictionary::validate`](crate::CompactDictionary::validate) is the
//!     *trust boundary*: it seals raw `(bytes, offsets)` into a trusted dictionary,
//!     which is what lets the decoder read tokens unchecked. Validate once, then
//!     decode on the fast (unchecked) path.
//!   * [`ColumnView::validate`](crate::ColumnView::validate) is a *pre-flight*, not
//!     a fast-path gate: the decode kernels bounds-check every code and row offset
//!     regardless, so it unlocks nothing faster. It just reports the same
//!     violations up front, as a `Result`, instead of as a mid-decode panic.
//! * **Infallible** — operations that are infallible by signature (the convenience
//!   decoders, the per-row/per-code guards) panic on malformed data through
//!   [`panic_malformed`], with a message derived from `InvalidColumn`'s `Display`.
//!
//! Bad *arguments* to the fallible encoding API (`compress`/`train`/`parse`) are a
//! separate domain ([`Error`](crate::Error)).

/// A violation found while validating compressed buffers.
///
/// Two kinds. **Safety** violations would let an unchecked decoder read or write
/// out of bounds — these are exactly the obligations an `unsafe new_unchecked`
/// caller must uphold to avoid UB. **Conformance** violations, including a
/// dictionary too large for the code address space, decode safely but make search /
/// tokenize give *wrong answers*. The `validate` family checks both, so a trusted
/// dictionary is fully conformant — indistinguishable from a trainer-built one.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum InvalidColumn {
    // ── Safety / addressability ──────────────────────────────────────────────
    /// Dictionary offsets decrease (`offsets[i] > offsets[i + 1]`), which would
    /// underflow the unchecked token-length subtraction.
    NonDecreasingOffsets,
    /// A dictionary token is longer than [`MAX_TOKEN_SIZE`](crate::MAX_TOKEN_SIZE).
    TokenTooLarge,
    /// A token offset has fewer than [`MAX_TOKEN_SIZE`](crate::MAX_TOKEN_SIZE)
    /// readable bytes after it, so the decoder's fixed-width read runs off the end.
    MissingPadding,
    /// The dictionary has more than `2^16` entries, or a code does not index the
    /// dictionary (`code >= num_tokens`). In either case, the `u16` token/code
    /// type cannot address the requested entry.
    CodeOutOfRange,
    /// Row offsets are not non-decreasing, or the last exceeds the code count.
    BadRowOffsets,
    /// The column's tokens sum to more than `usize::MAX` decoded bytes, so the
    /// decoded-length computation overflows and would under-size the output buffer.
    DecodedLenOverflow,
    // ── Conformance: decodes safely, but search / tokenize give wrong answers ──
    /// A dictionary token has zero length (offsets not strictly increasing).
    EmptyToken,
    /// Dictionary tokens are not in strictly ascending bytewise order, so they are
    /// not sorted (binary search breaks) or not unique.
    UnsortedTokens,
    /// The dictionary lacks one or more of the 256 single-byte tokens, so some
    /// inputs are not encodable.
    IncompleteAlphabet,
}

impl std::fmt::Display for InvalidColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::NonDecreasingOffsets => "dictionary offsets must be non-decreasing",
            Self::TokenTooLarge => "dictionary token exceeds MAX_TOKEN_SIZE",
            Self::MissingPadding => "dictionary bytes lack the required trailing decoder padding",
            Self::CodeOutOfRange => "code index out of range for dictionary",
            Self::BadRowOffsets => "row offsets must be non-decreasing and within the code stream",
            Self::DecodedLenOverflow => "column decodes to more than usize::MAX bytes",
            Self::EmptyToken => "dictionary contains an empty token",
            Self::UnsortedTokens => "dictionary tokens must be sorted and unique",
            Self::IncompleteAlphabet => "dictionary is missing one or more single-byte tokens",
        })
    }
}

impl std::error::Error for InvalidColumn {}

/// Panic for a malformed column/dictionary, message derived from
/// `InvalidColumn`'s `Display`. `#[cold]` + `#[inline(never)]` so a caller's
/// guard is laid out as a never-taken branch.
#[cold]
#[inline(never)]
pub(crate) fn panic_malformed(e: InvalidColumn) -> ! {
    panic!("onpair: {e}")
}
