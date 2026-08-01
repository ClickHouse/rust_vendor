// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! The dictionary: the token table a code stream indexes into.
//!
//! A dictionary is a code-addressable token vocabulary in one of two physical
//! representations of the same data:
//!
//! * [`CompactDictionary`] / [`CompactDictionaryView`] ([`compact`]) — Arrow
//!   binary: a flat `bytes` buffer plus an `offsets` index.
//! * [`WideDictionary`] / [`WideDictionaryView`] ([`wide`]) — a
//!   `num_tokens × MAX_TOKEN_SIZE` byte-strided copy.
//!
//! Both borrowed views implement [`DictionaryView`], the layout-agnostic
//! token-read interface; the owned forms implement [`Dictionary`], which lends
//! the matching view. The compact form is the serialized one; accelerate decoding
//! by materializing the wide form with [`CompactDictionaryView::to_wide`].
//!
//! # Trust
//! The four trusted types above are crate-private in their fields and implement a
//! **sealed** [`DictionaryView`], so external code cannot forge one. Raw
//! deserialized buffers cross into the trusted forms only through the
//! [`validate`](CompactDictionary::validate) (checked) and
//! [`new_unchecked`](CompactDictionary::new_unchecked) (`unsafe` backdoor)
//! constructors on the compact types.

mod compact;
mod wide;

pub(crate) use compact::pad_raw;
pub use compact::{CompactDictionary, CompactDictionaryView, code_bits_for_num_tokens};
pub use wide::{WideDictionary, WideDictionaryView};

use crate::core::types::Token;

/// Sealing for [`DictionaryView`] — implemented only by the crate's trusted view
/// types, so `DictionaryView` cannot be implemented downstream.
mod sealed {
    pub trait Sealed {}
    impl Sealed for super::CompactDictionaryView<'_> {}
    impl Sealed for super::WideDictionaryView<'_> {}
}

/// An owned dictionary that can lend its borrowed [`DictionaryView`].
///
/// Implemented by both representations ([`CompactDictionary`] and
/// [`WideDictionary`]), so generic code can accept any owned dictionary and
/// obtain a view.
pub trait Dictionary {
    /// The borrowed view this dictionary lends.
    type View<'a>: DictionaryView
    where
        Self: 'a;

    /// Borrow as a [`DictionaryView`].
    fn as_view(&self) -> Self::View<'_>;
}

/// A borrowed **trusted** dictionary's token-read interface, abstracted over the
/// layout ([`CompactDictionaryView`] or [`WideDictionaryView`]).
///
/// **Sealed:** implemented only by this crate's trusted view types, so a value of
/// `V: DictionaryView` is by construction a validated dictionary — the unchecked
/// accessors below rely on that. Untrusted buffers reach a view through
/// [`CompactDictionaryView::validate`], not by implementing this trait.
pub trait DictionaryView: Copy + sealed::Sealed {
    /// Number of tokens in the dictionary. The valid token ids are
    /// `0..num_tokens()`.
    fn num_tokens(&self) -> usize;

    /// Bytes of token `id`. Bounds-checked; panics if `id` is out of range.
    fn token(&self, id: Token) -> &[u8];

    /// Byte length of token `id`. Bounds-checked; panics if `id` is out of range.
    fn token_len(&self, id: Token) -> usize;

    /// Raw pointer to token `id`'s bytes.
    ///
    /// # Safety
    /// `id` is a valid code (less than the number of tokens), and the pointer
    /// must be valid for [`MAX_TOKEN_SIZE`](crate::MAX_TOKEN_SIZE) readable bytes
    /// — the fast decode path over-reads a fixed 16-byte chunk regardless of the
    /// true length.
    unsafe fn token_ptr(&self, id: Token) -> *const u8;

    /// Token `id`'s length — the unchecked counterpart of
    /// [`token_len`](Self::token_len).
    ///
    /// # Safety
    /// `id` is a valid code (less than the number of tokens).
    unsafe fn token_len_unchecked(&self, id: Token) -> usize;
}
