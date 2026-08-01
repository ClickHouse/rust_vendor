// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! The wide (decode-optimized) dictionary representation.
//!
//! [`WideDictionary`] stores `num_tokens` rows of [`MAX_TOKEN_SIZE`] bytes — row
//! `id` holds token `id` (zero-padded) — plus a per-token length. Trades space
//! for a **load-free** token address: token `id`'s bytes are at `data + id*16`,
//! with no `code → offset → bytes` indirection, so a decode is one independent
//! load.
//!
//! Build it from the compact form with
//! [`CompactDictionaryView::to_wide`](super::CompactDictionaryView::to_wide). The
//! compact form is the serialized one; the wide form exists only to accelerate
//! decoding, so the conversion is one-way. Both representations implement
//! [`DictionaryView`] (via their views), so the decode kernels treat them
//! uniformly.
//!
//! # Invariants
//! Upheld by [`CompactDictionaryView::to_wide`](super::CompactDictionaryView::to_wide),
//! the wide form's only producer: it is built from a **trusted** compact
//! dictionary and is therefore valid by construction. It is never deserialized
//! (only the compact form is), so there is no untrusted wide type. The logical
//! invariants match the compact form's — only the physical sizing clause differs
//! (the wide form needs no read-padding).
//! - `lens.len() == num_tokens` and `data.len() == num_tokens * MAX_TOKEN_SIZE`.
//! - **Bounded lengths** — every `lens[id]` is in `1..=MAX_TOKEN_SIZE`, so every
//!   token is non-empty; row `id`'s first `lens[id]` bytes are token `id`, and the
//!   rest of the row is unused.
//! - **Sorted** — tokens are in strictly ascending bytewise-lexicographic order.
//! - **Complete** — all 256 single-byte tokens are present, so any byte string
//!   is encodable.
//! - **Unique** — no two tokens are equal.

use super::{Dictionary, DictionaryView};
use crate::core::types::{MAX_TOKEN_SIZE, Token};

/// Owned wide dictionary: `num_tokens` rows of [`MAX_TOKEN_SIZE`] bytes plus
/// per-token lengths.
///
/// **Trusted** and crate-private, like [`CompactDictionary`](super::CompactDictionary): the only way to get
/// one is [`CompactDictionaryView::to_wide`](super::CompactDictionaryView::to_wide)
/// from a trusted compact dictionary, so it is valid by construction. There is no
/// untrusted wide type — the wide form is never deserialized.
#[derive(Default, Debug, Clone)]
pub struct WideDictionary {
    /// `num_tokens * MAX_TOKEN_SIZE` row-major token bytes; row `id` holds token
    /// `id`, zero-padded to the row width.
    data: Vec<u8>,
    /// `num_tokens` true token lengths.
    lens: Vec<u8>,
}

impl WideDictionary {
    /// Number of tokens.
    #[inline]
    pub fn num_tokens(&self) -> usize {
        self.lens.len()
    }

    /// Seal row-major `data` and `lens` into a wide dictionary. The single
    /// construction door, called by [`CompactDictionaryView::to_wide`](super::CompactDictionaryView::to_wide),
    /// which guarantees the invariants.
    #[inline]
    pub(crate) fn from_raw(data: Vec<u8>, lens: Vec<u8>) -> Self {
        Self { data, lens }
    }
}

impl Dictionary for WideDictionary {
    type View<'a> = WideDictionaryView<'a>;
    #[inline]
    fn as_view(&self) -> WideDictionaryView<'_> {
        WideDictionaryView {
            data: &self.data,
            lens: &self.lens,
        }
    }
}

/// Borrowed, `Copy`, **trusted** view over a wide dictionary's buffers.
///
/// Private fields: obtained only from a trusted [`WideDictionary`] via
/// [`Dictionary::as_view`].
#[derive(Copy, Clone, Debug)]
pub struct WideDictionaryView<'a> {
    /// `num_tokens * MAX_TOKEN_SIZE` row-major token bytes.
    data: &'a [u8],
    /// `num_tokens` true token lengths.
    lens: &'a [u8],
}

impl DictionaryView for WideDictionaryView<'_> {
    #[inline]
    fn num_tokens(&self) -> usize {
        self.lens.len()
    }

    #[inline]
    fn token(&self, id: Token) -> &[u8] {
        let row = id as usize * MAX_TOKEN_SIZE;
        &self.data[row..row + self.lens[id as usize] as usize]
    }

    #[inline]
    fn token_len(&self, id: Token) -> usize {
        self.lens[id as usize] as usize
    }

    #[inline]
    unsafe fn token_ptr(&self, id: Token) -> *const u8 {
        // SAFETY: id < num_tokens ⇒ row id is within data; the last row ends
        // exactly at data.len() (= n*16), so 16 bytes are readable. No load.
        unsafe { self.data.as_ptr().add(id as usize * MAX_TOKEN_SIZE) }
    }

    #[inline]
    unsafe fn token_len_unchecked(&self, id: Token) -> usize {
        // SAFETY: id < num_tokens ⇒ lens[id] is in bounds.
        unsafe { *self.lens.get_unchecked(id as usize) as usize }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::dictionary::{CompactDictionary, pad_raw};

    fn padded_compact(tokens: &[&[u8]]) -> CompactDictionary {
        let mut bytes = Vec::new();
        let mut offsets = vec![0u32];
        for t in tokens {
            bytes.extend_from_slice(t);
            offsets.push(bytes.len() as u32);
        }
        pad_raw(&mut bytes, &offsets);
        CompactDictionary::from_raw(bytes, offsets)
    }

    #[test]
    fn num_tokens_counts_rows() {
        let wide = padded_compact(&[b"a", b"bc", b"def"]).to_wide();
        assert_eq!(wide.num_tokens(), 3);
        assert_eq!(wide.as_view().num_tokens(), 3);
    }

    #[test]
    fn to_wide_rows_and_lens_match_tokens() {
        // `to_wide` lays each token in its own zero-padded MAX_TOKEN_SIZE row.
        let tokens: &[&[u8]] = &[b"a", b"bc", b"def", b"ghij"];
        let wide = padded_compact(tokens).to_wide();
        assert_eq!(wide.num_tokens(), tokens.len());
        for (id, tok) in tokens.iter().enumerate() {
            assert_eq!(wide.lens[id] as usize, tok.len());
            assert_eq!(
                &wide.data[id * MAX_TOKEN_SIZE..id * MAX_TOKEN_SIZE + tok.len()],
                *tok
            );
        }
    }
}
