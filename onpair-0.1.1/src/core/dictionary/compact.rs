// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! The compact dictionary: Arrow binary `bytes` + `offsets`.
//!
//! Layout is Arrow binary — a flat `bytes` buffer plus an `offsets` index of
//! length `num_tokens + 1`; token `i` is `bytes[offsets[i]..offsets[i + 1]]`.
//!
//! # Invariants
//! Upheld by the trainer; a precondition of every accessor and of decoding.
//! - `offsets[0] == 0` and `offsets.len() == num_tokens + 1`.
//! - **Strictly increasing** offsets — every token is non-empty, with length in
//!   `1..=MAX_TOKEN_SIZE`.
//! - **Sorted** — tokens are in strictly ascending bytewise-lexicographic order.
//! - **Complete** — all 256 single-byte tokens are present, so any byte string
//!   is encodable.
//! - **Unique** — no two tokens are equal.
//! - **Addressable** — at most `2^16` tokens, so every token id fits in
//!   [`Token`].
//! - **Read-padded** — `bytes` is readable for [`MAX_TOKEN_SIZE`] bytes past the
//!   highest token offset (applied by [`pad_raw`] at construction).
//!   `offsets.last()` is the logical length; `bytes.len()` may exceed it by the
//!   padding.

use super::{Dictionary, DictionaryView, WideDictionary};
use crate::core::types::{MAX_TOKEN_SIZE, Token};
use crate::core::validate::InvalidColumn;

/// Maximum number of dictionary entries addressable by a [`Token`].
const MAX_NUM_TOKENS: usize = Token::MAX as usize + 1;

/// Append `MAX_TOKEN_SIZE - len(last token)` zero bytes to `bytes` so the decoder's
/// fixed-width read from any token offset stays in bounds — the read-padding
/// invariant. Applied once, on the raw buffers, just before sealing a
/// [`CompactDictionary`]. Idempotent: a no-op once the padding is present or when
/// the last token is already `MAX_TOKEN_SIZE` wide.
pub(crate) fn pad_raw(bytes: &mut Vec<u8>, offsets: &[u32]) {
    if offsets.len() < 2 {
        return;
    }
    let last_token_start = offsets[offsets.len() - 2] as usize;
    let required = last_token_start + MAX_TOKEN_SIZE;
    if bytes.len() < required {
        bytes.resize(required, 0);
    }
}

/// Validate raw compact `(bytes, offsets)` against the [`CompactDictionary`]
/// invariants: both safety (no out-of-bounds decode) and conformance (correct
/// search / tokenize). `O(total token bytes)` — the token bytes are read once.
///
/// Two passes: the first checks the offset-only safety invariants and proves every
/// token's bytes are in bounds; the second reads those bytes to check sortedness
/// and alphabet completeness.
fn validate_compact(bytes: &[u8], offsets: &[u32]) -> Result<(), InvalidColumn> {
    if offsets.len().saturating_sub(1) > MAX_NUM_TOKENS {
        return Err(InvalidColumn::CodeOutOfRange);
    }

    // Pass 1 — safety: strictly-increasing offsets (so the length subtraction can't
    // underflow and no token is empty), bounded length, and the trailing
    // read-padding. `s` of the final window is the highest token offset, so a
    // single padding bound covers every token's fixed-width over-read. After this,
    // every `bytes[offsets[i]..offsets[i + 1]]` slice is in bounds.
    let mut last_offset = 0usize;
    for w in offsets.windows(2) {
        let (s, e) = (w[0], w[1]);
        if e < s {
            return Err(InvalidColumn::NonDecreasingOffsets);
        }
        if e == s {
            return Err(InvalidColumn::EmptyToken);
        }
        if (e - s) as usize > MAX_TOKEN_SIZE {
            return Err(InvalidColumn::TokenTooLarge);
        }
        last_offset = s as usize;
    }
    if offsets.len() >= 2 && last_offset + MAX_TOKEN_SIZE > bytes.len() {
        return Err(InvalidColumn::MissingPadding);
    }

    // Pass 2 — conformance: tokens strictly ascending (hence sorted and unique) and
    // the 256 single-byte tokens all present. Indexing `bytes` is sound after pass 1.
    let mut seen = [false; 256];
    let mut prev: &[u8] = &[];
    for w in offsets.windows(2) {
        let token = &bytes[w[0] as usize..w[1] as usize];
        if prev >= token {
            return Err(InvalidColumn::UnsortedTokens);
        }
        if token.len() == 1 {
            seen[token[0] as usize] = true;
        }
        prev = token;
    }
    if seen.iter().any(|&present| !present) {
        return Err(InvalidColumn::IncompleteAlphabet);
    }
    Ok(())
}

/// Minimum code width needed to address `num_tokens` distinct tokens:
/// `ceil(log2(num_tokens))`.
///
/// This is the runtime width of OnPair token codes for a dictionary with
/// `num_tokens` entries. It is independent from the training-time
/// [`MaxDictBits`](crate::MaxDictBits) budget.
pub fn code_bits_for_num_tokens(num_tokens: usize) -> u8 {
    debug_assert!(
        num_tokens >= 1,
        "log2(0) is undefined; num_tokens must be >= 1"
    );
    if num_tokens <= 1 {
        1
    } else {
        ((num_tokens as u32 - 1).ilog2() + 1) as u8
    }
}

/// Owned compact dictionary — **trusted**: holding one is a proof that its
/// buffers satisfy the invariants in this module's documentation.
///
/// The fields are private, so a value can only be obtained through a door that
/// establishes that proof: the trainer, or [`validate`](Self::validate) (checked) /
/// [`new_unchecked`](Self::new_unchecked) (`unsafe`) applied to deserialized
/// buffers. Read the buffers back with [`bytes`](Self::bytes) /
/// [`offsets`](Self::offsets), or move them out with [`into_raw`](Self::into_raw)
/// (e.g. to serialize).
#[derive(Default, Debug, Clone)]
pub struct CompactDictionary {
    /// Concatenated token bytes, followed by read-padding.
    bytes: Vec<u8>,
    /// `num_tokens + 1` offsets delimiting the tokens within `bytes`.
    offsets: Vec<u32>,
}

impl CompactDictionary {
    /// Number of tokens.
    #[inline]
    pub fn num_tokens(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    /// The token bytes, including trailing read-padding (the serialized
    /// `dict_bytes`; see `docs/interchange-format.md`).
    #[inline]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The `num_tokens + 1` token offsets (the serialized `dict_offsets`).
    #[inline]
    pub fn offsets(&self) -> &[u32] {
        &self.offsets
    }

    /// Consume the dictionary, returning its owned buffers `(dict_bytes,
    /// dict_offsets)` without copying — the inverse of [`validate`](Self::validate) /
    /// [`new_unchecked`](Self::new_unchecked). Use it to hand the read-padded token
    /// bytes and the `num_tokens + 1` offsets to another owner (e.g. to serialize)
    /// without the copy that [`bytes`](Self::bytes) / [`offsets`](Self::offsets)
    /// would force.
    ///
    /// This consumes `self`: the dictionary no longer exists afterward. It does not
    /// alter the buffers — `dict_bytes` keeps its trailing read-padding
    /// (`dict_bytes.len()` may exceed `dict_offsets.last()`), so the pair is still
    /// conformant and rebuilds into a trusted dictionary via
    /// [`validate`](Self::validate) (checked) or [`new_unchecked`](Self::new_unchecked).
    #[inline]
    pub fn into_raw(self) -> (Vec<u8>, Vec<u32>) {
        (self.bytes, self.offsets)
    }

    /// Seal raw buffers into a trusted dictionary. The crate-internal trust mint:
    /// the caller (trainer, or the [`validate`](Self::validate) door) guarantees the
    /// invariants.
    #[inline]
    pub(crate) fn from_raw(bytes: Vec<u8>, offsets: Vec<u32>) -> Self {
        Self { bytes, offsets }
    }

    /// Validate raw `(bytes, offsets)` deserialized from storage into a trusted
    /// dictionary (moved, no copy) — the checked door across the trust boundary. On
    /// success it is fully conformant: safe to decode *and* correct to
    /// search / tokenize, indistinguishable from a trainer-built dictionary.
    ///
    /// # Errors
    /// [`InvalidColumn`] for an addressability violation (too many tokens), a safety
    /// violation (decreasing offsets, an over-long token, missing read-padding), or
    /// a conformance violation (an empty token, unsorted/duplicate tokens, or an
    /// incomplete single-byte alphabet). Does not pad — a conformant serialized
    /// dictionary already carries the read-padding.
    pub fn validate(bytes: Vec<u8>, offsets: Vec<u32>) -> Result<Self, InvalidColumn> {
        validate_compact(&bytes, &offsets)?;
        Ok(Self::from_raw(bytes, offsets))
    }

    /// Seal raw `(bytes, offsets)` into a trusted dictionary **without** checking —
    /// the `unsafe` door across the trust boundary, for buffers the caller already
    /// knows are conformant (e.g. its own validated-on-write format).
    ///
    /// # Safety
    /// Every invariant in this module's docs is a precondition; the result must be
    /// indistinguishable from a [`validate`](Self::validate)d one. They split by how
    /// a violation bites, but both tiers are required:
    /// * **Read-safety** (violating these is UB — an unchecked decoder reads or
    ///   writes out of bounds): `offsets[0] == 0`, `offsets.len() == num_tokens + 1`,
    ///   non-decreasing offsets, every token `<= MAX_TOKEN_SIZE`, and read-padded
    ///   (`offsets.last() + MAX_TOKEN_SIZE <= bytes.len()`).
    /// * **Conformance** (violating these is not UB, but search / tokenize then give
    ///   wrong answers): no more than `2^16` tokens, strictly-increasing offsets,
    ///   tokens sorted and unique, and the 256-symbol alphabet complete.
    pub unsafe fn new_unchecked(bytes: Vec<u8>, offsets: Vec<u32>) -> Self {
        Self::from_raw(bytes, offsets)
    }

    /// Logical byte length — token bytes only, excluding read-padding.
    #[inline]
    pub fn logical_len(&self) -> usize {
        self.offsets.last().copied().unwrap_or(0) as usize
    }

    /// Minimum bits per code needed to address this dictionary,
    /// `ceil(log2(num_tokens))`. A consumer that bit-packs the code stream packs
    /// each code in this many bits. `8..=16` for a conformant dictionary.
    #[inline]
    pub fn code_bits(&self) -> u8 {
        code_bits_for_num_tokens(self.num_tokens())
    }

    /// Materialize the [`WideDictionary`] form (see
    /// [`CompactDictionaryView::to_wide`]). Borrow as a view first with
    /// [`Dictionary::as_view`].
    #[inline]
    pub fn to_wide(&self) -> WideDictionary {
        self.as_view().to_wide()
    }
}

impl Dictionary for CompactDictionary {
    type View<'a> = CompactDictionaryView<'a>;
    #[inline]
    fn as_view(&self) -> CompactDictionaryView<'_> {
        CompactDictionaryView {
            bytes: &self.bytes,
            offsets: &self.offsets,
        }
    }
}

/// Borrowed, `Copy`, **trusted** view over a compact dictionary's buffers.
///
/// Like [`CompactDictionary`] its fields are private: a value can only be obtained
/// from a trusted owned dictionary ([`Dictionary::as_view`]) or by validating raw
/// borrowed buffers with [`validate`](Self::validate) / [`new_unchecked`](Self::new_unchecked).
#[derive(Copy, Clone, Debug)]
pub struct CompactDictionaryView<'a> {
    /// Read-padded token bytes.
    bytes: &'a [u8],
    /// `num_tokens + 1` offsets into `bytes`.
    offsets: &'a [u32],
}

impl<'a> CompactDictionaryView<'a> {
    /// Seal raw borrowed buffers into a trusted view (crate-internal trust mint;
    /// the caller guarantees the invariants).
    #[inline]
    pub(crate) fn from_raw(bytes: &'a [u8], offsets: &'a [u32]) -> Self {
        Self { bytes, offsets }
    }

    /// Validate raw borrowed `(bytes, offsets)` into a trusted view over the same
    /// slices (no copy) — the checked door across the trust boundary. The borrowed
    /// bytes must already be read-padded (a borrow cannot be extended).
    ///
    /// # Errors
    /// As [`CompactDictionary::validate`].
    pub fn validate(bytes: &'a [u8], offsets: &'a [u32]) -> Result<Self, InvalidColumn> {
        validate_compact(bytes, offsets)?;
        Ok(Self::from_raw(bytes, offsets))
    }

    /// Seal raw borrowed `(bytes, offsets)` into a trusted view without checking.
    ///
    /// # Safety
    /// As [`CompactDictionary::new_unchecked`].
    pub unsafe fn new_unchecked(bytes: &'a [u8], offsets: &'a [u32]) -> Self {
        Self::from_raw(bytes, offsets)
    }

    /// Minimum bits per code needed to address this dictionary,
    /// `ceil(log2(num_tokens))`. See [`CompactDictionary::code_bits`].
    #[inline]
    pub fn code_bits(&self) -> u8 {
        code_bits_for_num_tokens(self.num_tokens())
    }

    /// Materialize the [`WideDictionary`] form: every token laid out in its own
    /// fixed [`MAX_TOKEN_SIZE`]-byte row, so a decode reaches a token at
    /// `code * MAX_TOKEN_SIZE` with no `code → offset → bytes` indirection. Worth
    /// building once to amortize over a bulk or repeated decode; see
    /// [`WideDictionary`] for the space/speed trade-off.
    ///
    /// The source is a **trusted** [`CompactDictionaryView`], so this never
    /// validates and never fails — the wide form is valid by construction. Two
    /// trusted invariants carry the build:
    ///
    /// * read-padding lets each row be filled with one fixed 16-byte copy from the
    ///   token's offset — an over-read past the token into neighbouring or padding
    ///   bytes (harmless: decode only ever reads a row's first `lens[id]` bytes),
    ///   kept in bounds by the padding;
    /// * the `≤ MAX_TOKEN_SIZE` length bound makes `lens[id] = len as u8` exact,
    ///   not a silent truncation.
    ///
    /// `O(num_tokens)`, dominated by the row copy.
    pub fn to_wide(&self) -> WideDictionary {
        let n = self.num_tokens();
        let mut data = vec![0u8; n * MAX_TOKEN_SIZE];
        let mut lens = vec![0u8; n];
        let src = self.bytes.as_ptr();
        let dst = data.as_mut_ptr();
        for id in 0..n {
            // SAFETY: a trusted view has `offsets.len() == n + 1`, so `id` and
            // `id + 1` index it in bounds. Offsets are strictly increasing with
            // every token length in `1..=MAX_TOKEN_SIZE`, so `end - off` neither
            // wraps nor overflows the `u8` length.
            let (off, end) = unsafe {
                (
                    *self.offsets.get_unchecked(id) as usize,
                    *self.offsets.get_unchecked(id + 1) as usize,
                )
            };
            // SAFETY: `id < n == lens.len()`.
            unsafe { *lens.get_unchecked_mut(id) = (end - off) as u8 };
            // SAFETY: dst — `(id + 1) * MAX_TOKEN_SIZE <= n * MAX_TOKEN_SIZE == data.len()`.
            // src — the read-padding invariant guarantees `MAX_TOKEN_SIZE` readable
            // bytes at `off`; `src` (borrowed dictionary) and the freshly-allocated
            // `dst` are distinct allocations, so the copy cannot overlap.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    src.add(off),
                    dst.add(id * MAX_TOKEN_SIZE),
                    MAX_TOKEN_SIZE,
                );
            }
        }
        WideDictionary::from_raw(data, lens)
    }
}

impl DictionaryView for CompactDictionaryView<'_> {
    #[inline]
    fn num_tokens(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    #[inline]
    fn token(&self, id: Token) -> &[u8] {
        let begin = self.offsets[id as usize] as usize;
        let end = self.offsets[id as usize + 1] as usize;
        &self.bytes[begin..end]
    }

    #[inline]
    fn token_len(&self, id: Token) -> usize {
        (self.offsets[id as usize + 1] - self.offsets[id as usize]) as usize
    }

    #[inline]
    unsafe fn token_ptr(&self, id: Token) -> *const u8 {
        // SAFETY: id < num_tokens ⇒ offsets[id] is in bounds; the read-padding
        // invariant guarantees MAX_TOKEN_SIZE readable bytes at the offset.
        unsafe {
            self.bytes
                .as_ptr()
                .add(*self.offsets.get_unchecked(id as usize) as usize)
        }
    }

    #[inline]
    unsafe fn token_len_unchecked(&self, id: Token) -> usize {
        // SAFETY: id < num_tokens ⇒ offsets[id] and offsets[id + 1] are in bounds.
        unsafe {
            (*self.offsets.get_unchecked(id as usize + 1)
                - *self.offsets.get_unchecked(id as usize)) as usize
        }
    }
}

impl<'a> From<&'a CompactDictionary> for CompactDictionaryView<'a> {
    #[inline]
    fn from(d: &'a CompactDictionary) -> Self {
        d.as_view()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dict(offsets: Vec<u32>, bytes: &[u8]) -> CompactDictionary {
        CompactDictionary::from_raw(bytes.to_vec(), offsets)
    }

    #[test]
    fn num_tokens_zero_when_offsets_empty() {
        assert_eq!(CompactDictionary::default().num_tokens(), 0);
    }

    #[test]
    fn num_tokens_is_offsets_len_minus_one() {
        assert_eq!(dict(vec![0, 3, 5, 8], b"").num_tokens(), 3);
    }

    #[test]
    fn token_returns_correct_slice() {
        let d = dict(vec![0, 1, 3, 6], b"abcdef");
        let v = d.as_view();
        assert_eq!(v.token(0), b"a");
        assert_eq!(v.token(1), b"bc");
        assert_eq!(v.token(2), b"def");
        assert_eq!(v.token_len(2), 3);
    }

    #[test]
    fn code_bits_is_ceil_log2_num_tokens() {
        assert_eq!(dict(vec![0; 257], b"").code_bits(), 8); // 256 tokens -> 8 bits
        assert_eq!(dict(vec![0; 258], b"").code_bits(), 9); // 257 tokens -> 9 bits
        assert_eq!(dict(vec![0; 513], b"").code_bits(), 9); // 512 tokens -> 9 bits
        assert_eq!(dict(vec![0; 514], b"").code_bits(), 10); // 513 tokens -> 10 bits
        assert_eq!(dict(vec![0; 65_537], b"").code_bits(), 16); // 65536 tokens -> 16 bits
    }

    #[test]
    fn pad_raw_extends_to_max_token_read() {
        // Last token "bc" is 2 bytes; padding fills to offset(last) + MAX_TOKEN_SIZE.
        let mut bytes = b"abc".to_vec();
        pad_raw(&mut bytes, &[0, 1, 3]);
        assert_eq!(bytes.len(), 1 + MAX_TOKEN_SIZE); // offset(last)=1, +16
    }

    #[test]
    fn pad_raw_is_idempotent() {
        let mut bytes = b"abc".to_vec();
        let offsets = [0u32, 1, 3];
        pad_raw(&mut bytes, &offsets);
        let len = bytes.len();
        pad_raw(&mut bytes, &offsets);
        assert_eq!(bytes.len(), len);
    }

    #[test]
    fn pad_raw_tops_up_insufficient_trailing_bytes() {
        // bytes already exceed logical_len (3) but lack room for a full
        // MAX_TOKEN_SIZE read from the last token's start (offset 1).
        let mut bytes = vec![b'a', b'b', b'c', 0];
        pad_raw(&mut bytes, &[0, 1, 3]);
        assert_eq!(bytes.len(), 1 + MAX_TOKEN_SIZE);
    }

    #[test]
    fn pad_raw_noop_for_full_width_last_token() {
        let mut bytes = vec![b'z'; MAX_TOKEN_SIZE];
        pad_raw(&mut bytes, &[0, MAX_TOKEN_SIZE as u32]);
        assert_eq!(bytes.len(), MAX_TOKEN_SIZE);
    }

    /// Read-padded `(bytes, offsets)` from an explicit token list — the caller
    /// controls exactly which conformance property is under test.
    fn padded(tokens: &[&[u8]]) -> (Vec<u8>, Vec<u32>) {
        let mut bytes = Vec::new();
        let mut offsets = vec![0u32];
        for t in tokens {
            bytes.extend_from_slice(t);
            offsets.push(bytes.len() as u32);
        }
        bytes.resize(bytes.len() + MAX_TOKEN_SIZE, 0); // worst-case read padding
        (bytes, offsets)
    }

    /// A conformant dictionary: all 256 single-byte tokens plus `extra`, sorted and
    /// deduped (so strictly increasing, sorted, unique, complete, and padded).
    fn conformant(extra: &[&[u8]]) -> (Vec<u8>, Vec<u32>) {
        let mut toks: Vec<Vec<u8>> = (0u16..256).map(|b| vec![b as u8]).collect();
        for &t in extra {
            toks.push(t.to_vec());
        }
        toks.sort();
        toks.dedup();
        let refs: Vec<&[u8]> = toks.iter().map(Vec::as_slice).collect();
        padded(&refs)
    }

    /// A conformant dictionary with exactly `num_tokens` entries. Multi-byte
    /// entries are generated as unique two-byte sequences, then sorted together
    /// with the complete single-byte alphabet.
    fn conformant_with_num_tokens(num_tokens: usize) -> (Vec<u8>, Vec<u32>) {
        assert!((256..=256 + u16::MAX as usize + 1).contains(&num_tokens));
        let mut toks: Vec<Vec<u8>> = (0u16..256).map(|b| vec![b as u8]).collect();
        toks.extend((0..num_tokens - 256).map(|value| (value as u16).to_be_bytes().to_vec()));
        toks.sort();
        let refs: Vec<&[u8]> = toks.iter().map(Vec::as_slice).collect();
        padded(&refs)
    }

    /// Map to `Result<(), _>` so we can compare (`CompactDictionary` isn't `Eq`).
    fn check(bytes: Vec<u8>, offsets: Vec<u32>) -> Result<(), InvalidColumn> {
        CompactDictionary::validate(bytes, offsets).map(|_| ())
    }

    #[test]
    fn validate_accepts_conformant() {
        let (bytes, offsets) = conformant(&[b"bc", b"def"]);
        assert_eq!(check(bytes, offsets), Ok(()));
    }

    #[test]
    fn validate_enforces_token_address_space() {
        let (bytes, offsets) = conformant_with_num_tokens(MAX_NUM_TOKENS);
        assert_eq!(check(bytes, offsets), Ok(()));

        let (bytes, offsets) = conformant_with_num_tokens(MAX_NUM_TOKENS + 1);
        assert_eq!(check(bytes, offsets), Err(InvalidColumn::CodeOutOfRange));
    }

    #[test]
    fn validate_classifies_safety_corruption() {
        // Decreasing offsets (would underflow the length subtraction).
        let mut bytes = b"ab".to_vec();
        bytes.resize(2 + MAX_TOKEN_SIZE, 0);
        assert_eq!(
            check(bytes, vec![0, 2, 1]),
            Err(InvalidColumn::NonDecreasingOffsets)
        );

        // Zero-length token (`e == s`).
        assert_eq!(
            check(vec![0u8; MAX_TOKEN_SIZE], vec![0, 0]),
            Err(InvalidColumn::EmptyToken)
        );

        // Token longer than MAX_TOKEN_SIZE.
        assert_eq!(
            check(vec![b'x'; 20 + MAX_TOKEN_SIZE], vec![0, 20]),
            Err(InvalidColumn::TokenTooLarge)
        );

        // Missing the trailing read-padding.
        assert_eq!(
            check(b"abc".to_vec(), vec![0, 1, 3]),
            Err(InvalidColumn::MissingPadding)
        );
    }

    #[test]
    fn validate_classifies_conformance_corruption() {
        // Safe + padded but out of order (also how a duplicate would surface).
        let (bytes, offsets) = padded(&[&[1u8], &[0u8]]);
        assert_eq!(check(bytes, offsets), Err(InvalidColumn::UnsortedTokens));

        // Sorted + safe but missing all but three single-byte tokens.
        let (bytes, offsets) = padded(&[&[0u8], &[1u8], &[2u8]]);
        assert_eq!(
            check(bytes, offsets),
            Err(InvalidColumn::IncompleteAlphabet)
        );
    }

    #[test]
    fn new_unchecked_matches_validate() {
        let (bytes, offsets) = conformant(&[b"bc"]);
        let checked = CompactDictionary::validate(bytes.clone(), offsets.clone()).unwrap();
        // SAFETY: `conformant` produces a conformant dictionary.
        let trusted = unsafe { CompactDictionary::new_unchecked(bytes, offsets) };
        assert_eq!(checked.bytes(), trusted.bytes());
        assert_eq!(checked.offsets(), trusted.offsets());
    }

    #[test]
    fn into_raw_returns_buffers_and_round_trips() {
        let (bytes, offsets) = conformant(&[b"bc", b"def"]);
        let num_tokens = offsets.len() - 1;
        let dict = CompactDictionary::validate(bytes.clone(), offsets.clone()).unwrap();

        // The owned buffers come back byte-for-byte, read-padding included.
        let (raw_bytes, raw_offsets) = dict.into_raw();
        assert_eq!(raw_bytes, bytes);
        assert_eq!(raw_offsets, offsets);

        // ...and rebuild into an equivalent trusted dictionary.
        let rebuilt = CompactDictionary::validate(raw_bytes, raw_offsets).unwrap();
        assert_eq!(rebuilt.num_tokens(), num_tokens);
    }

    #[test]
    fn view_validate_yields_usable_view() {
        let (bytes, offsets) = conformant(&[b"bc"]);
        let view = CompactDictionaryView::validate(&bytes, &offsets).unwrap();
        assert_eq!(view.num_tokens(), 257); // 256 single bytes + "bc"
        assert_eq!(view.token(0), &[0u8]); // byte 0 sorts first

        // A non-conformant borrow is rejected.
        let raw: &[u8] = b"abc";
        assert!(CompactDictionaryView::validate(raw, &[0, 1, 3]).is_err());
    }
}
