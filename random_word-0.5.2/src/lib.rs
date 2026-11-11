//! # random_word
//!
//! The `random_word` crate provides an efficient way to generate
//! random words. Included words can be filtered by length or
//! first character.
//!
//! ## Usage
//! You **MUST** enable a crate language feature.
//! Crate language features are mandatory to reduce binary size.
//! Example for English in Cargo.toml:
//! ```toml
//! [dependencies]
//! random_word = { version = "0.5.2", features = ["en"] }
//! ```
//!
//! **Supported Languages**
//! - English
//! - Spanish
//! - German
//! - French
//! - Japanese
//! - Russian
//! - Chinese
//!

#[allow(unused_imports)]
#[allow(unused_macros)]
#[allow(unused_variables)]
mod words;

#[allow(unused)]
mod tests;

pub use words::Lang;

use rand::{prelude::IndexedRandom, rng};


/// Returns all words with the given language.
///
/// # Example
/// ```
/// use random_word::Lang;
/// let words = random_word::all(Lang::En);
/// assert!(!words.is_empty());
/// ```
#[inline(always)]
pub fn all(lang: Lang) -> &'static [&'static str] {
    words::get(lang)
}

/// Returns a random word with the given language.
///
/// # Example
/// ```
/// use random_word::Lang;
/// let word = random_word::get(Lang::En);
/// assert!(!word.is_empty());
/// ```
#[inline(always)]
pub fn get(lang: Lang) -> &'static str {
    words::get(lang)
        .choose(&mut rng())
        .expect("array is empty")
}

/// Returns all words with the given length and language.
///
/// # Example
/// ```
/// use random_word::Lang;
/// let words = random_word::all_len(4, Lang::En);
/// assert!(words.is_some());
/// ```
#[inline(always)]
pub fn all_len(len: usize, lang: Lang) -> Option<&'static [&'static str]> {
    words::get_len(len, lang).map(|boxed| &**boxed)
}

/// Returns a random word with the given length and language.
///
/// # Example
/// ```
/// use random_word::Lang;
/// let word = random_word::get_len(4, Lang::En);
/// assert!(word.is_some());
/// ```
#[inline(always)]
pub fn get_len(len: usize, lang: Lang) -> Option<&'static str> {
    words::get_len(len, lang)?
        .choose(&mut rng())
        .copied()
}

/// Returns all words with the given starting character and language.
///
/// # Example
/// ```
/// use random_word::Lang;
/// let words = random_word::all_starts_with('c', Lang::En);
/// assert!(words.is_some());
/// ```
#[inline(always)]
pub fn all_starts_with(char: char, lang: Lang) -> Option<&'static [&'static str]> {
    words::get_starts_with(char, lang).map(|boxed| &**boxed)
}

/// Returns a random word with the given starting character and language.
///
/// # Example
/// ```
/// use random_word::Lang;
/// let word = random_word::get_starts_with('c', Lang::En);
/// assert!(word.is_some());
/// ```
#[inline(always)]
pub fn get_starts_with(char: char, lang: Lang) -> Option<&'static str> {
    words::get_starts_with(char, lang)?
        .choose(&mut rng())
        .copied()
}
