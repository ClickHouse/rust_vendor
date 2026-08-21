// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Fast non-cryptographic hashing for the training-time pair-frequency and
//! longest-prefix-match maps.

use std::hash::BuildHasherDefault;
use std::hash::Hasher;

const K: u64 = 0x517c_c1b7_2722_0a95;

/// FxHash-style hasher: a rotate-xor-multiply over each word. Fast and
/// adequate for the small integer keys the encoder hashes.
#[derive(Default)]
pub(crate) struct FxHasher {
    hash: u64,
}

impl Hasher for FxHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.hash
    }
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.hash = (self.hash.rotate_left(5) ^ b as u64).wrapping_mul(K);
        }
    }
    #[inline]
    fn write_u32(&mut self, i: u32) {
        self.hash = (self.hash.rotate_left(5) ^ i as u64).wrapping_mul(K);
    }
    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.hash = (self.hash.rotate_left(5) ^ i).wrapping_mul(K);
    }
}

/// [`BuildHasher`](std::hash::BuildHasher) for [`FxHasher`].
pub(crate) type FxBuildHasher = BuildHasherDefault<FxHasher>;

/// Hasher used by the longest-prefix-match maps.
pub(crate) type MapHasher = hashbrown::DefaultHashBuilder;

/// Hash map keyed through [`MapHasher`].
pub(crate) type Map<K, V> = hashbrown::HashMap<K, V, MapHasher>;

/// An empty [`Map`].
#[inline]
pub(crate) fn map<K, V>() -> Map<K, V> {
    Map::with_hasher(MapHasher::default())
}

/// A [`Map`] preallocated for `cap` entries.
#[inline]
pub(crate) fn map_with_capacity<K, V>(cap: usize) -> Map<K, V> {
    Map::with_capacity_and_hasher(cap, MapHasher::default())
}
