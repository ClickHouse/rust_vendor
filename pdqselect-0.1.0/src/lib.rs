//! Pattern-defeating quickselect
//!
//! The algorithm is based on pattern-defeating quicksort by Orson Peters, published at:
//! https://github.com/orlp/pdqsort
//! It is also heavily adapted from the Rust implementation of pdqsort
//! (https://github.com/stjepang/pdqsort) and Rust's own `sort_unstable`.
//!
//! # Properties
//!
//! - Best-case running time is `O(n)`.
//! - Worst-case running time is `O(n log n)`.
//! - Does not allocate additional memory.
//! - Uses `#![no_std]`.
//!
//! # Examples
//!
//! ```
//! let mut v = [-5i32, 4, 1, -3, 2];
//! let k = 3;
//!
//! pdqselect::select(&mut v, k);
//! let kth = v[k];
//! assert!(v[..k].iter().all(|&x| x <= kth));
//! assert!(v[k+1..].iter().all(|&x| x >= kth));
//!
//! pdqselect::select_by(&mut v, k, |a, b| b.cmp(a));
//! let kth = v[k];
//! assert!(v[..k].iter().all(|&x| x >= kth));
//! assert!(v[k+1..].iter().all(|&x| x <= kth));
//!
//! pdqselect::select_by_key(&mut v, k, |k| k.abs());
//! let kth = v[k].abs();
//! assert!(v[..k].iter().all(|&x| x.abs() <= kth));
//! assert!(v[k+1..].iter().all(|&x| x.abs() >= kth));
//! ```

#![no_std]

use core::cmp::{self, Ordering};
use core::mem;
use core::ptr;

/// When dropped, copies from `src` into `dest`.
struct CopyOnDrop<T> {
    src: *mut T,
    dest: *mut T,
}

impl<T> Drop for CopyOnDrop<T> {
    fn drop(&mut self) {
        unsafe { ptr::copy_nonoverlapping(self.src, self.dest, 1); }
    }
}

/// Shifts the first element to the right until it encounters a greater or equal element.
fn shift_head<T, F>(v: &mut [T], is_less: &mut F)
    where F: FnMut(&T, &T) -> bool
{
    let len = v.len();
    unsafe {
        // If the first two elements are out-of-order...
        if len >= 2 && is_less(v.get_unchecked(1), v.get_unchecked(0)) {
            // Read the first element into a stack-allocated variable. If a following comparison
            // operation panics, `hole` will get dropped and automatically write the element back
            // into the slice.
            let mut tmp = mem::ManuallyDrop::new(ptr::read(v.get_unchecked(0)));
            let mut hole = CopyOnDrop {
                src: &mut *tmp,
                dest: v.get_unchecked_mut(1),
            };
            ptr::copy_nonoverlapping(v.get_unchecked(1), v.get_unchecked_mut(0), 1);

            for i in 2..len {
                if !is_less(v.get_unchecked(i), &*tmp) {
                    break;
                }

                // Move `i`-th element one place to the left, thus shifting the hole to the right.
                ptr::copy_nonoverlapping(v.get_unchecked(i), v.get_unchecked_mut(i - 1), 1);
                hole.dest = v.get_unchecked_mut(i);
            }
            // `hole` gets dropped and thus copies `tmp` into the remaining hole in `v`.
        }
    }
}

/// Shifts the last element to the left until it encounters a smaller or equal element.
fn shift_tail<T, F>(v: &mut [T], is_less: &mut F)
    where F: FnMut(&T, &T) -> bool
{
    let len = v.len();
    unsafe {
        // If the last two elements are out-of-order...
        if len >= 2 && is_less(v.get_unchecked(len - 1), v.get_unchecked(len - 2)) {
            // Read the last element into a stack-allocated variable. If a following comparison
            // operation panics, `hole` will get dropped and automatically write the element back
            // into the slice.
            let mut tmp = mem::ManuallyDrop::new(ptr::read(v.get_unchecked(len - 1)));
            let mut hole = CopyOnDrop {
                src: &mut *tmp,
                dest: v.get_unchecked_mut(len - 2),
            };
            ptr::copy_nonoverlapping(v.get_unchecked(len - 2), v.get_unchecked_mut(len - 1), 1);

            for i in (0..len-2).rev() {
                if !is_less(&*tmp, v.get_unchecked(i)) {
                    break;
                }

                // Move `i`-th element one place to the right, thus shifting the hole to the left.
                ptr::copy_nonoverlapping(v.get_unchecked(i), v.get_unchecked_mut(i + 1), 1);
                hole.dest = v.get_unchecked_mut(i);
            }
            // `hole` gets dropped and thus copies `tmp` into the remaining hole in `v`.
        }
    }
}

/// Partially sorts a slice by shifting several out-of-order elements around.
///
/// Returns `true` if the slice is sorted at the end. This function is `O(n)` worst-case.
#[cold]
fn partial_insertion_sort<T, F>(v: &mut [T], is_less: &mut F) -> bool
    where F: FnMut(&T, &T) -> bool
{
    // Maximum number of adjacent out-of-order pairs that will get shifted.
    const MAX_STEPS: usize = 5;
    // If the slice is shorter than this, don't shift any elements.
    const SHORTEST_SHIFTING: usize = 50;

    let len = v.len();
    let mut i = 1;

    for _ in 0..MAX_STEPS {
        unsafe {
            // Find the next pair of adjacent out-of-order elements.
            while i < len && !is_less(v.get_unchecked(i), v.get_unchecked(i - 1)) {
                i += 1;
            }
        }

        // Are we done?
        if i == len {
            return true;
        }

        // Don't shift elements on short arrays, that has a performance cost.
        if len < SHORTEST_SHIFTING {
            return false;
        }

        // Swap the found pair of elements. This puts them in correct order.
        v.swap(i - 1, i);

        // Shift the smaller element to the left.
        // shift_tail(unsafe{v.get_unchecked_mut(..i)}, is_less);
        shift_tail(&mut v[..i], is_less);
        // Shift the greater element to the right.
        // shift_head(unsafe{v.get_unchecked_mut(i..)}, is_less);
        shift_head(&mut v[i..], is_less);
    }

    // Didn't manage to sort the slice in the limited number of steps.
    false
}

/// Sorts a slice using insertion sort, which is `O(n^2)` worst-case.
fn insertion_sort<T, F>(v: &mut [T], is_less: &mut F)
    where F: FnMut(&T, &T) -> bool
{
    for i in 1..v.len() {
        // shift_tail(unsafe{v.get_unchecked_mut(..i+1)}, is_less);
        shift_tail(&mut v[..i+1], is_less);
    }
}

/// Sorts `v` using heapsort, which guarantees `O(n log n)` worst-case.
#[cold]
pub fn heapsort<T, F>(v: &mut [T], is_less: &mut F)
    where F: FnMut(&T, &T) -> bool
{
    // This binary heap respects the invariant `parent >= child`.
    let mut sift_down = |v: &mut [T], mut node| {
        loop {
            // Children of `node`:
            let left = 2 * node + 1;
            let right = 2 * node + 2;

            // Choose the greater child.
            /*let greater = unsafe { if right < v.len() &&
                    is_less(v.get_unchecked(left), v.get_unchecked(right))
                {
                    right
                } else {
                    left
                }
            };*/

            let greater = if right < v.len() && is_less(&v[left], &v[right]) {
                right
            } else {
                left
            };

            // Stop if the invariant holds at `node`.
            /*unsafe {
                if greater >= v.len() ||
                    !is_less(v.get_unchecked(node), v.get_unchecked(greater))
                {
                    break;
                }
            }*/
            if greater >= v.len() || !is_less(&v[node], &v[greater]) {
                break;
            }

            // Swap `node` with the greater child, move one step down, and continue sifting.
            v.swap(node, greater);
            node = greater;
        }
    };

    // Build the heap in linear time.
    for i in (0 .. v.len() / 2).rev() {
        sift_down(v, i);
    }

    // Pop maximal elements from the heap.
    for i in (1 .. v.len()).rev() {
        v.swap(0, i);
        // sift_down(unsafe { v.get_unchecked_mut(..i) }, 0);
        sift_down(&mut v[..i], 0);
    }
}

/// Partitions `v` into elements smaller than `pivot`, followed by elements greater than or equal
/// to `pivot`.
///
/// Returns the number of elements smaller than `pivot`.
///
/// Partitioning is performed block-by-block in order to minimize the cost of branching operations.
/// This idea is presented in the [`BlockQuicksort`][pdf] paper.
///
/// [pdf]: http://drops.dagstuhl.de/opus/volltexte/2016/6389/pdf/LIPIcs-ESA-2016-38.pdf
fn partition_in_blocks<T, F>(v: &mut [T], pivot: &T, is_less: &mut F) -> usize
    where F: FnMut(&T, &T) -> bool
{
    // Number of elements in a typical block.
    const BLOCK: usize = 128;

    // The partitioning algorithm repeats the following steps until completion:
    //
    // 1. Trace a block from the left side to identify elements greater than or equal to the pivot.
    // 2. Trace a block from the right side to identify elements smaller than the pivot.
    // 3. Exchange the identified elements between the left and right side.
    //
    // We keep the following variables for a block of elements:
    //
    // 1. `block` - Number of elements in the block.
    // 2. `start` - Start pointer into the `offsets` array.
    // 3. `end` - End pointer into the `offsets` array.
    // 4. `offsets - Indices of out-of-order elements within the block.

    // The current block on the left side (from `l` to `l.offset(block_l)`).
    let mut l = v.as_mut_ptr();
    let mut block_l = BLOCK;
    let mut start_l = ptr::null_mut();
    let mut end_l = ptr::null_mut();
    let mut offsets_l: [u8; BLOCK] = unsafe { mem::uninitialized() };

    // The current block on the right side (from `r.offset(-block_r)` to `r`).
    let mut r = unsafe { l.offset(v.len() as isize) };
    let mut block_r = BLOCK;
    let mut start_r = ptr::null_mut();
    let mut end_r = ptr::null_mut();
    let mut offsets_r: [u8; BLOCK] = unsafe { mem::uninitialized() };

    // FIXME: When we get VLAs, try creating one array of length `min(v.len(), 2 * BLOCK)` rather
    // than two fixed-size arrays of length `BLOCK`. VLAs might be more cache-efficient.

    // Returns the number of elements between pointers `l` (inclusive) and `r` (exclusive).
    fn width<T>(l: *mut T, r: *mut T) -> usize {
        assert!(mem::size_of::<T>() > 0); // already done, no?
        (r as usize - l as usize) / mem::size_of::<T>()
    }

    loop {
        // We are done with partitioning block-by-block when `l` and `r` get very close. Then we do
        // some patch-up work in order to partition the remaining elements in between.
        let is_done = width(l, r) <= 2 * BLOCK;

        if is_done {
            // Number of remaining elements (still not compared to the pivot).
            let mut rem = width(l, r);
            if start_l < end_l || start_r < end_r {
                rem -= BLOCK;
            }

            // Adjust block sizes so that the left and right block don't overlap, but get perfectly
            // aligned to cover the whole remaining gap.
            if start_l < end_l {
                block_r = rem;
            } else if start_r < end_r {
                block_l = rem;
            } else {
                block_l = rem / 2;
                block_r = rem - block_l;
            }
            debug_assert!(block_l <= BLOCK && block_r <= BLOCK);
            debug_assert_eq!(width(l, r), block_l + block_r);
        }

        if start_l == end_l {
            // Trace `block_l` elements from the left side.
            start_l = offsets_l.as_mut_ptr();
            end_l = offsets_l.as_mut_ptr();
            let mut elem = l;

            for i in 0..block_l {
                unsafe {
                    // Branchless comparison.
                    *end_l = i as u8;
                    end_l = end_l.offset(!is_less(&*elem, pivot) as isize);
                    elem = elem.offset(1);
                }
            }
        }

        if start_r == end_r {
            // Trace `block_r` elements from the right side.
            start_r = offsets_r.as_mut_ptr();
            end_r = offsets_r.as_mut_ptr();
            let mut elem = r;

            for i in 0..block_r {
                unsafe {
                    // Branchless comparison.
                    elem = elem.offset(-1);
                    *end_r = i as u8;
                    end_r = end_r.offset(is_less(&*elem, pivot) as isize);
                }
            }
        }

        // Number of out-of-order elements to swap between the left and right side.
        let count = cmp::min(width(start_l, end_l), width(start_r, end_r));

        if count > 0 {
            macro_rules! left { () => { l.offset(*start_l as isize) } }
            macro_rules! right { () => { r.offset(-(*start_r as isize) - 1) } }

            // Instead of swapping one pair at the time, it is more efficient to perform a cyclic
            // permutation. This is not strictly equivalent to swapping, but produces a similar
            // result using fewer memory operations.
            unsafe {
                let tmp = ptr::read(left!());
                ptr::copy_nonoverlapping(right!(), left!(), 1);

                for _ in 1..count {
                    start_l = start_l.offset(1);
                    ptr::copy_nonoverlapping(left!(), right!(), 1);
                    start_r = start_r.offset(1);
                    ptr::copy_nonoverlapping(right!(), left!(), 1);
                }

                ptr::copy_nonoverlapping(&tmp, right!(), 1);
                mem::forget(tmp);
                start_l = start_l.offset(1);
                start_r = start_r.offset(1);
            }
        }

        if start_l == end_l {
            // All out-of-order elements in the left block were moved. Move to the next block.
            l = unsafe { l.offset(block_l as isize) };
        }

        if start_r == end_r {
            // All out-of-order elements in the right block were moved. Move to the previous block.
            r = unsafe { r.offset(-(block_r as isize)) };
        }

        if is_done {
            break;
        }
    }

    // All that remains now is at most one block (either the left or the right) with out-of-order
    // elements that need to be moved. Such remaining elements can be simply shifted to the end
    // within their block.

    if start_l < end_l {
        // The left block remains.
        // Move its remaining out-of-order elements to the far right.
        debug_assert_eq!(width(l, r), block_l);
        while start_l < end_l {
            unsafe {
                end_l = end_l.offset(-1);
                ptr::swap(l.offset(*end_l as isize), r.offset(-1));
                r = r.offset(-1);
            }
        }
        width(v.as_mut_ptr(), r)
    } else if start_r < end_r {
        // The right block remains.
        // Move its remaining out-of-order elements to the far left.
        debug_assert_eq!(width(l, r), block_r);
        while start_r < end_r {
            unsafe {
                end_r = end_r.offset(-1);
                ptr::swap(l, r.offset(-(*end_r as isize) - 1));
                l = l.offset(1);
            }
        }
        width(v.as_mut_ptr(), l)
    } else {
        // Nothing else to do, we're done.
        width(v.as_mut_ptr(), l)
    }
}

/// Partitions `v` into elements smaller than `v[pivot]`, followed by elements greater than or
/// equal to `v[pivot]`.
///
/// Returns a tuple of:
///
/// 1. Number of elements smaller than `v[pivot]`.
/// 2. True if `v` was already partitioned.
fn partition<T, F>(v: &mut [T], pivot: usize, is_less: &mut F) -> (usize, bool)
    where F: FnMut(&T, &T) -> bool
{
    let (mid, was_partitioned) = {
        // Place the pivot at the beginning of slice.
        v.swap(0, pivot);
        // let (pivot, v) = v.split_first_mut().unwrap();
        let (pivot, v) = v.split_at_mut(1);
        let pivot = &mut pivot[0];
        // could prob do `swap_remove`?

        // Read the pivot into a stack-allocated variable for efficiency. If a following comparison
        // operation panics, the pivot will be automatically written back into the slice.
        let mut tmp = mem::ManuallyDrop::new(unsafe { ptr::read(pivot) });
        let _pivot_guard = CopyOnDrop {
            src: &mut *tmp,
            dest: pivot,
        };
        let pivot = &*tmp;

        // Find the first pair of out-of-order elements.
        let mut l = 0;
        let mut r = v.len();
        unsafe {
            // Find the first element greater then or equal to the pivot.
            while l < r && is_less(v.get_unchecked(l), pivot) {
                l += 1;
            }

            // Find the last element smaller that the pivot.
            while l < r && !is_less(v.get_unchecked(r - 1), pivot) {
                r -= 1;
            }
        }

        // (l + partition_in_blocks(unsafe { v.get_unchecked_mut(l..r) }, pivot, is_less), l >= r)
        (l + partition_in_blocks(&mut v[l..r], pivot, is_less), l >= r)

        // `_pivot_guard` goes out of scope and writes the pivot (which is a stack-allocated
        // variable) back into the slice where it originally was. This step is critical in ensuring
        // safety!
    };

    // Place the pivot between the two partitions.
    v.swap(0, mid);

    (mid, was_partitioned)
}

/// Partitions `v` into elements equal to `v[pivot]` followed by elements greater than `v[pivot]`.
///
/// Returns the number of elements equal to the pivot. It is assumed that `v` does not contain
/// elements smaller than the pivot.
fn partition_equal<T, F>(v: &mut [T], pivot: usize, is_less: &mut F) -> usize
    where F: FnMut(&T, &T) -> bool
{
    // Place the pivot at the beginning of slice.
    v.swap(0, pivot);
    let (pivot, v) = v.split_at_mut(1);
    let pivot = &mut pivot[0];
    // let (pivot, v) = v.split_first_mut().unwrap();

    // Read the pivot into a stack-allocated variable for efficiency. If a following comparison
    // operation panics, the pivot will be automatically written back into the slice.
    let mut tmp = mem::ManuallyDrop::new(unsafe { ptr::read(pivot) });
    let _pivot_guard = CopyOnDrop {
        src: &mut *tmp,
        dest: pivot,
    };
    let pivot = &*tmp;

    // Now partition the slice.
    let mut l = 0;
    let mut r = v.len();
    loop {
        unsafe {
            // Find the first element greater that the pivot.
            while l < r && !is_less(pivot, v.get_unchecked(l)) {
                l += 1;
            }

            // Find the last element equal to the pivot.
            while l < r && is_less(pivot, v.get_unchecked(r - 1)) {
                r -= 1;
            }

            // Are we done?
            if l >= r {
                break;
            }

            // Swap the found pair of out-of-order elements.
            r -= 1;
            ptr::swap(v.get_unchecked_mut(l), v.get_unchecked_mut(r));
            l += 1;
        }
    }

    // We found `l` elements equal to the pivot. Add 1 to account for the pivot itself.
    l + 1

    // `_pivot_guard` goes out of scope and writes the pivot (which is a stack-allocated variable)
    // back into the slice where it originally was. This step is critical in ensuring safety!
}

/// Scatters some elements around in an attempt to break patterns that might cause imbalanced
/// partitions in quickselect.
#[cold]
fn break_patterns<T>(v: &mut [T]) {
    let len = v.len();
    if len >= 8 {
        // Pseudorandom number generator from the "Xorshift RNGs" paper by George Marsaglia.
        let mut random = len as u32;
        // TODO: bench this vs the Xoroshiro128 core
        let mut gen_u32 = || {
            random ^= random << 13;
            random ^= random >> 17;
            random ^ random << 5
        };
        let mut gen_usize = || {
            if mem::size_of::<usize>() <= 4 {
                gen_u32() as usize
            } else {
                (((gen_u32() as u64) << 32) | (gen_u32() as u64)) as usize
                // ((u64::from(gen_u32()) << 32) | u64::from(gen_u32())) as usize
            }
        };

        // Take random numbers modulo this number.
        // The number fits into `usize` because `len` is not greater than `isize::MAX`.
        let modulus = len.next_power_of_two();

        // Some pivot candidates will be in the nearby of this index. Let's randomize them.
        let pos = len / 4 * 2;

        for i in 0..3 {
            // Generate a random number modulo `len`. However, in order to avoid costly operations
            // we first take it modulo a power of two, and then decrease by `len` until it fits
            // into the range `[0, len - 1]`.
            let mut other = gen_usize() & (modulus - 1);

            // `other` is guaranteed to be less than `2 * len`.
            if other >= len {
                other -= len;
            }

            v.swap(pos - 1 + i, other);
        }
    }
}

/*#[cold]
fn break_xoroshiro<T>(v: &mut [T]) {
    let len = v.len();
    if len < 8 {
        return;
    }

    // Pseudorandom number generator from the "Xorshift RNGs" paper by George Marsaglia.
    let mut first = len as u64;
    let mut second = {
        let mut z = first + 0x9E37_79B9_7F4A_7C15_u64;
        // first = z;
        z = (z ^ (z >> 30)) * 0xBF58_476D_1CE4_E5B9_u64;
        z = (z ^ (z >> 27)) * 0x94D0_49BB_1331_11EB_u64;
        z ^ (z >> 31)
    };

    let mut gen_u64 = || {
        #[inline]
        fn rotl(x: u64, k: i32) -> u64 {
            (x << k) | (x >> (64 - k))
        }

        let s0 = first;
        let mut s1 = second;
        let result = s0 + s1;

        s1 ^= s0;
        first = rotl(s0, 55) ^ s1 ^ (s1 << 14);
        second = rotl(s1, 36);

        result
    };

    let mut gen_usize = || {
        if mem::size_of::<usize>() == 4 {
            gen_u64() as u32 as usize
        } else {
            gen_u64() as usize
        }
    };

    // Take random numbers modulo this number.
    // The number fits into `usize` because `len` is not greater than `isize::MAX`.
    let modulus = len.next_power_of_two();

    // Some pivot candidates will be in the nearby of this index. Let's randomize them.
    let pos = len / 4 * 2;

    for i in 0..3 {
        // Generate a random number modulo `len`. However, in order to avoid costly operations
        // we first take it modulo a power of two, and then decrease by `len` until it fits
        // into the range `[0, len - 1]`.
        let mut other = gen_usize() & (modulus - 1);

        // `other` is guaranteed to be less than `2 * len`.
        if other >= len {
            other -= len;
        }

        v.swap(pos - 1 + i, other);
    }
}*/

/*fn break_xoroshiro_alpha<T>(v: &mut [T]) {
    let len = v.len();
    if len < 8 {
        return;
    }

    // Pseudorandom number generator from the "Xorshift RNGs" paper by George Marsaglia.
    let mut first = len as u64;
    let mut second = {
        let mut z = first + 0x9E37_79B9_7F4A_7C15_u64;
        first = z;
        z = (z ^ (z >> 30)) * 0xBF58_476D_1CE4_E5B9_u64;
        z = (z ^ (z >> 27)) * 0x94D0_49BB_1331_11EB_u64;
        z ^ (z >> 31)
    };

    // Take random numbers modulo this number.
    // The number fits into `usize` because `len` is not greater than `isize::MAX`.
    let modulus = len.next_power_of_two();

    // Some pivot candidates will be in the nearby of this index. Let's randomize them.
    let pos = len / 4 * 2;

    for i in 0..2 {
        let mut gen_u64 = || {
            #[inline]
            fn rotl(x: u64, k: i32) -> u64 {
                (x << k) | (x >> (64 - k))
            }

            let s0 = first;
            let mut s1 = second;
            let result = s0 + s1;

            s1 ^= s0;
            first = rotl(s0, 55) ^ s1 ^ (s1 << 14);
            second = rotl(s1, 36);

            result
        };

        let mut gen_usize = || {
            if mem::size_of::<usize>() == 4 {
                gen_u64() as u32 as usize
            } else {
                gen_u64() as usize
            }
        };

        // Generate a random number modulo `len`. However, in order to avoid costly operations
        // we first take it modulo a power of two, and then decrease by `len` until it fits
        // into the range `[0, len - 1]`.
        let mut other = gen_usize() & (modulus - 1);

        // `other` is guaranteed to be less than `2 * len`.
        if other >= len {
            other -= len;
        }

        v.swap(pos - 1 + i, other);
    }
    let next = first + second;
    let next_usize = if mem::size_of::<usize>() == 4 {
        next as u32 as usize
    } else {
        next as usize
    };
    let mut other = next_usize & (modulus - 1);
    if other >= len {
        other -= len;
    }
    v.swap(pos - 1 + 2, other);
}

// #[cold]
fn break_xoroshiro_pure<T>(v: &mut [T]) {
    use self::std::num::Wrapping as w;

    let len = v.len();
    if len >= 8 {
        // Pseudorandom number generator from the "Xorshift RNGs" paper by George Marsaglia.
        let mut first = len as u64;
        let mut second = len as u64;

        let mut gen_u64 = || {
            #[inline]
            fn rotl(x: u64, k: i32) -> u64 {
                (x << k) | (x >> (64 - k))
            }

            let s0 = w(first);
            let mut s1 = w(second);
            let result = s0 + s1;

            s1 ^= s0;
            first = (w(s0.0.rotate_left(55)) ^ s1 ^ (s1 << 14)).0;
            second = s1.0.rotate_left(36);

            result.0
        };

        let mut gen_usize = || {
            if mem::size_of::<usize>() == 4 {
                gen_u64() as u32 as usize
            } else {
                gen_u64() as usize
            }
        };

        // Take random numbers modulo this number.
        // The number fits into `usize` because `len` is not greater than `isize::MAX`.
        let modulus = len.next_power_of_two();

        // Some pivot candidates will be in the nearby of this index. Let's randomize them.
        let pos = len / 4 * 2;

        for i in 0..3 {
            // Generate a random number modulo `len`. However, in order to avoid costly operations
            // we first take it modulo a power of two, and then decrease by `len` until it fits
            // into the range `[0, len - 1]`.
            let mut other = gen_usize() & (modulus - 1);

            // `other` is guaranteed to be less than `2 * len`.
            if other >= len {
                other -= len;
            }

            v.swap(pos - 1 + i, other);
        }
    }
}

fn break_xorshift_star<T>(v: &mut [T]) {
    let len = v.len();
    if len < 8 {
        return;
    }

    // Pseudorandom number generator from the "Xorshift RNGs" paper by George Marsaglia.
    let mut random = len as u64;

    let mut gen_u64 = || {
        random ^= random >> 12; // a
        random ^= random << 25; // b
        random ^= random >> 27; // c
        random * 0x2545_F491_4F6C_DD1D
    };

    let mut gen_usize = || {
        if mem::size_of::<usize>() == 4 {
            gen_u64() as u32 as usize
        } else {
            gen_u64() as usize
        }
    };

    // Take random numbers modulo this number.
    // The number fits into `usize` because `len` is not greater than `isize::MAX`.
    let modulus = len.next_power_of_two();

    // Some pivot candidates will be in the nearby of this index. Let's randomize them.
    let pos = len / 4 * 2;

    for i in 0..3 {
        // Generate a random number modulo `len`. However, in order to avoid costly operations
        // we first take it modulo a power of two, and then decrease by `len` until it fits
        // into the range `[0, len - 1]`.
        let mut other = gen_usize() & (modulus - 1);

        // `other` is guaranteed to be less than `2 * len`.
        if other >= len {
            other -= len;
        }

        v.swap(pos - 1 + i, other);
    }
}

fn break_xorshift_plus<T>(v: &mut [T]) {
    let len = v.len();
    if len < 8 {
        return;
    }

    // Pseudorandom number generator from the "Xorshift RNGs" paper by George Marsaglia.
    let mut random = len as u64;

    let mut gen_u64 = || {
        random ^= random >> 12; // a
        random ^= random << 25; // b
        random ^= random >> 27; // c
        random * 0x2545_F491_4F6C_DD1D
    };

    let mut gen_usize = || {
        if mem::size_of::<usize>() == 4 {
            gen_u64() as u32 as usize
        } else {
            gen_u64() as usize
        }
    };

    // Take random numbers modulo this number.
    // The number fits into `usize` because `len` is not greater than `isize::MAX`.
    let modulus = len.next_power_of_two();

    // Some pivot candidates will be in the nearby of this index. Let's randomize them.
    let pos = len / 4 * 2;

    for i in 0..3 {
        // Generate a random number modulo `len`. However, in order to avoid costly operations
        // we first take it modulo a power of two, and then decrease by `len` until it fits
        // into the range `[0, len - 1]`.
        let mut other = gen_usize() & (modulus - 1);

        // `other` is guaranteed to be less than `2 * len`.
        if other >= len {
            other -= len;
        }

        v.swap(pos - 1 + i, other);
    }
}

// #[cold]
fn break_splitmix<T>(v: &mut [T]) {
    use std::num::Wrapping as w;

    let len = v.len();
    if len >= 8 {
        // Pseudorandom number generator from the "Xorshift RNGs" paper by George Marsaglia.
        let mut random = len as u64;

        let mut gen_u64 = || {
            let mut z = w(random) + w(0x9E37_79B9_7F4A_7C15_u64);
            random = z.0;
            z = (z ^ (z >> 30)) * w(0xBF58_476D_1CE4_E5B9_u64);
            z = (z ^ (z >> 27)) * w(0x94D0_49BB_1331_11EB_u64);
            (z ^ (z >> 31)).0
        };

        let mut gen_usize = || {
            if mem::size_of::<usize>() == 4 {
                gen_u64() as u32 as usize
            } else {
                gen_u64() as usize
            }
        };

        // Take random numbers modulo this number.
        // The number fits into `usize` because `len` is not greater than `isize::MAX`.
        let modulus = len.next_power_of_two();

        // Some pivot candidates will be in the nearby of this index. Let's randomize them.
        let pos = len / 4 * 2;

        for i in 0..3 {
            // Generate a random number modulo `len`. However, in order to avoid costly operations
            // we first take it moduluso a power of two, and then decrease by `len` until it fits
            // into the range `[0, len - 1]`.
            let mut other = gen_usize() & (modulus - 1);

            // `other` is guaranteed to be less than `2 * len`.
            if other >= len {
                other -= len;
            }

            v.swap(pos - 1 + i, other);
        }
    }
}*/

/// Chooses a pivot in `v` and returns the index and `true` if the slice is likely already sorted.
///
/// Elements in `v` might be reordered in the process.
fn choose_pivot<T, F>(v: &mut [T], is_less: &mut F) -> (usize, bool)
    where F: FnMut(&T, &T) -> bool
{
    // Minimum length to choose the median-of-medians method.
    // Shorter slices use the simple median-of-three method.
    const SHORTEST_MEDIAN_OF_MEDIANS: usize = 50;
    // Maximum number of swaps that can be performed in this function.
    const MAX_SWAPS: usize = 4 * 3;

    let len = v.len();

    // Three indices near which we are going to choose a pivot.
    let mut a = len / 4/* * 1*/;
    let mut b = len / 4 * 2;
    let mut c = len / 4 * 3;

    // Counts the total number of swaps we are about to perform while sorting indices.
    let mut swaps = 0;

    if len >= 8 {
        // Swaps indices so that `v[a] <= v[b]`.
        let mut sort2 = |a: &mut usize, b: &mut usize| unsafe {
            if is_less(v.get_unchecked(*b), v.get_unchecked(*a)) {
                ptr::swap(a, b);
                swaps += 1;
            }
        };

        // Swaps indices so that `v[a] <= v[b] <= v[c]`.
        let mut sort3 = |a: &mut usize, b: &mut usize, c: &mut usize| {
            sort2(a, b);
            sort2(b, c);
            sort2(a, b);
        };

        if len >= SHORTEST_MEDIAN_OF_MEDIANS {
            // Finds the median of `v[a - 1], v[a], v[a + 1]` and stores the index into `a`.
            let mut sort_adjacent = |a: &mut usize| {
                let tmp = *a;
                sort3(&mut (tmp - 1), a, &mut (tmp + 1));
            };

            // Find medians in the neighborhoods of `a`, `b`, and `c`.
            sort_adjacent(&mut a);
            sort_adjacent(&mut b);
            sort_adjacent(&mut c);
        }

        // Find the median among `a`, `b`, and `c`.
        sort3(&mut a, &mut b, &mut c);
    }

    if swaps < MAX_SWAPS {
        (b, swaps == 0)
    } else {
        // The maximum number of swaps was performed. Chances are the slice is descending or mostly
        // descending, so reversing will probably help sort it faster.
        v.reverse();
        (len - 1 - b, true)
    }
}

/// Sorts `v` recursively.
///
/// If the slice had a predecessor in the original array, it is specified as `pred`.
///
/// `limit` is the number of allowed imbalanced partitions before switching to `heapsort`. If zero,
/// this function will immediately switch to heapsort.
fn recurse<'a, T, F>(mut v: &'a mut [T], mut k: usize, is_less: &mut F/*, mut pred: Option<&'a T>*//*b*/)
    where F: FnMut(&T, &T) -> bool,
{
    // Slices of up to this length get sorted using insertion sort.
    const MAX_INSERTION: usize = 20;

    // `limit` is the number of allowed imbalanced partitions before switching to `heapsort`. If zero,
    // this function will immediately switch to heapsort.
    let mut limit = mem::size_of::<usize>() * 8 - v.len().leading_zeros() as usize;

    // True if the last partitioning was reasonably balanced.
    let mut was_balanced = true;
    // True if the last partitioning didn't shuffle elements (the slice was already partitioned).
    let mut was_partitioned = true;

    let mut pred = None;

    loop {
        let len = v.len();

        // Very short slices get sorted using insertion sort.
        if len <= MAX_INSERTION {
            insertion_sort(v, is_less);
            return;
        }
        // if len <= 1 { return }

        // println!("v {:?}", v);
        // println!("k {:?}", k);

        // If too many bad pivot choices were made, simply fall back to heapsort in order to
        // guarantee `O(n log n)` worst-case.
        if limit == 0 {
            heapsort(v, is_less);
            return;
        }

        // If the last partitioning was imbalanced, try breaking patterns in the slice by shuffling
        // some elements around. Hopefully we'll choose a better pivot this time.
        if !was_balanced {
            // println!("break");
            break_patterns(v);
            limit -= 1;
        }

        // Choose a pivot and try guessing whether the slice is already sorted.
        let (pivot, likely_sorted) = choose_pivot(v, is_less);

        // println!("pivot {:?}, val: {:?}", pivot, v[pivot]);

        // If the last partitioning was decently balanced and didn't shuffle elements, and if pivot
        // selection predicts the slice is likely already sorted...
        if was_balanced && was_partitioned && likely_sorted {
            // Try identifying several out-of-order elements and shifting them to correct
            // positions. If the slice ends up being completely sorted, we're done.
            // println!("partial");
            if partial_insertion_sort(v, is_less) {
                return;
            }
        }

        // If the chosen pivot is equal to the predecessor, then it's the smallest element in the
        // slice. Partition the slice into elements equal to and elements greater than the pivot.
        // This case is usually hit when the slice contains many duplicate elements.
        if let Some(p) = pred {
            if !is_less(p, &v[pivot]) {
                // println!("equal");
                let mid = partition_equal(v, pivot, is_less);
                // mid is where the greaters start
                // If there are more than k items smaller than the pivot and
                // they are partitioned, we can exit.
                // unsure about this

                // `mid` items are smaller than v[mid] and are to the left
                // if `k` is smaller than `mid`, we have partitioned enough items
                // now just
                if mid > k {
                    return;
                }

                // Continue sorting elements greater than the pivot.
                v = &mut {v}[mid..];
                k -= mid;
                continue;
            }
        }

        // Partition the slice.
        let (mid, was_p) = partition(v, pivot, is_less);

        // println!("partition {:?}", v);
        // println!("mid {:?}", mid);

        // If the pivot is at `k`, then the `k` smallest items are properly partitioned.  Unsure
        // if this means that v[k] is in its final sorted position.  If that assumption is true, we
        // are done.
        if mid == k {
            return;
        }

        was_balanced = cmp::min(mid, len - mid) >= len / 8;
        was_partitioned = was_p;

        // Split the slice into `left`, `pivot`, and `right`.
        let (left, right) = {v}.split_at_mut(mid);
        let (pivot, right) = right.split_at_mut(1);
        let pivot = &pivot[0];

        // If k is to the right of the partition, partition the right
        if mid < k {
            // println!("right");
            // let (_, right) = {v}.split_at_mut(mid);
            // let (pivot, right) = right.split_first_mut().unwrap();
            pred = Some(pivot);
            v = right;
            k -= mid + 1; // select the `k - mid` smallest items of the new `v`
        } else { // Otherwise k is to the left of the partition, partition the left
            // println!("left");
            // let (left, _) = {v}.split_at_mut(mid);
            v = left;
        }
    }
}

/// Sorts `v` using pattern-defeating quickselect, which is `O(n log n)` worst-case.
fn quickselect<T, F>(v: &mut [T], k: usize, mut is_less: F)
    where F: FnMut(&T, &T) -> bool,
{
    // Sorting has no meaningful behavior on zero-sized types.
    if mem::size_of::<T>() == 0 {
        return;
    }

    // TODO: impl the cheaper `iter::min` function when k is 1

    recurse(v, k, &mut is_less);
}

/// Partially sorts a slice and puts the `k`th smallest item in place.
///
/// This sort is in-place, unstable, and `O(n log n)` worst-case.
///
/// The implementation is based on Orson Peters' pattern-defeating quickselect.
///
/// # Examples
///
/// ```
/// let mut v = [-5, 4, 1, -3, 2];
/// let k = 2;
/// pdqselect::select(&mut v, k);
/// assert!(v[..k].iter().all(|&x| x <= v[k]));
/// assert!(v[k+1..].iter().all(|&x| x >= v[k]));
/// ```
pub fn select<T>(v: &mut [T], k: usize)
    where T: Ord,
{
    quickselect(v, k, |a, b| a.lt(b));
}

/// Partially sorts a slice using `compare` to compare elements and puts the `k`th smallest
/// item in place.
///
/// This sort is in-place, unstable, and `O(n log n)` worst-case.
///
/// The implementation is based on Orson Peters' pattern-defeating quickselect.
///
/// # Examples
///
/// ```
/// let mut v = [5, 4, 1, 3, 2];
/// let k = 2;
/// pdqselect::select_by(&mut v, k, |a, b| a.cmp(b));
/// assert!(v[..k].iter().all(|&x| x <= v[k]));
/// assert!(v[k+1..].iter().all(|&x| x >= v[k]));
///
/// // reverse sorting
/// pdqselect::select_by(&mut v, k, |a, b| b.cmp(a));
/// assert!(v[..k].iter().all(|&x| x >= v[k]));
/// assert!(v[k+1..].iter().all(|&x| x <= v[k]));
/// ```
pub fn select_by<T, F>(v: &mut [T], k: usize, mut compare: F)
    where F: FnMut(&T, &T) -> Ordering,
{
    quickselect(v, k, |a, b| compare(a, b) == Ordering::Less);
}

/// Partially sorts a slice using `f` to extract a key to compare elements by and puts the `k`th
/// smallest item in place.
///
/// This sort is in-place, unstable, and `O(n log n)` worst-case.
///
/// The implementation is based on Orson Peters' pattern-defeating quicksort.
///
/// # Examples
///
/// ```
/// let mut v = [-5i32, 4, 1, -3, 2];
/// let k = 3;
/// pdqselect::select_by_key(&mut v, k, |x| x.abs());
/// assert!(v[..k].iter().all(|&x| x.abs() <= v[k].abs()));
/// assert!(v[k+1..].iter().all(|&x| x.abs() >= v[k].abs()));
/// ```
pub fn select_by_key<T, B, F>(v: &mut [T], k: usize, mut f: F)
    where F: FnMut(&T) -> B,
          B: Ord,
{
    quickselect(v, k, |a, b| f(a).lt(&f(b)));
}