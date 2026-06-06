#![no_std]
extern crate alloc;

pub mod key;
pub mod seg;
pub mod set;

pub const EMPTY_REF: u32 = u32::MAX;

pub trait ExpiredKey<E: Expiration>: Copy + Ord {
    fn expiration(&self) -> E;
}

pub trait ExpiredVal<E: Expiration>: Copy {
    fn expiration(&self) -> E;
}

pub trait Expiration: Copy + Ord {
    fn max_expiration() -> Self;
}

impl Expiration for u8 {
    #[inline]
    fn max_expiration() -> Self {
        u8::MAX
    }
}

impl Expiration for i8 {
    #[inline]
    fn max_expiration() -> Self {
        i8::MAX
    }
}

impl Expiration for u16 {
    #[inline]
    fn max_expiration() -> Self {
        u16::MAX
    }
}

impl Expiration for i16 {
    #[inline]
    fn max_expiration() -> Self {
        i16::MAX
    }
}

impl Expiration for u32 {
    #[inline]
    fn max_expiration() -> Self {
        u32::MAX
    }
}

impl Expiration for i32 {
    #[inline]
    fn max_expiration() -> Self {
        i32::MAX
    }
}

impl Expiration for u64 {
    #[inline]
    fn max_expiration() -> Self {
        u64::MAX
    }
}

impl Expiration for i64 {
    #[inline]
    fn max_expiration() -> Self {
        i64::MAX
    }
}

impl Expiration for usize {
    #[inline]
    fn max_expiration() -> Self {
        usize::MAX
    }
}
