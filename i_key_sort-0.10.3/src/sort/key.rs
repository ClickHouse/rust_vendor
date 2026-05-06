use core::cmp::Ordering;

pub trait KeyFn<T, K>: Fn(&T) -> K + Copy {}
impl<T, K, F: Fn(&T) -> K + Copy> KeyFn<T, K> for F {}

pub trait CmpFn<T>: Fn(&T, &T) -> Ordering + Copy {}
impl<T, F: Fn(&T, &T) -> Ordering + Copy> CmpFn<T> for F {}

pub trait SortKey: Copy + Ord {
    fn difference(self, other: Self) -> usize;
}

impl SortKey for u8 {
    #[inline(always)]
    fn difference(self, other: Self) -> usize {
        debug_assert!(self >= other, "difference() requires self >= other");
        (self - other) as usize
    }
}

impl SortKey for i8 {
    #[inline(always)]
    fn difference(self, other: Self) -> usize {
        debug_assert!(self >= other, "difference() requires self >= other");
        (self - other) as usize
    }
}

impl SortKey for u16 {
    #[inline(always)]
    fn difference(self, other: Self) -> usize {
        debug_assert!(self >= other, "difference() requires self >= other");
        (self - other) as usize
    }
}

impl SortKey for i16 {
    #[inline(always)]
    fn difference(self, other: Self) -> usize {
        debug_assert!(self >= other, "difference() requires self >= other");
        (self - other) as usize
    }
}

impl SortKey for u32 {
    #[inline(always)]
    fn difference(self, other: Self) -> usize {
        debug_assert!(self >= other, "difference() requires self >= other");
        (self - other) as usize
    }
}

impl SortKey for i32 {
    #[inline(always)]
    fn difference(self, other: Self) -> usize {
        debug_assert!(self >= other, "difference() requires self >= other");
        (self - other) as usize
    }
}

impl SortKey for u64 {
    #[inline(always)]
    fn difference(self, other: Self) -> usize {
        debug_assert!(self >= other, "difference() requires self >= other");
        (self - other) as usize
    }
}

impl SortKey for i64 {
    #[inline(always)]
    fn difference(self, other: Self) -> usize {
        debug_assert!(self >= other, "difference() requires self >= other");
        (self - other) as usize
    }
}

impl SortKey for usize {
    #[inline(always)]
    fn difference(self, other: Self) -> usize {
        debug_assert!(self >= other, "difference() requires self >= other");
        self - other
    }
}
