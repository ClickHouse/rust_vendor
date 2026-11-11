use core::ops::Range;

pub trait BinLayoutOp {
    fn offset(self, other: Self) -> usize;
}

impl BinLayoutOp for i64 {
    #[inline(always)]
    fn offset(self, other: Self) -> usize {
        self.wrapping_sub(other) as usize
    }
}

impl BinLayoutOp for i32 {
    #[inline(always)]
    fn offset(self, other: Self) -> usize {
        (self as i64).wrapping_sub(other as i64) as usize
    }
}

impl BinLayoutOp for usize {
    #[inline(always)]
    fn offset(self, other: Self) -> usize {
        self.wrapping_sub(other)
    }
}

pub struct BinLayout<T> {
    pub(crate) min_key: T,
    pub(crate) max_key: T,
    pub(crate) power: usize,
}

impl<T> BinLayout<T>
where
    T: Copy + BinLayoutOp + PartialOrd,
{
    #[inline(always)]
    pub fn index(&self, value: T) -> usize {
        value.offset(self.min_key) >> self.power
    }

    #[inline(always)]
    pub fn count(&self) -> usize {
        self.index(self.max_key) + 1
    }

    #[inline(always)]
    pub fn new(range: Range<T>, elements_count: usize) -> Option<BinLayout<T>> {
        let delta = range.end.offset(range.start) + 1;
        let max_possible_bin_count = delta.min(elements_count >> 1).min(16384);
        if max_possible_bin_count <= 1 {
            return None;
        }

        let scale = delta / max_possible_bin_count;
        let scale_power = log2(scale);
        Some(Self {
            min_key: range.start,
            max_key: range.end,
            power: scale_power,
        })
    }
}

pub trait BinKey<T> {
    fn bin_key(&self) -> T;
    fn bin_index(&self, layout: &BinLayout<T>) -> usize;
}

#[inline(always)]
fn log2(value: usize) -> usize {
    let n = value.leading_zeros();
    (usize::BITS - n) as usize
}