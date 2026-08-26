use crate::seg::heap::Heap32;

pub(super) struct Layout {
    min: i64,
    max: i64,
    scale: u32,
}

impl Layout {
    #[inline]
    pub(super) fn new(start: i64, end: i64) -> Option<Self> {
        let min = start;
        let max = end;
        let len = (max - min + 1) as usize;
        if len < Heap32::POWER as usize {
            return None;
        }
        let p = (len - 1).ilog2() + 1;
        if p < Heap32::POWER {
            return None;
        }
        let scale = p - Heap32::POWER;

        Some(Self { min, max, scale })
    }

    #[inline]
    pub(super) fn index(&self, value: i64) -> u32 {
        ((value - self.min) >> self.scale) as u32
    }

    #[inline]
    pub(super) fn count(&self) -> usize {
        let order = self.index(self.max);
        Heap32::order_to_heap_index(order) as usize + 1
    }

    #[inline]
    pub(super) fn insert_mask(&self, min: i64, max: i64) -> u64 {
        let start = self.index(min);
        let end = self.index(max);

        Heap32::range_to_place_mask(start, end)
    }

    #[inline]
    pub(super) fn intersect_mask(&self, min: i64, max: i64) -> u64 {
        let start = self.index(min);
        let end = self.index(max);

        Heap32::range_to_intersect_mask(start, end)
    }
}

#[cfg(test)]
mod tests {
    use crate::seg::layout::Layout;


    #[test]
    fn test_00() {
        let layout = Layout::new(0, 31).unwrap();
        for i in 0..31 {
            assert_eq!(layout.index(i), i as u32);
        }
    }

    #[test]
    fn test_01() {
        let layout = Layout::new(0, 63).unwrap();
        for i in 0..63 {
            assert_eq!(layout.index(i), (i / 2) as u32);
        }
    }

    #[test]
    fn test_02() {
        let layout = Layout::new(-63, 0).unwrap();
        for i in -63..0 {
            assert_eq!(layout.index(i), ((i + 63) / 2) as u32);
        }
    }

    #[test]
    fn test_03() {
        let layout = Layout::new(-10240, 15360).unwrap();
        let m0 = layout.insert_mask(-10240, 10240);
        let m1 = layout.intersect_mask(-10240, -10240);
        let inter = m0 & m1;

        assert_ne!(inter, 0);
    }
}