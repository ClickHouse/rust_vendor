use crate::flat::float::FloatFlatContoursBuffer;
use crate::source::resource::ShapeResource;
use i_float::float::compatible::FloatPointCompatible;
use i_float::float::number::FloatNumber;

pub struct FloatContoursBufferResourceIterator<'a, P> {
    buffer: &'a FloatFlatContoursBuffer<P>,
    index: usize,
}

impl<'a, P> FloatContoursBufferResourceIterator<'a, P> {
    #[inline]
    fn with_buffer(buffer: &'a FloatFlatContoursBuffer<P>) -> Self {
        Self { buffer, index: 0 }
    }
}

impl<'a, P> Iterator for FloatContoursBufferResourceIterator<'a, P> {
    type Item = &'a [P];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        while self.index < self.buffer.ranges.len() {
            let i = self.index;
            self.index += 1;
            if let Some(contour) = self.buffer.contour_pairs_at(i) {
                return Some(contour);
            }
        }

        None
    }
}

impl<P: FloatPointCompatible<T>, T: FloatNumber> ShapeResource<P, T> for FloatFlatContoursBuffer<P> {
    type ResourceIter<'a>
        = FloatContoursBufferResourceIterator<'a, P>
    where
        Self: 'a;

    #[inline]
    fn iter_paths(&self) -> Self::ResourceIter<'_> {
        FloatContoursBufferResourceIterator::with_buffer(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;
    use alloc::vec::Vec;
    use core::ops::Range;

    #[test]
    fn test_iter_paths() {
        let mut buffer = FloatFlatContoursBuffer::<[f64; 2]>::default();
        let points = [
            [0.0, 0.0],
            [2.0, 0.0],
            [2.0, 2.0],
            [10.0, 10.0],
            [11.0, 10.0],
            [11.0, 11.0],
        ];
        let ranges: [Range<usize>; 2] = [0..3, 3..6];
        buffer.set_flat(&points, &ranges);

        let mut iter = buffer.iter_paths();

        let c0 = iter.next().unwrap();
        assert_eq!(c0.len(), 3);
        assert_eq!(c0[0], [0.0, 0.0]);
        assert_eq!(c0[2], [2.0, 2.0]);

        let c1 = iter.next().unwrap();
        assert_eq!(c1.len(), 3);
        assert_eq!(c1[0], [10.0, 10.0]);
        assert_eq!(c1[2], [11.0, 11.0]);

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_set_with_resource() {
        let shape: Vec<Vec<[f64; 2]>> = vec![
            vec![[0.0, 0.0], [2.0, 0.0], [2.0, 2.0]],
            vec![[1.0, 1.0], [1.5, 1.0], [1.5, 1.5]],
        ];

        let mut buffer = FloatFlatContoursBuffer::<[f64; 2]>::default();
        buffer.set_with_resource(&shape);

        assert_eq!(buffer.ranges.len(), 2);
        let contours = buffer.to_contours();
        assert_eq!(contours, shape);
    }
}
