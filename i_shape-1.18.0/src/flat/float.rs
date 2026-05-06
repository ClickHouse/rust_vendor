use crate::base::data::Contour;
use crate::source::resource::ShapeResource;
use alloc::vec::Vec;
use core::ops::Range;
use i_float::float::compatible::FloatPointCompatible;
use i_float::float::number::FloatNumber;

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Default)]
pub struct FloatFlatContoursBuffer<P> {
    pub points: Vec<P>,
    pub ranges: Vec<Range<usize>>,
}

impl<P> FloatFlatContoursBuffer<P> {
    #[inline]
    pub fn with_capacity(points: usize, contours: usize) -> Self {
        Self {
            points: Vec::with_capacity(points.saturating_mul(2)),
            ranges: Vec::with_capacity(contours),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.points.is_empty()
    }

    #[inline]
    pub fn clear_and_reserve(&mut self, points: usize, contours: usize) {
        self.points.clear();
        self.points.reserve(points);

        self.ranges.clear();
        self.ranges.reserve(contours);
    }

    #[inline]
    pub fn add_contour(&mut self, contour: &[P])
    where
        P: Clone,
    {
        let start = self.points.len();
        self.points.extend_from_slice(contour);

        self.ranges.push(start..self.points.len());
    }

    #[inline]
    pub fn add_contour_iter<I>(&mut self, contour_iter: I)
    where
        I: IntoIterator<Item = P>,
        P: Clone,
    {
        let start = self.points.len();
        let mut iter = contour_iter.into_iter();
        self.points.extend(&mut iter);
        self.ranges.push(start..self.points.len());
    }

    #[inline]
    pub fn set_with_resource<R, T>(&mut self, resource: &R)
    where
        R: ShapeResource<P, T> + ?Sized,
        P: FloatPointCompatible<T>,
        T: FloatNumber,
    {
        let mut contours_count = 0;
        let mut points_count = 0;
        for contour in resource.iter_paths() {
            contours_count += 1;
            points_count += contour.len();
        }

        self.clear_and_reserve(points_count, contours_count);
        for contour in resource.iter_paths() {
            self.add_contour(contour);
        }
    }

    #[inline]
    pub fn set_flat(&mut self, points: &[P], ranges: &[Range<usize>])
    where
        P: Clone,
    {
        self.clear_and_reserve(points.len(), ranges.len());
        self.points.extend_from_slice(points);
        self.ranges.extend_from_slice(ranges);
    }

    #[inline]
    pub fn set_with_iter<I>(&mut self, iter: I, ranges: &[Range<usize>])
    where
        I: IntoIterator<Item = P>,
    {
        let mut iter = iter.into_iter();
        let max_range_end = ranges.iter().map(|r| r.end).max().unwrap_or(0);
        let (min_points, max_points) = iter.size_hint();
        let points_capacity = max_points.unwrap_or(min_points).max(max_range_end);
        self.clear_and_reserve(points_capacity, ranges.len());
        self.points.extend(&mut iter);
        self.ranges.extend_from_slice(ranges);
    }

    #[inline]
    pub fn to_contours(&self) -> Vec<Contour<P>>
    where
        P: Clone,
    {
        let mut contours = Vec::with_capacity(self.ranges.len());

        for range in self.ranges.iter() {
            let slice = &self.points[range.clone()];
            contours.push(slice.to_vec());
        }

        contours
    }

    #[inline]
    pub(crate) fn contour_pairs_at(&self, index: usize) -> Option<&[P]> {
        let range = self.ranges.get(index)?;
        if range.start >= range.end || range.end > self.points.len() {
            return None;
        }
        Some(&self.points[range.clone()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;
    use alloc::vec::Vec;

    #[test]
    fn test_add_contour() {
        let mut buffer = FloatFlatContoursBuffer::<[f64; 2]>::default();
        let c0 = [[0.0, 0.0], [2.0, 0.0], [2.0, 2.0]];
        let c1 = [[10.0, 10.0], [11.0, 10.0], [11.0, 11.0]];

        buffer.add_contour(&c0);
        buffer.add_contour(&c1);

        assert_eq!(buffer.ranges.len(), 2);
        assert_eq!(buffer.ranges[0], 0..3);
        assert_eq!(buffer.ranges[1], 3..6);
        assert_eq!(buffer.points.len(), 6);
    }

    #[test]
    fn test_set_with_resource() {
        let shape: Vec<Vec<[f64; 2]>> = vec![
            vec![[0.0, 0.0], [2.0, 0.0], [2.0, 2.0]],
            vec![[1.0, 1.0], [1.5, 1.0], [1.5, 1.5]],
        ];

        let mut buffer = FloatFlatContoursBuffer::<[f64; 2]>::default();
        buffer.set_with_resource(&shape);

        assert_eq!(buffer.ranges, vec![0..3, 3..6]);
        assert_eq!(buffer.to_contours(), shape);
    }

    #[test]
    fn test_set_with_iter() {
        let points = [
            [0.0, 0.0],
            [2.0, 0.0],
            [2.0, 2.0],
            [10.0, 10.0],
            [11.0, 10.0],
            [11.0, 11.0],
        ];
        let ranges = [0..3, 3..6];

        let mut buffer = FloatFlatContoursBuffer::<[f64; 2]>::default();
        buffer.set_with_iter(points, &ranges);

        assert_eq!(buffer.points, points);
        assert_eq!(buffer.ranges, ranges);
    }
}
