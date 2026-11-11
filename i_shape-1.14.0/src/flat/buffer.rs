use alloc::vec::Vec;
use core::ops::Range;
use i_float::int::point::IntPoint;
use crate::int::count::PointsCount;
use crate::int::shape::{IntContour, IntShape,};
use crate::util::reserve::Reserve;

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Default)]
pub struct FlatContoursBuffer {
    pub points: Vec<IntPoint>,
    pub ranges: Vec<Range<usize>>,
}

impl FlatContoursBuffer {

    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            points: Vec::with_capacity(capacity),
            ranges: Vec::new(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.points.is_empty()
    }

    #[inline]
    pub fn is_single_contour(&self) -> bool {
        self.ranges.len() == 1
    }

    #[inline]
    pub fn as_first_contour(&self) -> &[IntPoint] {
        if let Some(first_contour_range) = self.ranges.first() {
            &self.points[first_contour_range.clone()]
        } else {
            &self.points
        }
    }

    #[inline]
    pub fn as_first_contour_mut(&mut self) -> &mut [IntPoint] {
        if let Some(first_contour_range) = self.ranges.first() {
            &mut self.points[first_contour_range.clone()]
        } else {
            &mut self.points
        }
    }

    #[inline]
    pub fn set_with_contour(&mut self, contour: &[IntPoint]) {
        let points_len = contour.len();
        self.clear_and_reserve(points_len, 1);

        self.points.extend_from_slice(contour);
        self.ranges.push(0..points_len);
    }

    #[inline]
    pub fn set_with_shape(&mut self, shape: &[IntContour]) {
        let points_len = shape.points_count();
        let contours_len = shape.len();
        self.clear_and_reserve(points_len, contours_len);

        let mut offset = 0;
        for contour in shape.iter() {
            let len = contour.len();
            self.points.extend_from_slice(contour);
            self.ranges.push(offset..offset + len);
            offset += len;
        }
    }

    #[inline]
    pub fn set_with_shapes(&mut self, shapes: &[IntShape]) {
        let points_len = shapes.points_count();
        let contours_len = shapes.iter().map(Vec::len).sum();
        self.clear_and_reserve(points_len, contours_len);

        let mut points_offset = 0;
        for shape in shapes.iter() {
            for contour in shape.iter() {
                let len = contour.len();
                self.points.extend_from_slice(contour);
                self.ranges.push(points_offset..points_offset + len);
                points_offset += len;
            }
        }
    }

    #[inline]
    pub fn clear_and_reserve(&mut self, points: usize, contours: usize) {
        self.points.reserve_capacity(points);
        self.points.clear();

        self.ranges.reserve_capacity(contours);
        self.ranges.clear();
    }

    #[inline]
    pub fn add_contour(&mut self, contour: &[IntPoint]) {
        let start = self.points.len();
        let end = start + contour.len();
        self.ranges.push(start..end);
        self.points.extend_from_slice(contour);
    }

    #[inline]
    pub fn to_contours(&self) -> Vec<IntContour> {
        let mut contours = Vec::with_capacity(self.ranges.len());

        for range in self.ranges.iter() {
            contours.push(self.points[range.clone()].to_vec());
        }

        contours
    }
}

#[cfg(test)]
mod tests {
    use alloc::vec;
    use i_float::int_pnt;
    use rand::Rng;
    use super::*;
    use crate::int::shape::{IntContour, IntShape, IntShapes};

    fn make_contour(p: &[(i32, i32)]) -> IntContour {
        p.iter().map(|&(x, y)| int_pnt!(x, y)).collect()
    }

    #[test]
    fn test_contour_flat_round_trip() {
        let contour = make_contour(&[(1, 2), (3, 4), (5, 6)]);
        let mut flat = FlatContoursBuffer::with_capacity(0);
        flat.set_with_contour(&contour);

        let contours = flat.to_contours();
        assert_eq!(contours.len(), 1);
        assert_eq!(contours[0], contour);
    }

    #[test]
    fn test_shape_flat_round_trip() {
        let shape = vec![
            make_contour(&[(0, 0), (1, 0), (1, 1), (0, 1)]),
            make_contour(&[(2, 2), (3, 2), (3, 3), (2, 3)]),
        ];
        let mut flat = FlatContoursBuffer::with_capacity(0);
        flat.set_with_shape(&shape);

        let contours = flat.to_contours();
        assert_eq!(contours.len(), 2);
        assert_eq!(contours[0].len(), 4);
        assert_eq!(contours[1].len(), 4);
    }

    #[test]
    fn test_shapes_flat_round_trip() {
        let shapes = vec![
            vec![make_contour(&[(0, 0), (1, 0), (1, 1)])],
            vec![
                make_contour(&[(5, 5), (6, 5), (6, 6)]),
                make_contour(&[(7, 7), (8, 7), (8, 8)]),
            ],
        ];
        let mut flat = FlatContoursBuffer::with_capacity(0);
        flat.set_with_shapes(&shapes);

        let contours = flat.to_contours();
        assert_eq!(contours.len(), 3);
        assert_eq!(contours[0].len(), 3);
        assert_eq!(contours[1].len(), 3);
        assert_eq!(contours[2].len(), 3);
    }

    #[test]
    fn test_random_shapes_round_trip() {
        let mut rng = rand::rng();
        let mut shapes: IntShapes = Vec::new();

        for _ in 0..5 {
            let mut shape: IntShape = Vec::new();
            let contour_count = rng.random_range(1..4);
            for _ in 0..contour_count {
                let mut contour: IntContour = Vec::new();
                let point_count = rng.random_range(3..7);
                for _ in 0..point_count {
                    let x = rng.random_range(-100..100);
                    let y = rng.random_range(-100..100);
                    contour.push(IntPoint::new(x, y));
                }
                shape.push(contour);
            }
            shapes.push(shape);
        }

        let mut flat = FlatContoursBuffer::with_capacity(0);
        flat.set_with_shapes(&shapes);
        let contours = flat.to_contours();
        assert_eq!(contours.len(), shapes.iter().fold(0, |s, shape|s + shape.len()));
    }
}
