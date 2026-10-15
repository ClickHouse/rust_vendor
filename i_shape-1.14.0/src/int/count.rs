use alloc::vec::Vec;
use crate::int::path::IntPath;
use crate::int::shape::IntShape;

pub type IntShapes = Vec<IntShape>;

pub trait PointsCount {
    fn points_count(&self) -> usize;
}

impl PointsCount for [IntPath] {
    #[inline(always)]
    fn points_count(&self) -> usize {
        self.iter().fold(0, |acc, path| acc + path.len())
    }
}

impl PointsCount for [IntShape] {
    #[inline(always)]
    fn points_count(&self) -> usize {
        self.iter().fold(0, |acc, shape| acc + shape.points_count())
    }
}

pub trait BiggestShapePoints {
    fn points_in_biggest_shape(&self) -> usize;
}

impl BiggestShapePoints for [IntShape] {
    #[inline(always)]
    fn points_in_biggest_shape(&self) -> usize {
        self.iter()
            .map(|shape| shape.points_count())
            .max()
            .unwrap_or(0)
    }
}