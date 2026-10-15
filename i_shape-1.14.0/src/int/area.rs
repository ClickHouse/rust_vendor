use i_float::int::point::IntPoint;
use crate::int::path::ContourExtension;
use crate::int::shape::{IntContour, IntShape};

pub trait Area {
    fn area_two(&self) -> i64;
    fn area(&self) -> i64;
}

impl Area for [IntPoint] {

    #[inline]
    fn area_two(&self) -> i64 {
        self.unsafe_area()
    }

    #[inline]
    fn area(&self) -> i64 {
        self.area_two() / 2
    }
}

impl Area for [IntContour] {

    #[inline]
    fn area_two(&self) -> i64 {
        let mut s: i64 = 0;
        for path in self.iter() {
            s = s.wrapping_add(path.area_two())
        }
        s
    }

    #[inline]
    fn area(&self) -> i64 {
        self.area_two() / 2
    }
}

impl Area for [IntShape] {

    #[inline]
    fn area_two(&self) -> i64 {
        let mut s: i64 = 0;
        for shape in self.iter() {
            s = s.wrapping_add(shape.area_two())
        }
        s
    }

    #[inline]
    fn area(&self) -> i64 {
        self.area_two() / 2
    }
}