use i_float::float::compatible::FloatPointCompatible;
use i_float::float::number::FloatNumber;
use crate::base::data::{Contour, Shape};

pub trait Area<P: FloatPointCompatible<T>, T: FloatNumber> {
    fn area(&self) -> T;
}

impl<P: FloatPointCompatible<T>, T: FloatNumber> Area<P, T> for [P] {

    #[inline]
    fn area(&self) -> T {
        let mut area = T::from_float(0.0);

        let mut a = if let Some(p) = self.last() {
            *p
        } else {
            return area;
        };

        for &b in self.iter() {
            let ab = a.x() * b.y() - b.x() * a.y();
            area = area + ab;
            a = b;
        }

        T::from_float(0.5) * area
    }
}

impl<P: FloatPointCompatible<T>, T: FloatNumber> Area<P, T> for [Contour<P>] {
    #[inline]
    fn area(&self) -> T {
        let mut area = T::from_float(0.0);
        for contour in self.iter() {
            area = area + contour.area();
        }
        area
    }
}

impl<P: FloatPointCompatible<T>, T: FloatNumber> Area<P, T> for [Shape<P>] {
    #[inline]
    fn area(&self) -> T {
        let mut area = T::from_float(0.0);
        for shape in self.iter() {
            area = area + shape.area();
        }
        area
    }
}