use i_float::float::compatible::FloatPointCompatible;
use i_float::float::number::FloatNumber;
use crate::base::data::{Contour, Shape};

pub trait PointsCount<P, T> {
    fn points_count(&self) -> usize;
}

impl<P, T> PointsCount<P, T> for [Contour<P>]
where
    P: FloatPointCompatible<T>,
    T: FloatNumber,
{
    fn points_count(&self) -> usize {
        self.iter().fold(0, |acc, list| acc + list.len())
    }
}

impl<P, T> PointsCount<P, T> for [Shape<P>]
where
    P: FloatPointCompatible<T>,
    T: FloatNumber,
{
    fn points_count(&self) -> usize {
        self.iter().fold(0, |acc, lists| acc + lists.points_count())
    }
}