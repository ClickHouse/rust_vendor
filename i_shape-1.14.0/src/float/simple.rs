use i_float::adapter::FloatPointAdapter;
use i_float::float::compatible::FloatPointCompatible;
use i_float::float::number::FloatNumber;
use crate::base::data::{Contour, Shape, Shapes};
use crate::float::adapter::{PathToFloat, PathToInt, ShapeToFloat, ShapeToInt, ShapesToFloat, ShapesToInt};
use crate::int::simple::Simplify as IntSimplify;

/// A trait that provides methods for simplifying complex geometrical structures.
pub trait SimplifyContour<P: FloatPointCompatible<T>, T: FloatNumber> {
    /// Simplifies the structure in-place if it is not already simple.
    ///
    /// # Returns
    ///
    /// - `true` if the structure was simplified successfully.
    /// - `false` if the structure was already simple and no modification was made.
    fn simplify_contour(&mut self, adapter: &FloatPointAdapter<P, T>) -> bool;
}

impl<P: FloatPointCompatible<T>, T: FloatNumber> SimplifyContour<P, T> for Contour<P> {
    fn simplify_contour(&mut self, adapter: &FloatPointAdapter<P, T>) -> bool {
        let mut int_contour = self.to_int(adapter);
        if !int_contour.simplify_contour() { return false; }

        if int_contour.is_empty() {
            self.clear();
        } else {
            *self = int_contour.to_float(adapter);
        }
        true
    }
}

impl<P: FloatPointCompatible<T>, T: FloatNumber> SimplifyContour<P, T> for Shape<P> {
    fn simplify_contour(&mut self, adapter: &FloatPointAdapter<P, T>) -> bool {
        let mut int_shape = self.to_int(adapter);
        if !int_shape.simplify_contour() { return false; }

        if int_shape.is_empty() {
            self.clear();
        } else {
            *self = int_shape.to_float(adapter);
        }
        true
    }
}

impl<P: FloatPointCompatible<T>, T: FloatNumber> SimplifyContour<P, T> for Shapes<P> {
    fn simplify_contour(&mut self, adapter: &FloatPointAdapter<P, T>) -> bool {
        let mut int_shapes = self.to_int(adapter);
        if !int_shapes.simplify_contour() { return false; }

        if int_shapes.is_empty() {
            self.clear();
        } else {
            *self = int_shapes.to_float(adapter);
        }
        true
    }
}