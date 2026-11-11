use i_float::adapter::FloatPointAdapter;
use i_float::float::compatible::FloatPointCompatible;
use i_float::float::number::FloatNumber;
use crate::base::data::{Contour, Shape, Shapes};
use crate::float::adapter::{PathToFloat, PathToInt, ShapeToFloat, ShapeToInt, ShapesToFloat, ShapesToInt};
use crate::int::despike::DeSpike;

/// A trait that provides methods for despike complex geometrical structures.
pub trait DeSpikeContour<P: FloatPointCompatible<T>, T: FloatNumber> {
    /// Simplifies the structure in-place if it is not already simple.
    ///
    /// # Returns
    ///
    /// - `true` if the structure was simplified successfully.
    /// - `false` if the structure was already simple and no modification was made.
    fn despike_contour(&mut self, adapter: &FloatPointAdapter<P, T>) -> bool;
}

impl<P: FloatPointCompatible<T>, T: FloatNumber> DeSpikeContour<P, T> for Contour<P> {
    fn despike_contour(&mut self, adapter: &FloatPointAdapter<P, T>) -> bool {
        let mut int_contour = self.to_int(adapter);
        if !int_contour.remove_spikes() { return false; }

        if int_contour.is_empty() {
            self.clear();
        } else {
            *self = int_contour.to_float(adapter);
        }
        true
    }
}

impl<P: FloatPointCompatible<T>, T: FloatNumber> DeSpikeContour<P, T> for Shape<P> {
    fn despike_contour(&mut self, adapter: &FloatPointAdapter<P, T>) -> bool {
        let mut int_shape = self.to_int(adapter);
        if !int_shape.remove_spikes() { return false; }

        if int_shape.is_empty() {
            self.clear();
        } else {
            *self = int_shape.to_float(adapter);
        }
        true
    }
}

impl<P: FloatPointCompatible<T>, T: FloatNumber> DeSpikeContour<P, T> for Shapes<P> {
    fn despike_contour(&mut self, adapter: &FloatPointAdapter<P, T>) -> bool {
        let mut int_shapes = self.to_int(adapter);
        if !int_shapes.remove_spikes() { return false; }

        if int_shapes.is_empty() {
            self.clear();
        } else {
            *self = int_shapes.to_float(adapter);
        }
        true
    }
}