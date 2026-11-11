use i_float::adapter::FloatPointAdapter;
use i_float::float::compatible::FloatPointCompatible;
use i_float::float::number::FloatNumber;
use crate::base::data::Contour;
use crate::int::shape::IntContour;
use crate::util::reserve::Reserve;

pub trait IntContourInit<P: FloatPointCompatible<T>, T: FloatNumber> {
    fn set_with_float(&mut self, contour: &Contour<P>, adapter: &FloatPointAdapter<P, T>);
}

impl<P: FloatPointCompatible<T>, T: FloatNumber> IntContourInit<P, T> for IntContour {
    fn set_with_float(&mut self, contour: &Contour<P>, adapter: &FloatPointAdapter<P, T>) {
        self.reserve_capacity(contour.len());
        self.clear();
        for p in contour.iter() {
            self.push(adapter.float_to_int(p))
        }
    }
}