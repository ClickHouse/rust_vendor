use i_float::adapter::FloatPointAdapter;
use i_float::float::compatible::FloatPointCompatible;
use i_float::float::number::FloatNumber;

pub trait IntArea<P: FloatPointCompatible<T>, T: FloatNumber> {
    /// The area of the `Path`.
    /// - Returns: A positive double area if path is clockwise and negative double area otherwise.
    fn unsafe_int_area(&self, adapter: &FloatPointAdapter<P, T>) -> i64;
}

impl<P: FloatPointCompatible<T>, T: FloatNumber> IntArea<P, T> for [P] {
    fn unsafe_int_area(&self, adapter: &FloatPointAdapter<P, T>) -> i64 {
        let n = self.len();
        let mut p0 = adapter.float_to_int(&self[n - 1]);
        let mut area: i64 = 0;

        for pi in self.iter() {
            let p1 = adapter.float_to_int(pi);
            let a = (p1.x as i64).wrapping_mul(p0.y as i64);
            let b = (p1.y as i64).wrapping_mul(p0.x as i64);
            area = area.wrapping_add(a).wrapping_sub(b);
            p0 = p1;
        }

        area
    }
}