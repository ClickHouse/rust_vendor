use crate::int::shape::{IntShape, IntShapes};

pub trait IntContourReverse {
    fn reverse_contours(&mut self);
}

impl IntContourReverse for IntShape {
    #[inline]
    fn reverse_contours(&mut self) {
        for path in self {
            path.reverse()
        }
    }
}

impl IntContourReverse for IntShapes {
    #[inline]
    fn reverse_contours(&mut self) {
        for shape in self {
            for path in shape {
                path.reverse()
            }
        }
    }
}
