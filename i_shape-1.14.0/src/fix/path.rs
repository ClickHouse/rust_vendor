use alloc::vec::Vec;
use i_float::fix_vec::FixVec;

pub type FixPath = Vec<FixVec>;

pub trait FixPathExtension {
    fn area_x2(&self) -> i64;
    fn is_convex(&self) -> bool;
    fn is_clockwise_ordered(&self) -> bool;
}

impl FixPathExtension for FixPath {
    fn area_x2(&self) -> i64 {
        let n = self.len();
        let mut p0 = self[n - 1];
        let mut area: i64 = 0;

        for p1 in self.iter() {
            area += p1.cross_product(p0);
            p0 = *p1;
        }

        area
    }

    fn is_convex(&self) -> bool {
        let n = self.len();
        if n <= 2 {
            return true;
        }

        let p0 = self[n - 2];
        let mut p1 = self[n - 1];
        let mut e0 = p1 - p0;

        let mut sign: i64 = 0;
        for p in self.iter() {
            let p2 = *p;
            let e1 = p2 - p1;
            let cross = e1.cross_product(e0).signum();
            if cross == 0 {
                let dot = e1.dot_product(e0);
                if dot == -1 {
                    return false;
                }
            } else if sign == 0 {
                sign = cross
            } else if sign != cross {
                return false;
            }

            e0 = e1;
            p1 = p2;
        }

        true
    }

    #[inline(always)]
    fn is_clockwise_ordered(&self) -> bool {
        self.area_x2() >= 0
    }
}