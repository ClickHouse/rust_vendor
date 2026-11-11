use crate::int::shape::IntContour;

/// Trait for removing redundant points from a contour.
pub trait DedupContour {
    /// Removes consecutive duplicate points and a duplicated closing point
    /// (if the last point is equal to the first).
    ///
    /// Returns `true` if the contour was modified, `false` otherwise.
    fn dedup_contour(&mut self) -> bool;
}

impl DedupContour for IntContour {
    fn dedup_contour(&mut self) -> bool {
        let n = self.len();
        self.dedup();

        if let (Some(&first), Some(&last)) = (self.first(), self.last()) {
            if last == first {
                self.pop();
            }
        }

        self.len() < n
    }
}

#[cfg(test)]
mod tests {
    use alloc::vec;
    use i_float::int::point::IntPoint;
    use crate::int::dedup::DedupContour;

    #[test]
    fn test_0() {
        let mut contour =
            vec![
                    IntPoint::new(0, 0),
                    IntPoint::new(1, 0),
            ];

        let modified = contour.dedup_contour();

        assert_eq!(contour.len(), 2);
        assert_eq!(modified, false);
    }

    #[test]
    fn test_1() {
        let mut contour =
            vec![
                IntPoint::new(0, 0),
                IntPoint::new(1, 0),
                IntPoint::new(0, 0),
            ];

        let modified = contour.dedup_contour();

        assert_eq!(contour.len(), 2);
        assert_eq!(modified, true);
    }

    #[test]
    fn test_2() {
        let mut contour =
            vec![
                IntPoint::new(0, 0),
                IntPoint::new(0, 0),
                IntPoint::new(1, 0),
            ];

        let modified = contour.dedup_contour();

        assert_eq!(contour.len(), 2);
        assert_eq!(modified, true);
    }
}