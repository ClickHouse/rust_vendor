use alloc::vec;
use alloc::vec::Vec;
use i_float::int::point::IntPoint;
use crate::int::shape::{IntContour, IntShape, IntShapes};

/// A trait that provides methods for simplifying complex geometrical structures.
pub trait Simplify {
    /// Simplifies the structure in-place if it is not already simple.
    ///
    /// # Returns
    ///
    /// - `true` if the structure was simplified successfully.
    /// - `false` if the structure was already simple and no modification was made.
    fn simplify_contour(&mut self) -> bool;
}

pub trait Simplified {
    /// Simplified the structure if it is not already simple.
    fn simplified_contour(&self) -> Option<IntContour>;
}

/// A trait for determining if a contour is simple and for obtaining a simplified version.
pub trait SimpleContour {
    /// Checks if the contour is already simple, meaning it has no self-intersections
    /// and meets the minimum complexity required.
    ///
    /// # Returns
    ///
    /// - `true` if the contour is simple.
    /// - `false` if the contour is complex or does not meet simplicity criteria.
    fn is_simple(&self) -> bool;

    /// Returns an optional simplified version of the contour.
    ///
    /// # Returns
    ///
    /// - `Some(IntContour)` containing the simplified contour if simplification is possible.
    /// - `None` if the contour is degenerate or empty.
    fn simplified(&self) -> Option<IntContour>;
}

/// A trait for determining if a shape, composed of multiple contours, is simple,
/// and for obtaining a simplified version.
pub trait SimpleShape {
    /// Checks if the shape is simple, meaning all its contours are simple.
    ///
    /// # Returns
    ///
    /// - `true` if all contours in the shape are simple.
    /// - `false` if any contour is complex.
    fn is_simple(&self) -> bool;

    /// Returns an optional simplified version of the shape.
    ///
    /// # Returns
    ///
    /// - `Some(IntShape)` containing the simplified shape if simplification is possible.
    /// - `None` if the shape is degenerate or empty.
    fn simplified(&self) -> Option<IntShape>;
}

/// A trait for determining if a collection of shapes is simple, and for obtaining
/// a simplified version of the entire collection.
pub trait SimpleShapes {
    /// Checks if all shapes in the collection are simple.
    ///
    /// # Returns
    ///
    /// - `true` if all shapes are simple.
    /// - `false` if any shape in the collection is complex.
    fn is_simple(&self) -> bool;

    /// Returns an optional simplified version of the collection.
    ///
    /// # Returns
    ///
    /// - `IntShapes` the simplified shapes.
    fn simplified(&self) -> IntShapes;
}

impl Simplify for IntContour {
    #[inline]
    fn simplify_contour(&mut self) -> bool {
        if self.is_simple() {
            return false;
        }
        if let Some(contour) = self.simplified() {
            self.clear();
            self.extend(contour);
        } else {
            self.clear()
        }
        true
    }
}

impl Simplify for IntShape {
    fn simplify_contour(&mut self) -> bool {
        let mut any_simplified = false;
        let mut any_empty = false;

        for (index, contour) in self.iter_mut().enumerate() {
            if contour.is_simple() { continue; }
            any_simplified = true;

            if let Some(simple_contour) = contour.simplified() {
                contour.clear();
                contour.extend(simple_contour);
            } else if index == 0 {
                // early out main contour is empty
                self.clear();
                return true;
            } else {
                contour.clear();
                any_empty = true;
            }
        }

        if any_empty {
            self.retain(|contour| !contour.is_empty());
        }

        any_simplified
    }
}

impl Simplify for IntShapes {
    fn simplify_contour(&mut self) -> bool {
        let mut any_simplified = false;
        let mut any_empty = false;

        for shape in self.iter_mut() {
            if shape.is_simple() { continue; }
            any_simplified = true;
            if let Some(simple_shape) = shape.simplified() {
                *shape = simple_shape;
            } else {
                shape.clear();
                any_empty = true;
            }
        }

        if any_empty {
            self.retain(|contour| !contour.is_empty());
        }

        any_simplified
    }
}
impl SimpleShape for [IntContour] {
    #[inline]
    fn is_simple(&self) -> bool {
        for contour in self.iter() {
            if !contour.is_simple() {
                return false;
            }
        }
        true
    }

    fn simplified(&self) -> Option<IntShape> {
        let mut contours = Vec::with_capacity(self.len());
        for (i, contour) in self.iter().enumerate() {
            if contour.is_simple() {
                contours.push(contour.clone());
            } else if let Some(simple) = contour.simplified() {
                contours.push(simple);
            } else if i == 0 {
                return None;
            }
        }

        Some(contours)
    }
}

impl SimpleShapes for [IntShape] {
    #[inline]
    fn is_simple(&self) -> bool {
        for shape in self.iter() {
            if !shape.is_simple() {
                return false;
            }
        }
        true
    }

    fn simplified(&self) -> IntShapes {
        let mut shapes = Vec::with_capacity(self.len());
        for shape in self.iter() {
            if shape.is_simple() {
                shapes.push(shape.clone());
            } else if let Some(simple) = shape.simplified() {
                shapes.push(simple);
            }
        }

        shapes
    }
}

impl SimpleContour for [IntPoint] {
    fn is_simple(&self) -> bool {
        let count = self.len();

        if count < 3 { return false; }

        let mut p0 = self[count - 2];
        let p1 = self[count - 1];

        let mut v0 = p1.subtract(p0);
        p0 = p1;

        for &pi in self.iter() {
            let vi = pi.subtract(p0);
            let prod = vi.cross_product(v0);
            if prod == 0 {
                return false;
            }
            v0 = vi;
            p0 = pi;
        }

        true
    }

    fn simplified(&self) -> Option<IntContour> {
        if self.len() < 3 {
            return None;
        }

        let mut n = self.len();
        let mut nodes: Vec<Node> = vec![Node { next: 0, index: 0, prev: 0 }; n];
        let mut validated: Vec<bool> = vec![false; n];

        let mut i0 = n - 2;
        let mut i1 = n - 1;
        for i2 in 0..n {
            nodes[i1] = Node { next: i2, index: i1, prev: i0 };
            i0 = i1;
            i1 = i2;
        }

        let mut first: usize = 0;
        let mut node = nodes[first];
        let mut i = 0;
        while i < n {
            if validated[node.index] {
                node = nodes[node.next];
                continue;
            }

            let p0 = self[node.prev];
            let p1 = self[node.index];
            let p2 = self[node.next];

            if p1.subtract(p0).cross_product(p2.subtract(p1)) == 0 {
                n -= 1;
                if n < 3 {
                    return None;
                }

                // remove node
                nodes[node.prev].next = node.next;
                nodes[node.next].prev = node.prev;

                if node.index == first {
                    first = node.next
                }

                node = nodes[node.prev];

                if validated[node.prev] {
                    i -= 1;
                    validated[node.prev] = false
                }

                if validated[node.next] {
                    i -= 1;
                    validated[node.next] = false
                }

                if validated[node.index] {
                    i -= 1;
                    validated[node.index] = false
                }
            } else {
                validated[node.index] = true;
                i += 1;
                node = nodes[node.next];
            }
        }

        let mut buffer = vec![IntPoint::ZERO; n];
        node = nodes[first];

        for item in buffer.iter_mut().take(n) {
            *item = self[node.index];
            node = nodes[node.next];
        }

        Some(buffer)
    }
}

#[derive(Clone, Copy)]
struct Node {
    next: usize,
    index: usize,
    prev: usize,
}