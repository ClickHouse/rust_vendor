use alloc::vec::Vec;
use i_float::float::compatible::FloatPointCompatible;
use i_float::float::number::FloatNumber;
use i_float::float::rect::FloatRect;
use crate::base::data::{Path, Shape};

pub trait RectInit<P, T>
where
    P: FloatPointCompatible<T>,
    T: FloatNumber,
{
    fn with_path(path: &[P]) -> Option<FloatRect<T>>;
    fn with_paths(paths: &[Vec<P>]) -> Option<FloatRect<T>>;
    fn with_list_of_paths(list: &[Vec<Vec<P>>]) -> Option<FloatRect<T>>;
}

trait FirstPoint<P, T>
where
    P: FloatPointCompatible<T>,
    T: FloatNumber,
{
    fn first_point(&self) -> Option<P>;
}

impl<P, T> RectInit<P, T> for FloatRect<T>
where
    P: FloatPointCompatible<T>,
    T: FloatNumber,
{
    fn with_path(path: &[P]) -> Option<FloatRect<T>> {
        let &first_point = path.first()?;

        let mut rect = Self::with_point(first_point);

        for p in path.iter() {
            rect.unsafe_add_point(p);
        }

        Some(rect)
    }

    fn with_paths(paths: &[Path<P>]) -> Option<FloatRect<T>> {
        let first_point = paths.first_point()?;

        let mut rect = Self::with_point(first_point);

        for path in paths.iter() {
            for p in path.iter() {
                rect.unsafe_add_point(p);
            }
        }

        Some(rect)
    }

    fn with_list_of_paths(list: &[Vec<Path<P>>]) -> Option<FloatRect<T>> {
        let first_point = list.first_point()?;

        let mut rect = Self::with_point(first_point);

        for paths in list.iter() {
            for path in paths.iter() {
                for p in path.iter() {
                    rect.unsafe_add_point(p);
                }
            }
        }

        Some(rect)
    }
}

impl<P, T> FirstPoint<P, T> for [Path<P>]
where
    P: FloatPointCompatible<T>,
    T: FloatNumber,
{
    fn first_point(&self) -> Option<P> {
        for path in self.iter() {
            if let Some(&p) = path.first() {
                return Some(p);
            }
        }
        None
    }
}

impl<P, T> FirstPoint<P, T> for [Shape<P>]
where
    P: FloatPointCompatible<T>,
    T: FloatNumber,
{
    fn first_point(&self) -> Option<P> {
        for paths in self.iter() {
            for path in paths.iter() {
                if let Some(&p) = path.first() {
                    return Some(p);
                }
            }
        }
        None
    }
}
