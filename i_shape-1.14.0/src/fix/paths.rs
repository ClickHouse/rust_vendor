use alloc::vec::Vec;
use crate::fix::path::FixPath;

pub type FixPaths = Vec<FixPath>;

pub trait FixPathsExtension {
    fn points_count(&self) -> usize;
}

impl FixPathsExtension for FixPaths {

    #[inline(always)]
    fn points_count(&self) -> usize {
        self.iter().fold(0, |acc, x| acc + x.len())
    }
}

impl FixPathsExtension for [FixPath] {

    #[inline(always)]
    fn points_count(&self) -> usize {
        self.iter().fold(0, |acc, x| acc + x.len())
    }
}