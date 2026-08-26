pub fn min_max<T: Ord + Copy>(iter: impl Iterator<Item = T>) -> Option<(T, T)> {
    let mut iter = iter;
    let first = iter.next()?;
    let mut min = first;
    let mut max = first;

    for x in iter {
        if x < min {
            min = x;
        } else if x > max {
            max = x;
        }
    }

    Some((min, max))
}