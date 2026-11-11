use i_float::adapter::FloatPointAdapter;
use i_float::float::compatible::FloatPointCompatible;
use i_float::float::number::FloatNumber;
use i_float::float::rect::FloatRect;
use crate::flat::buffer::FlatContoursBuffer;
use crate::source::resource::ShapeResource;

impl FlatContoursBuffer {

    #[inline]
    pub fn set_with_resource<P, T, R>(&mut self, resource: &R) -> FloatPointAdapter<P, T>
    where
        T: FloatNumber,
        P: FloatPointCompatible<T>,
        R: ShapeResource<P, T> +?Sized
    {
        let mut contours_count = 0;
        let mut points_count = 0;
        let mut min_x = T::MAX;
        let mut max_x = T::MIN;
        let mut min_y = T::MAX;
        let mut max_y = T::MIN;
        for contour in resource.iter_paths() {
            contours_count += 1;
            points_count += contour.len();
            for p in contour.iter() {
                min_x = min_x.min(p.x());
                max_x = max_x.max(p.x());
                min_y = min_y.min(p.y());
                max_y = max_y.max(p.y());
            }
        }

        self.clear_and_reserve(points_count, contours_count);
        if points_count == 0 {
            return FloatPointAdapter::new(FloatRect::zero());
        }

        let rect = FloatRect::new(min_x, max_x, min_y, max_y);
        let adapter = FloatPointAdapter::new(rect);

        let mut offset = 0;
        for contour in resource.iter_paths() {
            for p in contour.iter() {
                self.points.push(adapter.float_to_int(p));
            }
            let contour_len = contour.len();
            self.ranges.push(offset..offset + contour_len);
            offset += contour_len;
        }

        adapter
    }
}