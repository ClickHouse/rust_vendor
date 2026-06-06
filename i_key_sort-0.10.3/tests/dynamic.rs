#[cfg(test)]
mod tests {
    use i_key_sort::sort::one_key::OneKeySort;
    use i_key_sort::sort::one_key_cmp::OneKeyAndCmpSort;
    use i_key_sort::sort::two_keys::TwoKeysSort;
    use i_key_sort::sort::two_keys_cmp::TwoKeysAndCmpSort;
    use std::f64::consts::PI;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct Point {
        pub x: i32,
        pub y: i32,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct Segment {
        // 16
        pub a: Point, // 8
        pub b: Point, // 8
    }

    impl Point {
        #[inline(always)]
        pub fn new(x: i32, y: i32) -> Self {
            Self { x, y }
        }
    }

    impl Segment {
        #[inline(always)]
        pub fn new(a: Point, b: Point) -> Self {
            Self { a, b }
        }
    }

    #[test]
    fn test_circle_one_key() {
        let mut buffer = Vec::new();
        for n in 1..1_000 {
            circle_one_key_test(n, 2.0, &mut buffer);
            circle_one_key_test(n, 10.0, &mut buffer);
            circle_one_key_test(n, 20.0, &mut buffer);
            circle_one_key_test(n, 50.0, &mut buffer);
            circle_one_key_test(n, 100.0, &mut buffer);
            circle_one_key_test(n, 200.0, &mut buffer);
            circle_one_key_test(n, 1000.0, &mut buffer);
        }

        circle_one_key_test(10_000, 10_000.0, &mut buffer);
        circle_one_key_test(100_000, 100_000.0, &mut buffer);
    }

    #[test]
    fn test_circle_one_key_cmp() {
        let mut buffer = Vec::new();
        for n in 1..1_000 {
            circle_one_key_cmp_test(n, 2.0, &mut buffer);
            circle_one_key_cmp_test(n, 10.0, &mut buffer);
            circle_one_key_cmp_test(n, 20.0, &mut buffer);
            circle_one_key_cmp_test(n, 50.0, &mut buffer);
            circle_one_key_cmp_test(n, 100.0, &mut buffer);
            circle_one_key_cmp_test(n, 200.0, &mut buffer);
            circle_one_key_cmp_test(n, 1000.0, &mut buffer);
        }
        circle_one_key_cmp_test(10_000, 10_000.0, &mut buffer);
        circle_one_key_cmp_test(100_000, 100_000.0, &mut buffer);
    }

    #[test]
    fn test_circle_two_keys() {
        let mut buffer = Vec::new();
        for n in 1..1_000 {
            circle_two_keys_test(n, 2.0, &mut buffer);
            circle_two_keys_test(n, 10.0, &mut buffer);
            circle_two_keys_test(n, 20.0, &mut buffer);
            circle_two_keys_test(n, 50.0, &mut buffer);
            circle_two_keys_test(n, 100.0, &mut buffer);
            circle_two_keys_test(n, 200.0, &mut buffer);
            circle_two_keys_test(n, 1000.0, &mut buffer);
        }
        circle_two_keys_test(10_000, 10_000.0, &mut buffer);
        circle_two_keys_test(100_000, 100_000.0, &mut buffer);
    }

    #[test]
    fn test_circle_two_keys_cmp() {
        let mut buffer = Vec::new();
        for n in 1..1_500 {
            circle_two_keys_cmp_test(n, 2.0, &mut buffer);
            circle_two_keys_cmp_test(n, 10.0, &mut buffer);
            circle_two_keys_cmp_test(n, 20.0, &mut buffer);
            circle_two_keys_cmp_test(n, 50.0, &mut buffer);
            circle_two_keys_cmp_test(n, 100.0, &mut buffer);
            circle_two_keys_cmp_test(n, 200.0, &mut buffer);
            circle_two_keys_cmp_test(n, 1000.0, &mut buffer);
        }
        circle_two_keys_cmp_test(10_000, 10_000.0, &mut buffer);
        circle_two_keys_cmp_test(100_000, 100_000.0, &mut buffer);
    }

    fn circle_one_key_test(count: usize, radius: f64, buffer: &mut Vec<i32>) {
        let mut segments = circle_x(radius, count);

        let res: Vec<_> = [false, true]
            .iter()
            .map(|&parallel| {
                let mut arr = segments.clone();
                arr.sort_by_one_key_and_buffer(parallel, buffer, |&x| x);
                arr
            })
            .collect();

        segments.sort_unstable_by(|x0, x1| x0.cmp(&x1));

        for arr in res {
            assert_eq!(arr, segments);
        }
    }

    fn circle_one_key_cmp_test(count: usize, radius: f64, buffer: &mut Vec<Point>) {
        let mut segments = circle_point(radius, count);

        let res: Vec<_> = [false, true]
            .iter()
            .map(|&parallel| {
                let mut arr = segments.clone();
                arr.sort_by_one_key_then_by_and_buffer(
                    parallel,
                    buffer,
                    |p| p.x,
                    |p0, p1| p0.y.cmp(&p1.y),
                );
                arr
            })
            .collect();

        segments.sort_unstable_by(|p0, p1| p0.x.cmp(&p1.x).then(p0.y.cmp(&p1.y)));

        for arr in res {
            assert_eq!(arr, segments);
        }
    }

    fn circle_two_keys_test(count: usize, radius: f64, buffer: &mut Vec<Point>) {
        let mut segments = circle_point(radius, count);

        let res: Vec<_> = [false, true]
            .iter()
            .map(|&parallel| {
                let mut arr = segments.clone();
                arr.sort_by_two_keys_and_buffer(parallel, buffer, |p| p.x, |p| p.y);
                arr
            })
            .collect();

        segments.sort_unstable_by(|p0, p1| p0.x.cmp(&p1.x).then(p0.y.cmp(&p1.y)));

        for arr in res {
            assert_eq!(arr, segments);
        }
    }

    fn circle_two_keys_cmp_test(count: usize, radius: f64, buffer: &mut Vec<Segment>) {
        let mut segments = circle_segments(radius, 0.0, count);

        let res: Vec<_> = [false, true]
            .iter()
            .map(|&parallel| {
                let mut arr = segments.clone();

                arr.sort_by_two_keys_then_by_and_buffer(
                    parallel,
                    buffer,
                    |s| s.a.x,
                    |s| s.a.y,
                    |s0, s1| s0.b.x.cmp(&s1.b.x).then(s0.b.y.cmp(&s1.b.y)),
                );
                arr
            })
            .collect();

        segments.sort_unstable_by(|s0, s1| {
            s0.a.x
                .cmp(&s1.a.x)
                .then(s0.a.y.cmp(&s1.a.y))
                .then(s0.b.x.cmp(&s1.b.x).then(s0.b.y.cmp(&s1.b.y)))
        });

        for arr in res {
            assert_eq!(arr, segments);
        }
    }

    fn circle_x(radius: f64, n: usize) -> Vec<i32> {
        let mut result = Vec::with_capacity(n);
        let da: f64 = PI * 0.5;
        let mut a: f64 = 0.0;
        for _ in 0..n {
            result.push((radius * a.cos()) as i32);
            a += da;
        }
        result
    }

    fn circle_point(radius: f64, n: usize) -> Vec<Point> {
        let mut result = Vec::with_capacity(n);
        let da: f64 = PI * 0.5;
        let mut a: f64 = 0.0;
        for _ in 0..n {
            let (sin, cos) = a.sin_cos();

            let x = radius * cos;
            let y = radius * sin;

            result.push(Point::new(x as i32, y as i32));
            a += da;
        }
        result
    }

    fn circle_segments(radius: f64, angle: f64, n: usize) -> Vec<Segment> {
        let mut result = Vec::with_capacity(n);
        let da: f64 = PI * 0.7;
        let mut a: f64 = angle;
        let r = 1024.0 * radius;
        let mut points = Vec::with_capacity(n);
        for _ in 0..n {
            let (sin, cos) = a.sin_cos();

            let x = r * cos;
            let y = r * sin;

            points.push(Point::new(x as i32, y as i32));
            a += da;
        }

        let mut p0 = points.last().unwrap().clone();
        for &pi in points.iter() {
            result.push(Segment::new(p0, pi));
            p0 = pi;
        }

        result
    }
}
