#[cfg(test)]
mod tests {
    use i_float::int::point::IntPoint;
    use i_shape::int::path::ContourExtension;
    use i_shape::int::simple::{SimpleContour, SimpleShape, SimpleShapes};

    #[test]
    fn test_no_degenerates() {
        let origin = [
            IntPoint::new(0, 0),
            IntPoint::new(0, 1),
            IntPoint::new(1, 1),
            IntPoint::new(1, 0)
        ];

        let path = origin.simplified().unwrap();

        assert_eq!(path.as_slice(), origin.as_ref(), "The path and origin are not equal!");
    }

    #[test]
    fn test_degenerates() {
        let origin = [
            IntPoint::new(0, 0),
            IntPoint::new(0, 1024),
            IntPoint::new(1024, 1024),
            IntPoint::new(1024, 0)
        ];

        let incorrect = [
            IntPoint::new(0, 0),
            IntPoint::new(0, 1024),
            IntPoint::new(0, 1024),
            IntPoint::new(512, 1024),
            IntPoint::new(1024, 1024),
            IntPoint::new(1024, 1024),
            IntPoint::new(1024, 0)
        ];

        let path = incorrect.simplified().unwrap();

        assert_eq!(path.as_slice(), origin.as_ref(), "The path and origin are not equal!");
    }

    #[test]
    fn test_degenerates_empty() {
        let incorrect = [
            IntPoint::new(0, 0),
            IntPoint::new(0, 512),
            IntPoint::new(0, 1)
        ];

        let path = incorrect.simplified();

        assert!(path.is_none());
    }

    #[test]
    fn test_degenerates_shape_0() {
        let shape = [
            vec![
                IntPoint::new(-10, -10),
                IntPoint::new(-10, 10),
                IntPoint::new(10, 10),
                IntPoint::new(-10, 10),
                IntPoint::new(-10, -10),
                IntPoint::new(10, -10),
            ],
            vec![
                IntPoint::new(-5, -5),
                IntPoint::new(5, -5),
                IntPoint::new(5, 5),
                IntPoint::new(-5, 5),
            ],
            vec![
                IntPoint::new(-5, -5),
                IntPoint::new(5, -5),
                IntPoint::new(5, 5),
                IntPoint::new(-5, 5),
            ],
        ];

        assert!(shape.simplified().is_none());
    }

    #[test]
    fn test_degenerates_shape_1() {
        let shape = [
            vec![
                IntPoint::new(-10, -10),
                IntPoint::new(-10, 10),
                IntPoint::new(10, 10),
                IntPoint::new(10, -10),
            ],
            vec![
                IntPoint::new(-5, -5),
                IntPoint::new(5, -5),
                IntPoint::new(5, 5),
                IntPoint::new(-5, 5),
            ],
        ];

        assert_eq!(shape.simplified().unwrap().len(), 2);
    }

    #[test]
    fn test_degenerates_shape_2() {
        let shape = [
            vec![
                IntPoint::new(-10, -10),
                IntPoint::new(-10, 10),
                IntPoint::new(10, 10),
                IntPoint::new(10, -10),
            ],
            vec![
                IntPoint::new(-5, -5),
                IntPoint::new(5, -5),
                IntPoint::new(-5, -5)
            ],
        ];

        let simple = shape.simplified();

        assert_eq!(simple.unwrap().len(), 1);
    }

    #[test]
    fn test_degenerates_shapes_0() {
        let shapes = [
            vec![
                vec![
                    IntPoint::new(-10, -10),
                    IntPoint::new(-10, 10),
                    IntPoint::new(10, 10),
                    IntPoint::new(-10, 10),
                    IntPoint::new(-10, -10),
                    IntPoint::new(10, -10),
                ],
                vec![
                    IntPoint::new(-5, -5),
                    IntPoint::new(5, -5),
                    IntPoint::new(5, 5),
                    IntPoint::new(-5, 5),
                ],
                vec![
                    IntPoint::new(-5, -5),
                    IntPoint::new(5, -5),
                    IntPoint::new(5, 5),
                    IntPoint::new(-5, 5),
                ],
            ],
            vec![
                vec![
                    IntPoint::new(-10, -10),
                    IntPoint::new(-10, 10),
                    IntPoint::new(10, 10),
                    IntPoint::new(10, -10)
                ],
            ]
        ];

        assert_eq!(shapes.simplified().len(), 1);
    }

    #[test]
    fn test_degenerates_shapes_1() {
        let shapes = [
            vec![
                vec![
                    IntPoint::new(-10, -10),
                    IntPoint::new(-10, 10),
                    IntPoint::new(10, 10),
                    IntPoint::new(-10, 10),
                    IntPoint::new(-10, -10),
                    IntPoint::new(10, -10),
                ],
                vec![
                    IntPoint::new(-5, -5),
                    IntPoint::new(5, -5),
                    IntPoint::new(5, 5),
                    IntPoint::new(-5, 5),
                ],
                vec![
                    IntPoint::new(-5, -5),
                    IntPoint::new(5, -5),
                    IntPoint::new(5, 5),
                    IntPoint::new(-5, 5),
                ],
            ],
            vec![
                vec![
                    IntPoint::new(-10, -10),
                    IntPoint::new(-10, 10),
                    IntPoint::new(10, 10),
                    IntPoint::new(-10, 10),
                    IntPoint::new(-10, -10),
                    IntPoint::new(10, -10),
                ],
                vec![
                    IntPoint::new(-5, -5),
                    IntPoint::new(5, -5),
                    IntPoint::new(5, 5),
                    IntPoint::new(-5, 5),
                ],
                vec![
                    IntPoint::new(-5, -5),
                    IntPoint::new(5, -5),
                    IntPoint::new(5, 5),
                    IntPoint::new(-5, 5),
                ],
            ]
        ];

        assert!(shapes.simplified().is_empty());
    }

    #[test]
    fn test_area_1() {
        let mut path = [
            IntPoint::new(0, 0),
            IntPoint::new(0, 1),
            IntPoint::new(1, 1),
            IntPoint::new(1, 0)
        ];

        let area_1 = path.to_vec().unsafe_area();
        path.reverse();

        let area_2 = path.to_vec().unsafe_area();

        assert_eq!(area_1, 2);
        assert_eq!(area_2, -2);
    }

    #[test]
    fn test_order_00() {
        let path = [
            IntPoint::new(-10, -10),
            IntPoint::new(-10, 10),
            IntPoint::new(10, 10),
            IntPoint::new(10, -10)
        ].to_vec();

        assert_eq!(path.is_clockwise_ordered(), true);
    }

    #[test]
    fn test_order_01() {
        let path = [
            IntPoint::new(-10, -10),
            IntPoint::new(10, -10),
            IntPoint::new(10, 10),
            IntPoint::new(-10, 10)
        ].to_vec();

        assert_eq!(path.is_clockwise_ordered(), false);
    }

    #[test]
    fn test_convex_00() {
        let mut path = [
            IntPoint::new(-10, -10),
            IntPoint::new(-10, 10),
            IntPoint::new(10, 10),
            IntPoint::new(10, -10)
        ].to_vec();

        assert_eq!(path.is_convex(), true);
        path.reverse();
        assert_eq!(path.is_convex(), true);
    }

    #[test]
    fn test_convex_01() {
        let mut path = [
            IntPoint::new(-10, -10),
            IntPoint::new(0, 10),
            IntPoint::new(10, -10),
            IntPoint::new(0, -5)
        ].to_vec();

        assert_eq!(path.is_convex(), false);
        path.reverse();
        assert_eq!(path.is_convex(), false);
    }

    #[test]
    fn test_convex_02() {
        let mut path = [
            IntPoint::new(0, 0),
            IntPoint::new(1, 2),
            IntPoint::new(3, 3),
            IntPoint::new(4, 1),
            IntPoint::new(2, 0)
        ].to_vec();

        assert_eq!(path.is_convex(), true);
        path.reverse();
        assert_eq!(path.is_convex(), true);
    }

    #[test]
    fn test_convex_03() {
        let mut path = [
            IntPoint::new(0, 0),
            IntPoint::new(1, 2),
            IntPoint::new(0, 4),
            IntPoint::new(4, 2),
            IntPoint::new(2, 0)
        ].to_vec();

        assert_eq!(path.is_convex(), false);
        path.reverse();
        assert_eq!(path.is_convex(), false);
    }

    #[test]
    fn test_convex_04() {
        let path1 = [
            IntPoint::new(0, 0)
        ].to_vec();

        assert_eq!(path1.is_convex(), true);

        let path2 = [
            IntPoint::new(0, 0),
            IntPoint::new(1, 0)
        ].to_vec();

        assert_eq!(path2.is_convex(), true);
    }

    #[test]
    fn test_convex_05() {
        let mut path = [
            IntPoint::new(0, 0),
            IntPoint::new(1, 2),
            IntPoint::new(2, 3),
            IntPoint::new(3, 2),
            IntPoint::new(4, 1),
            IntPoint::new(2, 0)
        ].to_vec();

        assert_eq!(path.is_convex(), true);
        path.reverse();
        assert_eq!(path.is_convex(), true);
    }

    #[test]
    fn test_convex_06() {
        let mut path = [
            IntPoint::new(-10, -10),
            IntPoint::new(-10, 0),
            IntPoint::new(-10, 10),
            IntPoint::new(0, 10),
            IntPoint::new(10, 10),
            IntPoint::new(10, 0),
            IntPoint::new(10, -10),
            IntPoint::new(0, -10)
        ].to_vec();

        assert_eq!(path.is_convex(), true);
        path.reverse();
        assert_eq!(path.is_convex(), true);
    }

    #[test]
    fn test_convex_07() {
        let mut path = [
            IntPoint::new(-10, -10),
            IntPoint::new(-9, 0),
            IntPoint::new(-10, 10),
            IntPoint::new(0, 10),
            IntPoint::new(10, 10),
            IntPoint::new(10, 0),
            IntPoint::new(10, -10),
            IntPoint::new(0, -10)
        ].to_vec();

        assert_eq!(path.is_convex(), false);
        path.reverse();
        assert_eq!(path.is_convex(), false);
    }
}