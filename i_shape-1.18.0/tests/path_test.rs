#[cfg(test)]
mod tests {
    use i_shape::int::path::ContourExtension;
    use i_shape::int::simple::{SimpleContour, SimpleShape, SimpleShapes};
    use i_shape::{int_path, int_shape, int_shapes};

    #[test]
    fn test_no_degenerates() {
        let origin = int_path![[0, 0], [0, 1], [1, 1], [1, 0],];

        let path = origin.simplified().unwrap();

        assert_eq!(path, origin, "The path and origin are not equal!");
    }

    #[test]
    fn test_degenerates() {
        let origin = int_path![[0, 0], [0, 1024], [1024, 1024], [1024, 0],];

        let incorrect = int_path![
            [0, 0],
            [0, 1024],
            [0, 1024],
            [512, 1024],
            [1024, 1024],
            [1024, 1024],
            [1024, 0],
        ];

        let path = incorrect.simplified().unwrap();

        assert_eq!(path, origin, "The path and origin are not equal!");
    }

    #[test]
    fn test_degenerates_empty() {
        let incorrect = int_path![[0, 0], [0, 512], [0, 1],];

        let path = incorrect.simplified();

        assert!(path.is_none());
    }

    #[test]
    fn test_degenerates_shape_0() {
        let shape = int_shape![
            [[-10, -10], [-10, 10], [10, 10], [-10, 10], [-10, -10], [10, -10],],
            [[-5, -5], [5, -5], [5, 5], [-5, 5],],
            [[-5, -5], [5, -5], [5, 5], [-5, 5],],
        ];

        assert!(shape.simplified().is_none());
    }

    #[test]
    fn test_degenerates_shape_1() {
        let shape = int_shape![
            [[-10, -10], [-10, 10], [10, 10], [10, -10],],
            [[-5, -5], [5, -5], [5, 5], [-5, 5],],
        ];

        assert_eq!(shape.simplified().unwrap().len(), 2);
    }

    #[test]
    fn test_degenerates_shape_2() {
        let shape = int_shape![
            [[-10, -10], [-10, 10], [10, 10], [10, -10],],
            [[-5, -5], [5, -5], [-5, -5],],
        ];

        let simple = shape.simplified();

        assert_eq!(simple.unwrap().len(), 1);
    }

    #[test]
    fn test_degenerates_shapes_0() {
        let shapes = int_shapes![
            [
                [[-10, -10], [-10, 10], [10, 10], [-10, 10], [-10, -10], [10, -10],],
                [[-5, -5], [5, -5], [5, 5], [-5, 5],],
                [[-5, -5], [5, -5], [5, 5], [-5, 5],],
            ],
            [[[-10, -10], [-10, 10], [10, 10], [10, -10],],],
        ];

        assert_eq!(shapes.simplified().len(), 1);
    }

    #[test]
    fn test_degenerates_shapes_1() {
        let shapes = int_shapes![
            [
                [[-10, -10], [-10, 10], [10, 10], [-10, 10], [-10, -10], [10, -10],],
                [[-5, -5], [5, -5], [5, 5], [-5, 5],],
                [[-5, -5], [5, -5], [5, 5], [-5, 5],],
            ],
            [
                [[-10, -10], [-10, 10], [10, 10], [-10, 10], [-10, -10], [10, -10],],
                [[-5, -5], [5, -5], [5, 5], [-5, 5],],
                [[-5, -5], [5, -5], [5, 5], [-5, 5],],
            ],
        ];

        assert!(shapes.simplified().is_empty());
    }

    #[test]
    fn test_area_1() {
        let mut path = int_path![[0, 0], [0, 1], [1, 1], [1, 0],];

        let area_1 = path.unsafe_area();
        path.reverse();

        let area_2 = path.unsafe_area();

        assert_eq!(area_1, 2);
        assert_eq!(area_2, -2);
    }

    #[test]
    fn test_order_00() {
        let path = int_path![[-10, -10], [-10, 10], [10, 10], [10, -10],];

        assert_eq!(path.is_clockwise_ordered(), true);
    }

    #[test]
    fn test_order_01() {
        let path = int_path![[-10, -10], [10, -10], [10, 10], [-10, 10],];

        assert_eq!(path.is_clockwise_ordered(), false);
    }

    #[test]
    fn test_convex_00() {
        let mut path = int_path![[-10, -10], [-10, 10], [10, 10], [10, -10],];

        assert_eq!(path.is_convex(), true);
        path.reverse();
        assert_eq!(path.is_convex(), true);
    }

    #[test]
    fn test_convex_01() {
        let mut path = int_path![[-10, -10], [0, 10], [10, -10], [0, -5],];

        assert_eq!(path.is_convex(), false);
        path.reverse();
        assert_eq!(path.is_convex(), false);
    }

    #[test]
    fn test_convex_02() {
        let mut path = int_path![[0, 0], [1, 2], [3, 3], [4, 1], [2, 0],];

        assert_eq!(path.is_convex(), true);
        path.reverse();
        assert_eq!(path.is_convex(), true);
    }

    #[test]
    fn test_convex_03() {
        let mut path = int_path![[0, 0], [1, 2], [0, 4], [4, 2], [2, 0],];

        assert_eq!(path.is_convex(), false);
        path.reverse();
        assert_eq!(path.is_convex(), false);
    }

    #[test]
    fn test_convex_04() {
        let path1 = int_path![[0, 0]];

        assert_eq!(path1.is_convex(), true);

        let path2 = int_path![[0, 0], [1, 0],];

        assert_eq!(path2.is_convex(), true);
    }

    #[test]
    fn test_convex_05() {
        let mut path = int_path![[0, 0], [1, 2], [2, 3], [3, 2], [4, 1], [2, 0],];

        assert_eq!(path.is_convex(), true);
        path.reverse();
        assert_eq!(path.is_convex(), true);
    }

    #[test]
    fn test_convex_06() {
        let mut path = int_path![
            [-10, -10],
            [-10, 0],
            [-10, 10],
            [0, 10],
            [10, 10],
            [10, 0],
            [10, -10],
            [0, -10],
        ];

        assert_eq!(path.is_convex(), true);
        path.reverse();
        assert_eq!(path.is_convex(), true);
    }

    #[test]
    fn test_convex_07() {
        let mut path = int_path![
            [-10, -10],
            [-9, 0],
            [-10, 10],
            [0, 10],
            [10, 10],
            [10, 0],
            [10, -10],
            [0, -10],
        ];

        assert_eq!(path.is_convex(), false);
        path.reverse();
        assert_eq!(path.is_convex(), false);
    }
}
