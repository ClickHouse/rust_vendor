#[test]
fn resolution() {
    let index = 0x8a1fb46622dffff;

    assert_eq!(h3o_bit::get_resolution(index), 10, "get_resolution");

    let cleared = h3o_bit::clr_resolution(index);
    let expected = 0x801fb46622dffff;
    assert_eq!(cleared, expected, "clr_resolution");

    let restored = h3o_bit::set_resolution(cleared, 10);
    assert_eq!(restored, index, "set_resolution");
}

#[test]
fn base_cell() {
    let index = 0x8a1fb46622dffff;

    assert_eq!(h3o_bit::get_base_cell(index), 15, "get_base_cell");

    let updated = h3o_bit::set_base_cell(index, 42);
    let expected = 0x8a55b46622dffff;
    assert_eq!(updated, expected, "set_base_cell");
}

#[test]
fn direction() {
    let index = 0x8a1fb46622dffff;

    assert_eq!(h3o_bit::get_direction(index, 4), 3, "get_direction");

    let cleared = h3o_bit::clr_direction(index, 4);
    let expected = 0x8a1fb40622dffff;
    assert_eq!(cleared, expected, "clr_direction");

    let updated = h3o_bit::set_direction(index, 4, 6);
    let expected = 0x8a1fb4c622dffff;
    assert_eq!(updated, expected, "set_resolution");
}
