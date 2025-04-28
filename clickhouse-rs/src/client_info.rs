use crate::binary::Encoder;

pub const CLICK_HOUSE_REVISION: u64 = 54429; // DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS
pub const CLICK_HOUSE_DBMSVERSION_MAJOR: u64 = 1;
pub const CLICK_HOUSE_DBMSVERSION_MINOR: u64 = 1;

pub fn write(encoder: &mut Encoder, client_name: &str) {
    encoder.string(client_name);
    encoder.uvarint(CLICK_HOUSE_DBMSVERSION_MAJOR);
    encoder.uvarint(CLICK_HOUSE_DBMSVERSION_MINOR);
    encoder.uvarint(CLICK_HOUSE_REVISION);
}

pub fn description(client_name: &str) -> String {
    format!(
        "{client_name} {CLICK_HOUSE_DBMSVERSION_MAJOR}.{CLICK_HOUSE_DBMSVERSION_MINOR}.{CLICK_HOUSE_REVISION}",
    )
}

#[test]
fn test_description() {
    assert_eq!(
        description("Rust SQLDriver"),
        format!(
            "Rust SQLDriver {}.{}.{}",
            CLICK_HOUSE_DBMSVERSION_MAJOR, CLICK_HOUSE_DBMSVERSION_MINOR, CLICK_HOUSE_REVISION
        )
    )
}
