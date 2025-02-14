use crate::identifier::Identifier;
use tracing::error;

pub(super) fn get_identifier_from_string(val: &str) -> Identifier {
    match Identifier::from_str_value(val) {
        Ok(id) => id,
        Err(err) => {
            error!("Failed to parse stream id due to error: {}", err);
            panic!("{}", err.as_string());
        }
    }
}
