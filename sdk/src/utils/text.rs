use crate::error::IggyError;
use base64::engine::general_purpose;
use base64::Engine;

pub fn from_base64_as_bytes(value: &str) -> Result<Vec<u8>, IggyError> {
    let result = general_purpose::STANDARD.decode(value);
    if result.is_err() {
        return Err(IggyError::InvalidFormat);
    }

    Ok(result.unwrap())
}

pub fn as_base64(value: &[u8]) -> String {
    general_purpose::STANDARD.encode(value)
}
