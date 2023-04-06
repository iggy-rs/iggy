use std::io;
use std::io::{Error, ErrorKind};
use tracing::info;

pub fn handle_status(buffer: &mut [u8; 1024]) -> io::Result<()> {
    if buffer.is_empty() {
        return Err(Error::new(ErrorKind::Other, "Invalid status response."));
    }

    let status = buffer[0];
    if status == 0 {
        info!("Status: OK.");
        return Ok(());
    }

    Err(Error::new(
        ErrorKind::Other,
        format!("Invalid status response: {}.", status),
    ))
}
