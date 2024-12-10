pub mod flush_unsaved_buffer;
pub mod poll_messages;
pub mod send_messages;

const MAX_HEADERS_SIZE: u32 = 1_000_000;
pub const MAX_PAYLOAD_SIZE: u32 = 1_000_000_000;
