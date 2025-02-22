pub mod flush_unsaved_buffer;
pub mod poll_messages;
pub mod send_messages;
pub mod send_messages_server;

const MAX_HEADERS_SIZE: u32 = 100 * 1000;
pub const MAX_PAYLOAD_SIZE: u32 = 10 * 1000 * 1000;
