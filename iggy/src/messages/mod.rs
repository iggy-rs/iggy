pub mod poll_messages;
pub mod send_messages;

const MAX_HEADERS_SIZE: u32 = 100 * 1024;
const MAX_PAYLOAD_SIZE: u32 = 10 * 1024 * 1024;
