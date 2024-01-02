use bytes::Bytes;
use iggy::models::messages::{Message, MessageState};
use iggy::utils::checksum;

pub mod consumer_offsets;
pub mod messages;
pub mod partition;
pub mod persistence;
pub mod segments;
pub mod storage;

#[allow(dead_code)]
fn create_messages() -> Vec<Message> {
    vec![
        create_message(0, 1, "message 1"),
        create_message(1, 2, "message 2"),
        create_message(2, 3, "message 3"),
        create_message(3, 2, "message 3.2"),
        create_message(4, 1, "message 1.2"),
        create_message(5, 3, "message 3.3"),
    ]
}

fn create_message(offset: u64, id: u128, payload: &str) -> Message {
    let payload = Bytes::from(payload.to_string());
    let checksum = checksum::calculate(payload.as_ref());
    Message::create(
        offset,
        MessageState::Available,
        1,
        id,
        payload,
        checksum,
        None,
    )
}
