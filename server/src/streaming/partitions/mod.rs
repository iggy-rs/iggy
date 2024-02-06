use bytes::Bytes;
use iggy::messages::send_messages;

pub mod consumer_offsets;
pub mod messages;
pub mod partition;
pub mod persistence;
pub mod segments;
pub mod storage;

#[allow(dead_code)]
fn create_messages() -> Vec<send_messages::Message> {
    vec![
        create_message(1, "message 1"),
        create_message(2, "message 2"),
        create_message(3, "message 3"),
        create_message(2, "message 3.2"),
        create_message(1, "message 1.2"),
        create_message(3, "message 3.3"),
    ]
}

fn create_message(id: u128, payload: &str) -> send_messages::Message {
    let payload = Bytes::from(payload.to_string());
    send_messages::Message::new(Some(id), payload, None)
}
