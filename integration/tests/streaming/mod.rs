use bytes::Bytes;
use iggy::messages::send_messages::Message;

mod common;
mod consumer_offset;
mod messages;
mod partition;
mod segment;
mod stream;
mod system;
mod topic;
mod topic_messages;

fn create_messages() -> Vec<Message> {
    vec![
        create_message(1, "message 1"),
        create_message(2, "message 2"),
        create_message(3, "message 3"),
        create_message(4, "message 3.2"),
        create_message(5, "message 1.2"),
        create_message(6, "message 3.3"),
    ]
}

fn create_message(id: u128, payload: &str) -> Message {
    let payload = Bytes::from(payload.to_string());
    Message {
        id,
        length: payload.len() as u32,
        payload,
        headers: None,
    }
}
