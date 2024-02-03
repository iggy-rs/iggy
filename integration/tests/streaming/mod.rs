use bytes::Bytes;
use iggy::models::messages::{Message, MessageState};
use iggy::utils::checksum;
use rand::Rng;
use std::ops::Range;

mod common;
mod consumer_group;
mod consumer_offset;
mod messages;
mod partition;
mod personal_access_token;
mod segment;
mod server_side_compression;
mod stream;
mod system;
mod topic;
mod topic_messages;
mod user;

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

fn create_random_payload(range: Range<usize>) -> String {
    let mut rng = rand::thread_rng();
    let length: usize = rng.gen_range(range);
    let bytes = (0..length)
        .map(|_| rng.sample(rand::distributions::Alphanumeric))
        .collect::<Vec<_>>();

    String::from_utf8(bytes).unwrap()
}
