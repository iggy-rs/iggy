use crate::handlers::response_handler::handle_status;
use crate::topic::Topic;
use std::io;
use std::str::from_utf8;
use tokio::net::UdpSocket;
use tracing::info;

const COMMAND: &[u8] = &[20];
const PARTS: usize = 1;

pub async fn handle(input: &[&str], socket: &UdpSocket, buffer: &mut [u8; 1024]) -> io::Result<()> {
    if input.len() != PARTS {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("Invalid get topics command, expected {} parts.", PARTS),
        ));
    }

    let stream = input[0].parse::<u32>();
    if let Err(error) = stream {
        return Err(io::Error::new(io::ErrorKind::Other, error));
    }

    let stream = &stream.unwrap().to_le_bytes();
    socket.send([COMMAND, stream].concat().as_slice()).await?;
    let payload_length = socket.recv(buffer).await?;
    handle_status(buffer)?;

    if payload_length == 1 {
        info!("No topics found.");
        return Ok(());
    }

    let mut topics = Vec::new();
    let payload = &buffer[1..payload_length];
    let length = payload_length - 1;
    let mut position = 0;
    while position < length {
        let id = u32::from_le_bytes(payload[position..position + 4].try_into().unwrap());
        let partitions =
            u32::from_le_bytes(payload[position + 4..position + 8].try_into().unwrap());
        let name_length =
            u32::from_le_bytes(payload[position + 8..position + 12].try_into().unwrap()) as usize;
        let name = from_utf8(&payload[position + 12..position + 12 + name_length]);
        topics.push(Topic {
            id,
            partitions,
            name: name.unwrap().to_string(),
        });
        position += 4 + 4 + 4 + name_length;

        if position >= length {
            break;
        }
    }

    topics.sort_by(|x, y| x.id.cmp(&y.id));
    info!("Topics: {:#?}", topics);

    Ok(())
}
