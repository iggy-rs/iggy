use crate::handlers::response_handler::handle_status;
use crate::message::Message;
use std::str::from_utf8;
use std::{io, str};
use tokio::net::UdpSocket;
use tracing::info;

const COMMAND: &[u8] = &[2];
const PARTS: usize = 6;

enum Format {
    Binary,
    String,
}

pub async fn handle(input: &[&str], socket: &UdpSocket, buffer: &mut [u8; 1024]) -> io::Result<()> {
    if input.len() < PARTS {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("Invalid poll command, expected {} parts.", PARTS),
        ));
    }

    let stream = input[0].parse::<u32>();
    if let Err(error) = stream {
        return Err(io::Error::new(io::ErrorKind::Other, error));
    }

    let topic = input[1].parse::<u32>();
    if let Err(error) = topic {
        return Err(io::Error::new(io::ErrorKind::Other, error));
    }

    let partition_id = input[2].parse::<u32>();
    if let Err(error) = partition_id {
        return Err(io::Error::new(io::ErrorKind::Other, error));
    }

    let kind = input[3].parse::<u8>();
    if let Err(error) = kind {
        return Err(io::Error::new(io::ErrorKind::Other, error));
    }

    let value = input[4].parse::<u64>();
    if let Err(error) = value {
        return Err(io::Error::new(io::ErrorKind::Other, error));
    }

    let count = input[5].parse::<u32>();
    if let Err(error) = count {
        return Err(io::Error::new(io::ErrorKind::Other, error));
    }

    let stream = &stream.unwrap().to_le_bytes();
    let topic = &topic.unwrap().to_le_bytes();
    let partition_id = &partition_id.unwrap().to_le_bytes();
    let kind = &kind.unwrap().to_le_bytes();
    let value = &value.unwrap().to_le_bytes();
    let count = &count.unwrap().to_le_bytes();
    let format = match input.get(6) {
        Some(format) => match *format {
            "b" => Format::Binary,
            "s" => Format::String,
            _ => return Err(io::Error::new(io::ErrorKind::Other, "Invalid format.")),
        },
        None => Format::Binary,
    };

    socket
        .send(
            [COMMAND, stream, topic, partition_id, kind, value, count]
                .concat()
                .as_slice(),
        )
        .await?;
    handle_response(socket, buffer, format).await?;
    Ok(())
}

async fn handle_response(
    socket: &UdpSocket,
    buffer: &mut [u8; 1024],
    format: Format,
) -> io::Result<()> {
    let payload_length = socket.recv(buffer).await?;
    handle_status(buffer)?;

    if payload_length == 1 {
        info!("No messages found.");
        return Ok(());
    }

    let payload = &buffer[1..payload_length];
    let payload_length = payload.len();
    let messages_count = u32::from_le_bytes(payload[..4].try_into().unwrap());
    let mut position = 4;
    let mut messages = Vec::new();
    while position < payload_length {
        let offset = u64::from_le_bytes(payload[position..position + 8].try_into().unwrap());
        let timestamp =
            u64::from_le_bytes(payload[position + 8..position + 16].try_into().unwrap());
        let message_length =
            u64::from_le_bytes(payload[position + 16..position + 24].try_into().unwrap()) as usize;
        let payload = payload[position + 24..position + 24 + message_length].to_vec();

        position = position + 24 + message_length;
        messages.push(Message {
            offset,
            timestamp,
            length: message_length as u64,
            payload,
        });
        if position >= payload_length {
            break;
        }
    }

    messages.sort_by(|x, y| x.offset.cmp(&y.offset));

    let mut text = format!("Received {} messages:", messages_count);
    for message in messages {
        text += &format!(
            "\noffset: {}, timestamp: {}, length: {}, payload: ",
            message.offset, message.timestamp, message.length
        );
        match format {
            Format::Binary => text += &format!("{:?}", message.payload),
            Format::String => text += from_utf8(&message.payload).unwrap(),
        }
    }

    info!("{}", text);

    Ok(())
}
