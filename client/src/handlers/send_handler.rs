use crate::handlers::response_handler::handle_status;
use std::{io, str};
use tokio::net::UdpSocket;

const COMMAND: &[u8] = &[3];
const PARTS: usize = 4;

pub async fn handle(input: &[&str], socket: &UdpSocket, buffer: &mut [u8; 1024]) -> io::Result<()> {
    if input.len() != PARTS {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("Invalid send command, expected {} parts.", PARTS),
        ));
    }

    let topic = input[0].parse::<u32>();
    if let Err(error) = topic {
        return Err(io::Error::new(io::ErrorKind::Other, error));
    }

    let key_kind = input[1].parse::<u8>();
    if let Err(error) = key_kind {
        return Err(io::Error::new(io::ErrorKind::Other, error));
    }

    let key_value = input[2].parse::<u32>();
    if let Err(error) = key_value {
        return Err(io::Error::new(io::ErrorKind::Other, error));
    }

    let topic = &topic.unwrap().to_le_bytes();
    let key_kind = &key_kind.unwrap().to_le_bytes();
    let key_value = &key_value.unwrap().to_le_bytes();
    let payload = input[3].as_bytes();

    socket
        .send(
            [COMMAND, topic, key_kind, key_value, payload]
                .concat()
                .as_slice(),
        )
        .await?;
    handle_response(socket, buffer).await?;
    Ok(())
}

async fn handle_response(socket: &UdpSocket, buffer: &mut [u8; 1024]) -> io::Result<()> {
    socket.recv(buffer).await?;
    handle_status(buffer)?;
    Ok(())
}
