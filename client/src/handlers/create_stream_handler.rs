use crate::handlers::response_handler::handle_status;
use std::io;
use std::str::from_utf8;
use tokio::net::UdpSocket;

const COMMAND: &[u8] = &[11];
const PARTS: usize = 2;

pub async fn handle(input: &[&str], socket: &UdpSocket, buffer: &mut [u8; 1024]) -> io::Result<()> {
    if input.len() != PARTS {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("Invalid create stream command, expected {} parts.", PARTS),
        ));
    }

    let stream = input[0].parse::<u32>();
    if let Err(error) = stream {
        return Err(io::Error::new(io::ErrorKind::Other, error));
    }

    let name = from_utf8(input[1].as_bytes()).unwrap();
    if name.len() > 100 {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "Invalid stream name: {}, expected between 1 and 100 characters.",
                name
            ),
        ));
    }

    let stream = &stream.unwrap().to_le_bytes();
    let name = name.as_bytes();

    socket
        .send([COMMAND, stream, name].concat().as_slice())
        .await?;
    handle_response(socket, buffer).await?;
    Ok(())
}

async fn handle_response(socket: &UdpSocket, buffer: &mut [u8; 1024]) -> io::Result<()> {
    socket.recv(buffer).await?;
    handle_status(buffer)?;
    Ok(())
}
