use std::io;
use tokio::net::UdpSocket;
use crate::handlers::response_handler::handle_status;

const COMMAND: &[u8] = &[12];
const PARTS: usize = 1;

pub async fn handle(input: &[&str], socket: &UdpSocket, buffer: &mut [u8; 1024]) -> io::Result<()> {
    if input.len() != PARTS {
        return Err(io::Error::new(io::ErrorKind::Other, format!("Invalid delete topic command, expected {} parts.", PARTS)));
    }

    let id = input[0].parse::<u32>();
    if let Err(error) = id {
        return Err(io::Error::new(io::ErrorKind::Other, error));
    }

    let id = &id.unwrap().to_le_bytes();
    socket.send([COMMAND, id].concat().as_slice()).await?;
    handle_response(socket, buffer).await?;
    Ok(())
}

async fn handle_response(socket: &UdpSocket, buffer: &mut [u8; 1024]) -> io::Result<()> {
    socket.recv(buffer).await?;
    handle_status(buffer)?;
    Ok(())
}