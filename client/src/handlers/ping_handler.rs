use crate::handlers::response_handler::handle_status;
use std::io;
use tokio::net::UdpSocket;

const COMMAND: &[u8] = &[1];

pub async fn handle(socket: &UdpSocket, buffer: &mut [u8; 1024]) -> io::Result<()> {
    socket.send(COMMAND).await?;
    socket.recv(buffer).await?;
    handle_status(buffer)?;
    Ok(())
}
