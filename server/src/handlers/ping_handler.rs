use std::net::SocketAddr;
use tokio::net::UdpSocket;
use streaming::stream_error::StreamError;
use crate::handlers::STATUS_OK;

pub const COMMAND: &[u8] = &[1];

pub async fn handle(socket: &UdpSocket, address: SocketAddr) -> Result<(), StreamError>  {
    if socket.send_to(STATUS_OK, address).await.is_err() {
        return Err(StreamError::NetworkError);
    }

    Ok(())
}