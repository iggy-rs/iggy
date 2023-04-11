use bytes::Bytes;
use std::net::SocketAddr;

#[derive(Debug)]
pub enum ServerCommand {
    HandleRequest(Bytes, SocketAddr),
    SaveMessages,
    Shutdown,
}
