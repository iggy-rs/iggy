use crate::server_error::ServerError;
use crate::shared::command;
use crate::tcp::tcp_sender::TcpSender;
use std::sync::Arc;
use streaming::system::System;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tracing::{error, info};

pub fn start(address: &str, system: Arc<RwLock<System>>) {
    let address = address.to_string();
    tokio::spawn(async move {
        let listener = TcpListener::bind(address).await;
        if listener.is_err() {
            panic!("Unable to start TCP server.");
        }

        let listener = listener.unwrap();
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("New connection: {}", addr);
                    let system = system.clone();
                    tokio::spawn(async move {
                        if let Err(error) = handle_connection(stream, system).await {
                            error!("Connection has failed: {}", error.to_string())
                        }
                    });
                }
                Err(error) => error!("Unable to accept socket, error: {}", error),
            }
        }
    });
}

async fn handle_connection(
    stream: TcpStream,
    system: Arc<RwLock<System>>,
) -> Result<(), ServerError> {
    // TODO: Refactor using the actual request length received from the client.
    let mut buffer = vec![0; 1024 * 1024];
    let mut sender = TcpSender { stream };

    loop {
        let length = sender.stream.read(&mut buffer).await;
        if length.is_err() {
            error!("Error when reading the TCP request: {:?}", length.err());
            continue;
        }

        let length = length.unwrap();
        info!("Received a TCP request, length: {}", length);
        if length == 0 {
            info!("Client disconnected");
            break;
        }

        let request = &buffer[..length];
        let result = command::handle(request, &mut sender, system.clone()).await;
        if result.is_err() {
            error!("Error when handling the TCP request: {:?}", result.err());
            continue;
        }
        info!("Sent a response");
    }
    Ok(())
}
