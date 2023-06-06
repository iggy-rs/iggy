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
            let result = listener.accept().await;
            if result.is_err() {
                error!("Unable to accept socket: {:?}", result);
                continue;
            }

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
                Err(error) => error!("Unable to accept socket: {:?}, error: {}", result, error),
            }
        }
    });
}

async fn handle_connection(
    stream: TcpStream,
    system: Arc<RwLock<System>>,
) -> Result<(), ServerError> {
    let mut buffer = Vec::new();
    let mut sender = TcpSender { stream };
    loop {
        let length = sender.stream.read_to_end(&mut buffer).await;
        if length.is_err() {
            error!("Error when reading the TCP request: {:?}", length.err());
            continue;
        }

        let length = length.unwrap();
        if length == 0 {
            info!("Connection closed");
            break;
        }

        let request = &buffer[..length];
        let result = command::handle(request, &mut sender, system.clone()).await;
        if result.is_err() {
            error!("Error when handling the TCP request: {:?}", result.err());
            continue;
        }
    }
    Ok(())
}
