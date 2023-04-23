use crate::server::Server;
use crate::server_command::ServerCommand;
use crate::server_error::ServerError;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info};

impl Server {
    pub async fn start_listener(&self) -> Result<(), ServerError> {
        while let Some(incoming_connection) = self.endpoint.accept().await {
            info!(
                "Incoming connection from client: {}",
                incoming_connection.remote_address()
            );
            let future = handle_connection(incoming_connection, self.sender.clone());
            tokio::spawn(async move {
                if let Err(error) = future.await {
                    error!("Connection has failed: {}", error.to_string())
                }
            });
        }

        Ok(())
    }
}

async fn handle_connection(
    incoming_connection: quinn::Connecting,
    sender: Arc<mpsc::Sender<ServerCommand>>,
) -> Result<(), ServerError> {
    let connection = incoming_connection.await?;
    async {
        info!("Client has connected: {}", connection.remote_address());
        loop {
            let stream = connection.accept_bi().await;
            let stream = match stream {
                Err(quinn::ConnectionError::ApplicationClosed { .. }) => {
                    info!("Connection closed");
                    return Ok(());
                }
                Err(error) => {
                    return Err(error);
                }
                Ok(stream) => stream,
            };

            if let Err(error) = sender.send(ServerCommand::HandleRequest(stream)).await {
                error!("Error when handling the request: {:?}", error);
            }
        }
    }
    .await?;
    Ok(())
}
