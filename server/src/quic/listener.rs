use crate::quic::quic_command::QuicCommand;
use crate::server_error::ServerError;
use flume::Sender;
use quinn::Endpoint;
use std::sync::Arc;
use tracing::{error, info};

pub async fn start(endpoint: Endpoint, sender: Sender<QuicCommand>) -> Result<(), ServerError> {
    let sender = Arc::new(sender);
    while let Some(incoming_connection) = endpoint.accept().await {
        info!(
            "Incoming connection from client: {}",
            incoming_connection.remote_address()
        );
        let future = handle_connection(incoming_connection, sender.clone());
        tokio::spawn(async move {
            if let Err(error) = future.await {
                error!("Connection has failed: {}", error.to_string())
            }
        });
    }

    Ok(())
}

async fn handle_connection(
    incoming_connection: quinn::Connecting,
    sender: Arc<Sender<QuicCommand>>,
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

            if let Err(error) = sender
                .send_async(QuicCommand {
                    send: stream.0,
                    recv: stream.1,
                })
                .await
            {
                error!("Error when handling the request: {:?}", error);
            }
        }
    }
    .await?;
    Ok(())
}
