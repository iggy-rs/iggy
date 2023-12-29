use crate::binary::command;
use crate::quic::quic_sender::QuicSender;
use crate::server_error::ServerError;
use crate::streaming::clients::client_manager::Transport;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use iggy::bytes_serializable::BytesSerializable;
use iggy::command::Command;
use quinn::Endpoint;
use tracing::{debug, error, info};

const LISTENERS_COUNT: u32 = 10;
const INITIAL_BYTES_LENGTH: usize = 4;

pub fn start(endpoint: Endpoint, system: SharedSystem) {
    for _ in 0..LISTENERS_COUNT {
        let endpoint = endpoint.clone();
        let system = system.clone();
        tokio::spawn(async move {
            while let Some(incoming_connection) = endpoint.accept().await {
                info!(
                    "Incoming connection from client: {}",
                    incoming_connection.remote_address()
                );
                let system = system.clone();
                tokio::spawn(async move {
                    if let Err(error) = handle_connection(incoming_connection, system).await {
                        error!("Connection has failed: {error}");
                    }
                });
            }
        });
    }
}

async fn handle_connection(
    incoming_connection: quinn::Connecting,
    system: SharedSystem,
) -> Result<(), ServerError> {
    let connection = incoming_connection.await?;
    let address = connection.remote_address();
    async {
        info!("Client has connected: {address}");
        let client_id = system
            .read()
            .add_client(&address, Transport::Quic)
            .await;
        let mut session = Session::from_client_id(client_id, address);
        loop {
            let stream = connection.accept_bi().await;
            let mut stream = match stream {
                Err(quinn::ConnectionError::ApplicationClosed { .. }) => {
                    info!("Connection closed");
                    system.read().delete_client(&address).await;
                    return Ok(());
                }
                Err(error) => {
                    error!("Error when handling QUIC stream: {:?}", error);
                    system.read().delete_client(&address).await;
                    return Err(error);
                }
                Ok(stream) => stream,
            };

            let request = stream.1.read_to_end(10 * 1000 * 1000).await;
            if request.is_err() {
                error!("Error when reading the QUIC request: {:?}", request.err());
                continue;
            }

            let request = request.unwrap();
            if request.len() < INITIAL_BYTES_LENGTH {
                error!(
                "Unable to read the QUIC request length, expected: {INITIAL_BYTES_LENGTH} bytes, received: {} bytes.",
                request.len()
            );
                continue;
            }

            debug!("Trying to read command...");
            let length = &request[..INITIAL_BYTES_LENGTH];
            let length = u32::from_le_bytes(length.try_into().unwrap_or([0; 4]));
            let command = Command::from_bytes(&request[INITIAL_BYTES_LENGTH..]);
            if command.is_err() {
                error!(
                    "Error when reading the QUIC request command: {:?}",
                    command.err()
                );
                continue;
            }

            let command = command.unwrap();
            debug!("Received a QUIC command: {command}, payload size: {length}");

            let result = command::handle(
                &command,
                &mut QuicSender {
                    send: stream.0,
                    recv: stream.1,
                },
                &mut session,
                system.clone(),
            )
            .await;
            if result.is_err() {
                error!("Error when handling the QUIC request: {:?}", result.err());
                continue;
            }
        }
    }
    .await?;
    Ok(())
}
