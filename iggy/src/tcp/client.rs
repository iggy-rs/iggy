use crate::binary::binary_client::BinaryClient;
use crate::client::Client;
use crate::error::Error;
use crate::tcp::config::TcpClientConfig;
use async_trait::async_trait;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::log::trace;
use tracing::{error, info};

const REQUEST_INITIAL_BYTES_LENGTH: usize = 4;
const RESPONSE_INITIAL_BYTES_LENGTH: usize = 8;
const EMPTY_RESPONSE: Vec<u8> = vec![];
const NAME: &str = "Iggy";

#[derive(Debug)]
pub struct TcpClient {
    pub(crate) server_address: SocketAddr,
    pub(crate) stream: Option<Mutex<TcpStream>>,
    pub(crate) config: Arc<TcpClientConfig>,
}

unsafe impl Send for TcpClient {}
unsafe impl Sync for TcpClient {}

#[async_trait]
impl Client for TcpClient {
    async fn connect(&mut self) -> Result<(), Error> {
        let mut retry_count = 0;
        let stream;
        loop {
            info!(
                "{} client is connecting to server: {}...",
                NAME, self.config.server_address
            );
            let connection = TcpStream::connect(self.server_address).await;
            if connection.is_err() {
                error!(
                    "Failed to connect to server: {}",
                    self.config.server_address
                );
                if retry_count < self.config.reconnection_retries {
                    retry_count += 1;
                    info!(
                        "Retrying to connect to server ({}/{}): {} in: {} ms...",
                        retry_count,
                        self.config.reconnection_retries,
                        self.config.server_address,
                        self.config.reconnection_interval
                    );
                    sleep(Duration::from_millis(self.config.reconnection_interval)).await;
                    continue;
                }

                return Err(Error::NotConnected);
            }

            stream = connection.unwrap();
            break;
        }

        let remote_address = stream.peer_addr()?;
        self.stream = Some(Mutex::new(stream));

        info!(
            "{} client has connected to server: {}",
            NAME, remote_address
        );

        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), Error> {
        info!("{} client is disconnecting from server...", NAME);
        self.stream = None;
        info!("{} client has disconnected from server.", NAME);
        Ok(())
    }
}

#[async_trait]
impl BinaryClient for TcpClient {
    async fn send_with_response(&self, command: u32, payload: &[u8]) -> Result<Vec<u8>, Error> {
        if let Some(stream) = &self.stream {
            let payload_length = payload.len() + 4;
            let mut buffer = Vec::with_capacity(REQUEST_INITIAL_BYTES_LENGTH + payload_length);
            buffer.extend((payload_length as u32).to_le_bytes());
            buffer.extend(command.to_le_bytes());
            buffer.extend(payload);

            let mut stream = stream.lock().await;
            trace!("Sending a TCP request...");
            stream.write_all(&buffer).await?;
            trace!("Sent a TCP request, waiting for a response...");

            let mut response_buffer = [0u8; RESPONSE_INITIAL_BYTES_LENGTH];
            let read_bytes = stream.read_exact(&mut response_buffer).await?;
            if read_bytes != RESPONSE_INITIAL_BYTES_LENGTH {
                error!("Received an invalid or empty response.");
                return Err(Error::EmptyResponse);
            }

            let status = u32::from_le_bytes(response_buffer[..4].try_into().unwrap());
            let length = u32::from_le_bytes(response_buffer[4..].try_into().unwrap());
            return self.handle_response(status, length, &mut stream).await;
        }

        error!("Cannot send data. Client is not connected.");
        Err(Error::NotConnected)
    }
}

impl TcpClient {
    pub fn new(server_address: &str) -> Result<Self, Error> {
        Self::create(Arc::new(TcpClientConfig {
            server_address: server_address.to_string(),
            ..Default::default()
        }))
    }

    pub fn create(config: Arc<TcpClientConfig>) -> Result<Self, Error> {
        let server_address = config.server_address.parse::<SocketAddr>()?;

        Ok(Self {
            config,
            server_address,
            stream: None,
        })
    }

    async fn handle_response(
        &self,
        status: u32,
        length: u32,
        stream: &mut TcpStream,
    ) -> Result<Vec<u8>, Error> {
        if status != 0 {
            error!("Received an invalid response with status: {:?}.", status);
            return Err(Error::InvalidResponse(status));
        }

        trace!("Status: OK. Response length: {}", length);
        if length <= 1 {
            return Ok(EMPTY_RESPONSE);
        }

        let mut response_buffer = vec![0u8; length as usize];
        stream.read_exact(&mut response_buffer).await?;
        Ok(response_buffer)
    }
}
