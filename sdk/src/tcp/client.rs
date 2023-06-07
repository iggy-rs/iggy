use crate::binary::binary_client::BinaryClient;
use crate::client::Client;
use crate::error::Error;
use crate::tcp::config::TcpClientConfig;
use async_trait::async_trait;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::{error, info};

const EMPTY_RESPONSE: Vec<u8> = vec![];

const NAME: &str = "Iggy";

#[derive(Debug)]
pub struct TcpClient {
    pub(crate) server_address: SocketAddr,
    pub(crate) stream: Option<Mutex<TcpStream>>,
    pub(crate) config: TcpClientConfig,
}

unsafe impl Send for TcpClient {}
unsafe impl Sync for TcpClient {}

impl TcpClient {
    pub fn new(server_address: &str) -> Result<Self, Error> {
        Self::create(TcpClientConfig {
            server_address: server_address.to_string(),
        })
    }

    pub fn create(config: TcpClientConfig) -> Result<Self, Error> {
        let server_address = config.server_address.parse::<SocketAddr>()?;

        Ok(Self {
            config,
            server_address,
            stream: None,
        })
    }

    async fn handle_response(&self, data: &[u8]) -> Result<Vec<u8>, Error> {
        if data.is_empty() {
            return Err(Error::EmptyResponse);
        }

        let status = data[0];
        if status != 0 {
            error!("Received an invalid response with status: {:?}.", status);
            return Err(Error::InvalidResponse(status));
        }

        let length = data.len();
        info!("Status: OK. Response length: {}", length);

        if length <= 1 {
            return Ok(EMPTY_RESPONSE);
        }

        Ok(data[1..length].to_vec())
    }
}

#[async_trait]
impl Client for TcpClient {
    async fn connect(&mut self) -> Result<(), Error> {
        info!(
            "{} client is connecting to server: {}",
            NAME, self.config.server_address
        );
        let stream = TcpStream::connect(self.server_address).await?;
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
    async fn send_with_response(&self, buffer: &[u8]) -> Result<Vec<u8>, Error> {
        if let Some(stream) = &self.stream {
            let mut stream = stream.lock().await;
            stream.write_all(buffer).await?;
            stream.flush().await?;
            info!("Sent a TCP request, waiting for a response...");
            // TODO: Refactor using the actual response length received from the server.
            let mut buffer = vec![0; 1024 * 1024];
            let length = stream.read(&mut buffer).await?;
            return self.handle_response(&buffer[..length]).await;
        }

        error!("Cannot send data. Client is not connected.");
        Err(Error::NotConnected)
    }
}
