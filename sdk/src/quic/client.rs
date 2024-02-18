use crate::binary::binary_client::{BinaryClient, ClientState};
use crate::client::Client;
use crate::error::IggyError;
use crate::quic::config::QuicClientConfig;
use async_trait::async_trait;
use bytes::Bytes;
use quinn::{ClientConfig, Connection, Endpoint, IdleTimeout, RecvStream, VarInt};
use rustls::client::{ServerCertVerified, ServerCertVerifier};
use rustls::{Certificate, ServerName};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{error, info, trace};

const REQUEST_INITIAL_BYTES_LENGTH: usize = 4;
const RESPONSE_INITIAL_BYTES_LENGTH: usize = 8;
const NAME: &str = "Iggy";

/// QUIC client for interacting with the Iggy API.
#[derive(Debug)]
pub struct QuicClient {
    pub(crate) endpoint: Endpoint,
    pub(crate) connection: Mutex<Option<Connection>>,
    pub(crate) config: Arc<QuicClientConfig>,
    pub(crate) server_address: SocketAddr,
    pub(crate) state: Mutex<ClientState>,
}

unsafe impl Send for QuicClient {}
unsafe impl Sync for QuicClient {}

impl Default for QuicClient {
    fn default() -> Self {
        QuicClient::create(Arc::new(QuicClientConfig::default())).unwrap()
    }
}

#[async_trait]
impl Client for QuicClient {
    async fn connect(&self) -> Result<(), IggyError> {
        if self.get_state().await == ClientState::Connected {
            return Ok(());
        }

        let mut retry_count = 0;
        let connection;
        loop {
            info!(
                "{} client is connecting to server: {}...",
                NAME, self.config.server_address
            );
            let connection_result = self
                .endpoint
                .connect(self.server_address, &self.config.server_name)
                .unwrap()
                .await;

            if connection_result.is_err() {
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

                return Err(IggyError::NotConnected);
            }

            connection = connection_result.unwrap();
            break;
        }

        self.set_state(ClientState::Connected).await;
        self.connection.lock().await.replace(connection);

        Ok(())
    }

    async fn disconnect(&self) -> Result<(), IggyError> {
        if self.get_state().await == ClientState::Disconnected {
            return Ok(());
        }

        info!("{} client is disconnecting from server...", NAME);
        self.set_state(ClientState::Disconnected).await;
        self.connection.lock().await.take();
        self.endpoint.wait_idle().await;
        info!("{} client has disconnected from server.", NAME);
        Ok(())
    }
}

#[async_trait]
impl BinaryClient for QuicClient {
    async fn get_state(&self) -> ClientState {
        *self.state.lock().await
    }

    async fn set_state(&self, state: ClientState) {
        *self.state.lock().await = state;
    }

    async fn send_with_response(&self, command: u32, payload: Bytes) -> Result<Bytes, IggyError> {
        if self.get_state().await == ClientState::Disconnected {
            return Err(IggyError::NotConnected);
        }

        let connection = self.connection.lock().await;
        if let Some(connection) = connection.as_ref() {
            let payload_length = payload.len() + REQUEST_INITIAL_BYTES_LENGTH;

            let (mut send, mut recv) = connection.open_bi().await?;
            trace!("Sending a QUIC request...");
            send.write_all(&(payload_length as u32).to_le_bytes())
                .await?;
            send.write_all(&command.to_le_bytes()).await?;
            send.write_all(&payload).await?;
            send.finish().await?;
            trace!("Sent a QUIC request, waiting for a response...");
            return self.handle_response(&mut recv).await;
        }

        error!("Cannot send data. Client is not connected.");
        Err(IggyError::NotConnected)
    }
}

impl QuicClient {
    /// Creates a new QUIC client for the provided client and server addresses.
    pub fn new(
        client_address: &str,
        server_address: &str,
        server_name: &str,
        validate_certificate: bool,
    ) -> Result<Self, IggyError> {
        Self::create(Arc::new(QuicClientConfig {
            client_address: client_address.to_string(),
            server_address: server_address.to_string(),
            server_name: server_name.to_string(),
            validate_certificate,
            ..Default::default()
        }))
    }

    /// Create a new QUIC client for the provided configuration.
    pub fn create(config: Arc<QuicClientConfig>) -> Result<Self, IggyError> {
        let server_address = config.server_address.parse::<SocketAddr>()?;
        let client_address = if server_address.is_ipv6()
            && config.client_address == QuicClientConfig::default().client_address
        {
            "[::1]:0"
        } else {
            &config.client_address
        }
        .parse::<SocketAddr>()?;

        let quic_config = configure(&config)?;
        let endpoint = Endpoint::client(client_address);
        if endpoint.is_err() {
            error!("Cannot create client endpoint");
            return Err(IggyError::CannotCreateEndpoint);
        }

        let mut endpoint = endpoint.unwrap();
        endpoint.set_default_client_config(quic_config);

        Ok(Self {
            config,
            endpoint,
            server_address,
            connection: Mutex::new(None),
            state: Mutex::new(ClientState::Disconnected),
        })
    }

    async fn handle_response(&self, recv: &mut RecvStream) -> Result<Bytes, IggyError> {
        let buffer = recv
            .read_to_end(self.config.response_buffer_size as usize)
            .await?;
        if buffer.is_empty() {
            return Err(IggyError::EmptyResponse);
        }

        let status = u32::from_le_bytes(buffer[..4].try_into().unwrap());
        if status != 0 {
            error!(
                "Received an invalid response with status: {} ({}).",
                status,
                IggyError::from_code_as_string(status)
            );

            let length =
                u32::from_le_bytes(buffer[4..RESPONSE_INITIAL_BYTES_LENGTH].try_into().unwrap());
            let error_message = String::from_utf8_lossy(
                &buffer[RESPONSE_INITIAL_BYTES_LENGTH + 4
                    ..RESPONSE_INITIAL_BYTES_LENGTH + length as usize],
            )
            .to_string();

            return Err(IggyError::InvalidResponse(status, length, error_message));
        }

        let length =
            u32::from_le_bytes(buffer[4..RESPONSE_INITIAL_BYTES_LENGTH].try_into().unwrap());
        trace!("Status: OK. Response length: {}", length);
        if length <= 1 {
            return Ok(Bytes::new());
        }

        Ok(Bytes::copy_from_slice(
            &buffer[RESPONSE_INITIAL_BYTES_LENGTH..RESPONSE_INITIAL_BYTES_LENGTH + length as usize],
        ))
    }
}

fn configure(config: &QuicClientConfig) -> Result<ClientConfig, IggyError> {
    let max_concurrent_bidi_streams = VarInt::try_from(config.max_concurrent_bidi_streams);
    if max_concurrent_bidi_streams.is_err() {
        error!(
            "Invalid 'max_concurrent_bidi_streams': {}",
            config.max_concurrent_bidi_streams
        );
        return Err(IggyError::InvalidConfiguration);
    }

    let receive_window = VarInt::try_from(config.receive_window);
    if receive_window.is_err() {
        error!("Invalid 'receive_window': {}", config.receive_window);
        return Err(IggyError::InvalidConfiguration);
    }

    let mut transport = quinn::TransportConfig::default();
    transport.initial_mtu(config.initial_mtu);
    transport.send_window(config.send_window);
    transport.receive_window(receive_window.unwrap());
    transport.datagram_send_buffer_size(config.datagram_send_buffer_size as usize);
    transport.max_concurrent_bidi_streams(max_concurrent_bidi_streams.unwrap());
    if config.keep_alive_interval > 0 {
        transport.keep_alive_interval(Some(Duration::from_millis(config.keep_alive_interval)));
    }
    if config.max_idle_timeout > 0 {
        let max_idle_timeout =
            IdleTimeout::try_from(Duration::from_millis(config.max_idle_timeout));
        if max_idle_timeout.is_err() {
            error!("Invalid 'max_idle_timeout': {}", config.max_idle_timeout);
            return Err(IggyError::InvalidConfiguration);
        }
        transport.max_idle_timeout(Some(max_idle_timeout.unwrap()));
    }

    let mut client_config = match config.validate_certificate {
        true => ClientConfig::with_native_roots(),
        false => ClientConfig::new(Arc::new(
            rustls::ClientConfig::builder()
                .with_safe_defaults()
                .with_custom_certificate_verifier(SkipServerVerification::new())
                .with_no_client_auth(),
        )),
    };
    client_config.transport_config(Arc::new(transport));
    Ok(client_config)
}

#[derive(Debug)]
struct SkipServerVerification;

impl SkipServerVerification {
    fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

impl ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _: &Certificate,
        _: &[Certificate],
        _: &ServerName,
        _: &mut dyn Iterator<Item = &[u8]>,
        _: &[u8],
        _: SystemTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }
}
