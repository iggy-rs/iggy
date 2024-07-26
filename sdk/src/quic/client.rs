use crate::binary::binary_client::BinaryClient;
use crate::binary::{BinaryTransport, ClientState};
use crate::client::{AutoSignIn, Client, Credentials, PersonalAccessTokenClient, UserClient};
use crate::command::Command;
use crate::diagnostic::DiagnosticEvent;
use crate::error::IggyError;
use crate::quic::config::QuicClientConfig;
use async_trait::async_trait;
use bytes::Bytes;
use flume::{Receiver, Sender};
use quinn::crypto::rustls::QuicClientConfig as QuinnQuicClientConfig;
use quinn::{ClientConfig, Connection, Endpoint, IdleTimeout, RecvStream, VarInt};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, Error, SignatureScheme};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{error, info, trace, warn};

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
    events: (Sender<DiagnosticEvent>, Receiver<DiagnosticEvent>),
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
        QuicClient::connect(self).await
    }

    async fn disconnect(&self) -> Result<(), IggyError> {
        QuicClient::disconnect(self).await
    }

    async fn events(&self) -> Receiver<DiagnosticEvent> {
        self.events.1.clone()
    }
}

#[async_trait]
impl BinaryTransport for QuicClient {
    async fn get_state(&self) -> ClientState {
        *self.state.lock().await
    }

    async fn set_state(&self, state: ClientState) {
        *self.state.lock().await = state;
    }

    async fn send_with_response<T: Command>(&self, command: &T) -> Result<Bytes, IggyError> {
        command.validate()?;
        self.send_raw_with_response(command.code(), command.to_bytes())
            .await
    }

    async fn send_raw_with_response(&self, code: u32, payload: Bytes) -> Result<Bytes, IggyError> {
        let result = self.send_raw(code, payload.clone()).await;
        if result.is_ok() {
            return result;
        }

        let error = result.unwrap_err();
        if !matches!(error, IggyError::Disconnected | IggyError::EmptyResponse) {
            return Err(error);
        }

        if self.config.reconnection.enabled {
            self.disconnect().await?;
            info!("Reconnecting to the server...");
            self.connect().await?;
        }

        self.send_raw(code, payload).await
    }
}

impl BinaryClient for QuicClient {}

impl QuicClient {
    /// Creates a new QUIC client for the provided client and server addresses.
    pub fn new(
        client_address: &str,
        server_address: &str,
        server_name: &str,
        validate_certificate: bool,
        auto_sign_in: AutoSignIn,
    ) -> Result<Self, IggyError> {
        Self::create(Arc::new(QuicClientConfig {
            client_address: client_address.to_string(),
            server_address: server_address.to_string(),
            server_name: server_name.to_string(),
            validate_certificate,
            auto_sign_in,
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
            events: flume::unbounded(),
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

    async fn connect(&self) -> Result<(), IggyError> {
        let state = self.get_state().await;
        if matches!(state, ClientState::Connected | ClientState::Authenticated) {
            trace!("Client is already connected.");
            return Ok(());
        }

        if state == ClientState::Connecting {
            trace!("Client is already connecting.");
            return Ok(());
        }

        self.set_state(ClientState::Connecting).await;
        let mut retry_count = 0;
        let connection;
        let remote_address;
        loop {
            info!(
                "{NAME} client is connecting to server: {}...",
                self.config.server_address
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
                if !self.config.reconnection.enabled {
                    warn!("Automatic reconnection is disabled.");
                    return Err(IggyError::CannotEstablishConnection);
                }

                let unlimited_retries = self.config.reconnection.max_retries.is_none();
                let max_retries = self.config.reconnection.max_retries.unwrap_or_default();
                let max_retries_str =
                    if let Some(max_retries) = self.config.reconnection.max_retries {
                        max_retries.to_string()
                    } else {
                        "unlimited".to_string()
                    };

                let interval_str = self.config.reconnection.interval.as_human_time_string();
                if unlimited_retries || retry_count < max_retries {
                    retry_count += 1;
                    info!(
                        "Retrying to connect to server ({retry_count}/{max_retries_str}): {} in: {interval_str}",
                        self.config.server_address,
                    );
                    sleep(self.config.reconnection.interval.get_duration()).await;
                    continue;
                }

                self.set_state(ClientState::Disconnected).await;
                self.send_event(DiagnosticEvent::Disconnected).await;
                return Err(IggyError::CannotEstablishConnection);
            }

            connection = connection_result.unwrap();
            remote_address = connection.remote_address();
            break;
        }

        self.set_state(ClientState::Connected).await;
        self.connection.lock().await.replace(connection);
        self.send_event(DiagnosticEvent::Connected).await;

        info!("{NAME} client has connected to server: {remote_address}",);

        match &self.config.auto_sign_in {
            AutoSignIn::Disabled => {
                info!("Automatic sign-in is disabled.");
                Ok(())
            }
            AutoSignIn::Enabled(credentials) => {
                info!("{NAME} client is signing in...");
                match credentials {
                    Credentials::UsernamePassword(username, password) => {
                        self.login_user(username, password).await?;
                        Ok(())
                    }
                    Credentials::PersonalAccessToken(token) => {
                        self.login_with_personal_access_token(token).await?;
                        Ok(())
                    }
                }
            }
        }
    }

    async fn disconnect(&self) -> Result<(), IggyError> {
        if self.get_state().await == ClientState::Disconnected {
            return Ok(());
        }

        info!("{} client is disconnecting from server...", NAME);
        self.set_state(ClientState::Disconnected).await;
        self.connection.lock().await.take();
        self.endpoint.wait_idle().await;
        self.send_event(DiagnosticEvent::Disconnected).await;
        info!("{} client has disconnected from server.", NAME);
        Ok(())
    }

    async fn send_raw(&self, code: u32, payload: Bytes) -> Result<Bytes, IggyError> {
        if self.get_state().await == ClientState::Disconnected {
            trace!("Cannot send data. Client is not connected.");
            return Err(IggyError::NotConnected);
        }

        let connection = self.connection.lock().await;
        if let Some(connection) = connection.as_ref() {
            let payload_length = payload.len() + REQUEST_INITIAL_BYTES_LENGTH;
            let (mut send, mut recv) = connection.open_bi().await?;
            trace!("Sending a QUIC request...");
            send.write_all(&(payload_length as u32).to_le_bytes())
                .await?;
            send.write_all(&code.to_le_bytes()).await?;
            send.write_all(&payload).await?;
            send.finish()?;
            trace!("Sent a QUIC request, waiting for a response...");
            return self.handle_response(&mut recv).await;
        }

        error!("Cannot send data. Client is not connected.");
        Err(IggyError::NotConnected)
    }

    async fn send_event(&self, event: DiagnosticEvent) {
        if let Err(error) = self.events.0.send_async(event).await {
            error!("Failed to send a QUIC diagnostic event: {error}");
        }
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

    if CryptoProvider::get_default().is_none() {
        rustls::crypto::ring::default_provider()
            .install_default()
            .expect("Failed to install rustls crypto provider");
    }
    let mut client_config = match config.validate_certificate {
        true => ClientConfig::with_platform_verifier(),
        false => {
            match QuinnQuicClientConfig::try_from(
                rustls::ClientConfig::builder()
                    .dangerous()
                    .with_custom_certificate_verifier(SkipServerVerification::new())
                    .with_no_client_auth(),
            ) {
                Ok(config) => ClientConfig::new(Arc::new(config)),
                Err(error) => {
                    error!("Failed to create QUIC client configuration: {error}");
                    return Err(IggyError::InvalidConfiguration);
                }
            }
        }
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
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        // Signature used by the server to sign self-signed certificate (using rcgen)
        vec![SignatureScheme::ECDSA_NISTP256_SHA256]
    }
}
