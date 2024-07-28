use crate::binary::binary_client::BinaryClient;
use crate::binary::{BinaryTransport, ClientState};
use crate::client::{AutoSignIn, Client, Credentials, PersonalAccessTokenClient, UserClient};
use crate::command::Command;
use crate::diagnostic::DiagnosticEvent;
use crate::error::{IggyError, IggyErrorDiscriminants};
use crate::tcp::config::TcpClientConfig;
use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use flume::{Receiver, Sender};
use std::fmt::Debug;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tokio_native_tls::native_tls::TlsConnector;
use tokio_native_tls::TlsStream;
use tracing::{error, info, trace, warn};

const REQUEST_INITIAL_BYTES_LENGTH: usize = 4;
const RESPONSE_INITIAL_BYTES_LENGTH: usize = 8;
const NAME: &str = "Iggy";

/// TCP client for interacting with the Iggy API.
/// It requires a valid server address.
#[derive(Debug)]
pub struct TcpClient {
    pub(crate) stream: Mutex<Option<Box<dyn ConnectionStream>>>,
    pub(crate) config: Arc<TcpClientConfig>,
    pub(crate) state: Mutex<ClientState>,
    events: (Sender<DiagnosticEvent>, Receiver<DiagnosticEvent>),
}

unsafe impl Send for TcpClient {}
unsafe impl Sync for TcpClient {}

#[async_trait]
pub(crate) trait ConnectionStream: Debug + Sync + Send {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, IggyError>;
    async fn write(&mut self, buf: &[u8]) -> Result<(), IggyError>;
    async fn flush(&mut self) -> Result<(), IggyError>;
}

#[derive(Debug)]
struct TcpConnectionStream {
    reader: BufReader<OwnedReadHalf>,
    writer: BufWriter<OwnedWriteHalf>,
}

impl TcpConnectionStream {
    pub fn new(stream: TcpStream) -> Self {
        let (reader, writer) = stream.into_split();
        Self {
            reader: BufReader::new(reader),
            writer: BufWriter::new(writer),
        }
    }
}

#[derive(Debug)]
pub(crate) struct TcpTlsConnectionStream {
    stream: TlsStream<TcpStream>,
}

unsafe impl Send for TcpConnectionStream {}
unsafe impl Sync for TcpConnectionStream {}

unsafe impl Send for TcpTlsConnectionStream {}
unsafe impl Sync for TcpTlsConnectionStream {}

#[async_trait]
impl ConnectionStream for TcpConnectionStream {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, IggyError> {
        let result = self.reader.read_exact(buf).await;
        if let Err(error) = result {
            return Err(IggyError::from(error));
        }

        Ok(result.unwrap())
    }

    async fn write(&mut self, buf: &[u8]) -> Result<(), IggyError> {
        Ok(self.writer.write_all(buf).await?)
    }

    async fn flush(&mut self) -> Result<(), IggyError> {
        Ok(self.writer.flush().await?)
    }
}

#[async_trait]
impl ConnectionStream for TcpTlsConnectionStream {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, IggyError> {
        let result = self.stream.read_exact(buf).await;
        if let Err(error) = result {
            return Err(IggyError::from(error));
        }

        Ok(result.unwrap())
    }

    async fn write(&mut self, buf: &[u8]) -> Result<(), IggyError> {
        let result = self.stream.write_all(buf).await;
        if let Err(error) = result {
            return Err(IggyError::from(error));
        }

        Ok(())
    }

    async fn flush(&mut self) -> Result<(), IggyError> {
        Ok(())
    }
}

impl Default for TcpClient {
    fn default() -> Self {
        TcpClient::create(Arc::new(TcpClientConfig::default())).unwrap()
    }
}

#[async_trait]
impl Client for TcpClient {
    async fn connect(&self) -> Result<(), IggyError> {
        TcpClient::connect(self).await
    }

    async fn disconnect(&self) -> Result<(), IggyError> {
        TcpClient::disconnect(self).await
    }

    async fn subscribe_events(&self) -> Receiver<DiagnosticEvent> {
        self.events.1.clone()
    }
}

#[async_trait]
impl BinaryTransport for TcpClient {
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

    async fn publish_event(&self, event: DiagnosticEvent) {
        if let Err(error) = self.events.0.send_async(event).await {
            error!("Failed to send a TCP diagnostic event: {error}");
        }
    }
}

impl BinaryClient for TcpClient {}

impl TcpClient {
    /// Create a new TCP client for the provided server address.
    pub fn new(server_address: &str, auto_sign_in: AutoSignIn) -> Result<Self, IggyError> {
        Self::create(Arc::new(TcpClientConfig {
            server_address: server_address.to_string(),
            auto_sign_in,
            ..Default::default()
        }))
    }

    /// Create a new TCP client for the provided server address using TLS.
    pub fn new_tls(
        server_address: &str,
        domain: &str,
        auto_sign_in: AutoSignIn,
    ) -> Result<Self, IggyError> {
        Self::create(Arc::new(TcpClientConfig {
            server_address: server_address.to_string(),
            tls_enabled: true,
            tls_domain: domain.to_string(),
            auto_sign_in,
            ..Default::default()
        }))
    }

    /// Create a new TCP client based on the provided configuration.
    pub fn create(config: Arc<TcpClientConfig>) -> Result<Self, IggyError> {
        Ok(Self {
            config,
            stream: Mutex::new(None),
            state: Mutex::new(ClientState::Disconnected),
            events: flume::unbounded(),
        })
    }

    async fn handle_response(
        &self,
        status: u32,
        length: u32,
        stream: &mut dyn ConnectionStream,
    ) -> Result<Bytes, IggyError> {
        if status != 0 {
            // TEMP: See https://github.com/iggy-rs/iggy/pull/604 for context.
            if status == IggyErrorDiscriminants::TopicIdAlreadyExists as u32
                || status == IggyErrorDiscriminants::TopicNameAlreadyExists as u32
                || status == IggyErrorDiscriminants::StreamIdAlreadyExists as u32
                || status == IggyErrorDiscriminants::StreamNameAlreadyExists as u32
                || status == IggyErrorDiscriminants::UserAlreadyExists as u32
                || status == IggyErrorDiscriminants::PersonalAccessTokenAlreadyExists as u32
                || status == IggyErrorDiscriminants::ConsumerGroupIdAlreadyExists as u32
                || status == IggyErrorDiscriminants::ConsumerGroupNameAlreadyExists as u32
            {
                tracing::debug!(
                    "Received a server resource already exists response: {} ({})",
                    status,
                    IggyError::from_code_as_string(status)
                )
            } else {
                error!(
                    "Received an invalid response with status: {} ({}).",
                    status,
                    IggyError::from_code_as_string(status),
                );
            }

            let mut error_details_buffer = BytesMut::with_capacity(length as usize);
            error_details_buffer.put_bytes(0, length as usize);
            stream.read(&mut error_details_buffer).await?;

            let string_length = error_details_buffer.get_u32_le();
            let error_message = String::from_utf8_lossy(&error_details_buffer);

            return Err(IggyError::InvalidResponse(
                status,
                string_length,
                error_message.to_string(),
            ));
        }

        trace!("Status: OK. Response length: {}", length);
        if length <= 1 {
            return Ok(Bytes::new());
        }

        let mut response_buffer = BytesMut::with_capacity(length as usize);
        response_buffer.put_bytes(0, length as usize);
        stream.read(&mut response_buffer).await?;
        Ok(response_buffer.freeze())
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
        let tls_enabled = self.config.tls_enabled;
        let mut retry_count = 0;
        let connection_stream: Box<dyn ConnectionStream>;
        let remote_address;
        loop {
            info!(
                "{NAME} client is connecting to server: {}...",
                self.config.server_address
            );

            let connection = TcpStream::connect(&self.config.server_address).await;
            if connection.is_err() {
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
                self.publish_event(DiagnosticEvent::Disconnected).await;
                return Err(IggyError::CannotEstablishConnection);
            }

            let stream = connection.unwrap();
            remote_address = stream.peer_addr()?;

            if !tls_enabled {
                connection_stream = Box::new(TcpConnectionStream::new(stream));
                break;
            }

            let connector =
                tokio_native_tls::TlsConnector::from(TlsConnector::builder().build().unwrap());
            let stream = tokio_native_tls::TlsConnector::connect(
                &connector,
                &self.config.tls_domain,
                stream,
            )
            .await
            .map_err(|error| {
                error!("Failed to establish a TLS connection: {error}");
                IggyError::CannotEstablishConnection
            })?;

            connection_stream = Box::new(TcpTlsConnectionStream { stream });
            break;
        }

        info!("{NAME} client has connected to server: {remote_address}",);
        self.stream.lock().await.replace(connection_stream);
        self.set_state(ClientState::Connected).await;
        self.publish_event(DiagnosticEvent::Connected).await;
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
                        self.publish_event(DiagnosticEvent::SignedIn).await;
                        info!("{NAME} client has signed in with the user credentials.",);
                        Ok(())
                    }
                    Credentials::PersonalAccessToken(token) => {
                        self.login_with_personal_access_token(token).await?;
                        self.publish_event(DiagnosticEvent::SignedIn).await;
                        info!("{NAME} client has signed in with a personal access token.",);
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
        self.stream.lock().await.take();
        self.publish_event(DiagnosticEvent::Disconnected).await;
        info!("{} client has disconnected from server.", NAME);
        Ok(())
    }

    async fn send_raw(&self, code: u32, payload: Bytes) -> Result<Bytes, IggyError> {
        if self.get_state().await == ClientState::Disconnected {
            trace!("Cannot send data. Client is not connected.");
            return Err(IggyError::NotConnected);
        }

        info!("Sending a TCP request with code: {code}");
        let mut stream = self.stream.lock().await;
        if let Some(stream) = stream.as_mut() {
            let payload_length = payload.len() + REQUEST_INITIAL_BYTES_LENGTH;
            trace!("Sending a TCP request...");
            stream.write(&(payload_length as u32).to_le_bytes()).await?;
            stream.write(&code.to_le_bytes()).await?;
            stream.write(&payload).await?;
            stream.flush().await?;
            trace!("Sent a TCP request, waiting for a response...");

            let mut response_buffer = [0u8; RESPONSE_INITIAL_BYTES_LENGTH];
            let read_bytes = stream.read(&mut response_buffer).await;
            if let Err(error) = read_bytes {
                error!("Failed to read response: {}", error);
                return Err(IggyError::Disconnected);
            }

            let read_bytes = read_bytes.unwrap();
            if read_bytes != RESPONSE_INITIAL_BYTES_LENGTH {
                error!("Received an invalid or empty response.");
                return Err(IggyError::EmptyResponse);
            }

            let status = u32::from_le_bytes(response_buffer[..4].try_into().unwrap());
            let length = u32::from_le_bytes(response_buffer[4..].try_into().unwrap());
            return self.handle_response(status, length, stream.as_mut()).await;
        }

        error!("Cannot send data. Client is not connected.");
        Err(IggyError::NotConnected)
    }
}
