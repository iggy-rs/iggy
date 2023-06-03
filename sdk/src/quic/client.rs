use crate::client::Client;
use crate::error::Error;
use crate::quic::config::Config;
use quinn::{ClientConfig, Connection, Endpoint, RecvStream, VarInt};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{error, info, trace};

const EMPTY_RESPONSE: Vec<u8> = vec![];

const NAME: &str = "Iggy";

#[derive(Debug)]
pub struct QuicBaseClient {
    pub(crate) config: Config,
    pub(crate) endpoint: Endpoint,
    pub(crate) server_address: SocketAddr,
}

#[derive(Debug)]
pub struct QuicClient {
    pub(crate) endpoint: Endpoint,
    pub(crate) connection: Connection,
    pub(crate) config: Config,
}

impl Client for QuicClient {}

unsafe impl Send for QuicClient {}
unsafe impl Sync for QuicClient {}

impl QuicBaseClient {
    pub fn new(
        client_address: &str,
        server_address: &str,
        server_name: &str,
    ) -> Result<Self, Error> {
        QuicBaseClient::create(Config {
            client_address: client_address.to_string(),
            server_address: server_address.to_string(),
            server_name: server_name.to_string(),
            ..Default::default()
        })
    }

    pub fn create(config: Config) -> Result<Self, Error> {
        let client_address = config.client_address.parse::<SocketAddr>()?;
        let server_address = config.server_address.parse::<SocketAddr>()?;
        let quic_config = configure(&config)?;
        let endpoint = Endpoint::client(client_address);
        if endpoint.is_err() {
            error!("Cannot create client endpoint");
            return Err(Error::CannotCreateEndpoint);
        }

        let mut endpoint = endpoint.unwrap();
        endpoint.set_default_client_config(quic_config);

        Ok(Self {
            config,
            endpoint,
            server_address,
        })
    }

    pub async fn connect(&self) -> Result<QuicClient, Error> {
        info!(
            "{} client is connecting to server: {}",
            NAME, self.config.server_name
        );
        let connection = self
            .endpoint
            .connect(self.server_address, &self.config.server_name)
            .unwrap()
            .await
            .unwrap();

        info!(
            "{} client has connected to server: {}",
            NAME,
            connection.remote_address()
        );

        Ok(QuicClient {
            endpoint: self.endpoint.clone(),
            connection,
            config: self.config.clone(),
        })
    }
}

impl QuicClient {
    pub async fn disconnect(&self) -> Result<(), Error> {
        info!("{} client is disconnecting from server...", NAME);
        self.endpoint.wait_idle().await;
        info!("{} client has disconnected from server.", NAME);
        Ok(())
    }

    pub(crate) async fn send_with_response(&self, buffer: &[u8]) -> Result<Vec<u8>, Error> {
        let (mut send, mut recv) = self.connection.open_bi().await?;
        send.write_all(buffer).await?;
        send.finish().await?;
        self.handle_response(&mut recv).await
    }

    async fn handle_response(&self, recv: &mut RecvStream) -> Result<Vec<u8>, Error> {
        let buffer = recv
            .read_to_end(self.config.response_buffer_size as usize)
            .await?;
        if buffer.is_empty() {
            return Err(Error::EmptyResponse);
        }

        let status = buffer[0];
        if status != 0 {
            error!("Received an invalid response with status: {:?}.", status);
            return Err(Error::InvalidResponse(status));
        }

        let length = buffer.len();
        trace!("Status: OK. Response length: {}", length);

        if length <= 1 {
            return Ok(EMPTY_RESPONSE);
        }

        Ok(buffer[1..length].to_vec())
    }
}

fn configure(config: &Config) -> Result<ClientConfig, Error> {
    let max_concurrent_bidi_streams = VarInt::try_from(config.max_concurrent_bidi_streams);
    if max_concurrent_bidi_streams.is_err() {
        error!(
            "Invalid 'max_concurrent_bidi_streams': {}",
            config.max_concurrent_bidi_streams
        );
        return Err(Error::InvalidConfiguration);
    }

    let crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(SkipServerVerification::new())
        .with_no_client_auth();

    let mut transport = quinn::TransportConfig::default();
    transport.initial_mtu(config.initial_mtu);
    transport.send_window(config.send_window);
    transport.receive_window(VarInt::try_from(config.receive_window).unwrap());
    transport.datagram_send_buffer_size(config.datagram_send_buffer_size);
    transport.max_concurrent_bidi_streams(max_concurrent_bidi_streams.unwrap());
    transport.keep_alive_interval(Some(std::time::Duration::from_secs(5)));

    let mut config = ClientConfig::new(Arc::new(crypto));
    config.transport_config(Arc::new(transport));

    Ok(config)
}

struct SkipServerVerification;

impl SkipServerVerification {
    fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

impl rustls::client::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}
