use crate::error::Error;
use bytes::Bytes;
use quinn::{ClientConfig, Connection, Endpoint, RecvStream};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{error, info, trace};

const NAME: &str = "Iggy";

pub struct Client {
    pub(crate) server: SocketAddr,
    pub(crate) server_name: String,
    pub(crate) endpoint: Endpoint,
    pub(crate) connection: Option<Connection>,
    pub(crate) buffer: [u8; 64 * 1024],
}

impl Client {
    pub async fn new(address: &str, server: &str, server_name: &str) -> Result<Self, Error> {
        let address = address.parse::<SocketAddr>()?;
        let server = server.parse::<SocketAddr>()?;
        let config = configure();
        let mut endpoint = Endpoint::client(address)?;
        endpoint.set_default_client_config(config);

        Ok(Self {
            endpoint,
            server,
            server_name: server_name.to_string(),
            connection: None,
            buffer: [0; 64 * 1024],
        })
    }

    pub async fn connect(&mut self) -> Result<(), Error> {
        let connection = self
            .endpoint
            .connect(self.server, &self.server_name)
            .unwrap()
            .await
            .unwrap();

        info!(
            "{} client has connected to server: {}",
            NAME,
            connection.remote_address()
        );

        self.connection = Some(connection);
        Ok(())
    }

    pub(crate) async fn send_with_response(&mut self, buffer: &[u8]) -> Result<&[u8], Error> {
        let connection = self.connection.as_mut().unwrap();
        let (mut send, mut recv) = connection.open_bi().await?;
        send.write_all(buffer).await?;
        send.finish().await?;
        self.handle_response(&mut recv).await
    }

    async fn handle_response(&mut self, recv: &mut RecvStream) -> Result<&[u8], Error> {
        // recv.read_chunk(&mut self.bytes).await?;
        let length = recv.read(&mut self.buffer).await?;
        if self.buffer.is_empty() {
            return Err(Error::EmptyResponse);
        }

        let status = self.buffer[0];
        if status == 0 {
            trace!("Status: OK.");
            return Ok(&self.buffer[1..length.unwrap()]);
        }

        error!("Received an invalid response with status: {:?}.", status);
        Err(Error::InvalidResponse(status))
    }
}

fn configure() -> ClientConfig {
    let crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(SkipServerVerification::new())
        .with_no_client_auth();

    ClientConfig::new(Arc::new(crypto))
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
