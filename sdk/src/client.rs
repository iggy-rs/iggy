use crate::error::Error;
use quinn::{ClientConfig, Connection, Endpoint, RecvStream};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{error, info, trace};

const NAME: &str = "Iggy";

pub struct Client {
    pub(crate) server: SocketAddr,
    pub(crate) server_name: String,
    pub(crate) endpoint: Endpoint,
}

pub struct ConnectedClient {
    pub(crate) endpoint: Endpoint,
    pub(crate) connection: Connection,
}

impl Client {
    pub fn new(address: &str, server: &str, server_name: &str) -> Result<Self, Error> {
        let client_address = address.parse::<SocketAddr>()?;
        let server_address = server.parse::<SocketAddr>()?;
        let config = configure();
        let mut endpoint = Endpoint::client(client_address)?;
        endpoint.set_default_client_config(config);

        Ok(Self {
            endpoint,
            server: server_address,
            server_name: server_name.to_string(),
        })
    }

    pub async fn connect(&self) -> Result<ConnectedClient, Error> {
        info!(
            "{} client is connecting to server: {}",
            NAME, self.server_name
        );
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

        Ok(ConnectedClient {
            endpoint: self.endpoint.clone(),
            connection,
        })
    }
}

impl ConnectedClient {
    pub async fn disconnect(&self) -> Result<(), Error> {
        info!("{} client is disconnecting from server.", NAME);
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
        let mut buffer = vec![0; 1024 * 1024];
        let length = recv.read(&mut buffer).await?;
        if buffer.is_empty() {
            return Err(Error::EmptyResponse);
        }

        let status = buffer[0];
        if status == 0 {
            trace!("Status: OK.");
            return Ok(buffer[1..length.unwrap()].to_vec());
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

    let mut transport = quinn::TransportConfig::default();
    transport.keep_alive_interval(Some(std::time::Duration::from_secs(5)));

    let mut config = ClientConfig::new(Arc::new(crypto));
    config.transport_config(Arc::new(transport));

    config
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
