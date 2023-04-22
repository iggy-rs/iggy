use quinn::{ClientConfig, Endpoint};
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;

pub async fn start(server: SocketAddr) -> Result<(), Box<dyn Error>> {
    let config = configure();
    let mut endpoint = Endpoint::client("127.0.0.1:0".parse().unwrap())?;
    endpoint.set_default_client_config(config);

    let connection = endpoint
        .connect(server, "localhost")
        .unwrap()
        .await
        .unwrap();
    info!("Connected to server: {}", connection.remote_address());
    drop(connection);
    endpoint.wait_idle().await;
    Ok(())
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
