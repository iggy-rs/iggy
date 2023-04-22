use quinn::{Endpoint, ServerConfig};
use std::error::Error;
use std::net::SocketAddr;
use tracing::info;

pub async fn start(address: SocketAddr) -> Result<(), Box<dyn Error>> {
    let config = configure()?;
    let endpoint = Endpoint::server(config, address)?;
    let connection = endpoint.accept().await.unwrap();
    let connection = connection.await.unwrap();
    info!(
        "Connection accepted from client: {}",
        connection.remote_address()
    );
    Ok(())
}

fn configure() -> Result<ServerConfig, Box<dyn Error>> {
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let certificate_der = certificate.serialize_der().unwrap();
    let private_key = certificate.serialize_private_key_der();
    let private_key = rustls::PrivateKey(private_key);
    let cert_chain = vec![rustls::Certificate(certificate_der)];
    let server_config = ServerConfig::with_single_cert(cert_chain, private_key)?;
    Ok(server_config)
}
