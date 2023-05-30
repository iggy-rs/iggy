use crate::quic::listener;
use anyhow::Result;
use quinn::Endpoint;
use std::error::Error;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;
use tracing::info;

pub fn start(address: String, system: Arc<RwLock<System>>) {
    info!("Initializing Iggy QUIC server...");
    let quic_config = configure_quic();
    if let Err(error) = quic_config {
        panic!("Error when configuring QUIC: {:?}", error);
    }

    let endpoint = Endpoint::server(quic_config.unwrap(), address.parse().unwrap()).unwrap();
    info!("Iggy QUIC server has started on: {:?}", address);
    listener::start(endpoint, system)
}

fn configure_quic() -> Result<quinn::ServerConfig, Box<dyn Error>> {
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let certificate_der = certificate.serialize_der().unwrap();
    let private_key = certificate.serialize_private_key_der();
    let private_key = rustls::PrivateKey(private_key);
    let cert_chain = vec![rustls::Certificate(certificate_der)];
    let server_config = quinn::ServerConfig::with_single_cert(cert_chain, private_key)?;
    Ok(server_config)
}
