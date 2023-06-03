use crate::quic::listener;
use crate::server_config::QuicConfig;
use anyhow::Result;
use quinn::{Endpoint, VarInt};
use std::error::Error;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;
use tracing::info;

pub fn start(config: QuicConfig, system: Arc<RwLock<System>>) {
    info!("Initializing Iggy QUIC server...");
    let quic_config = configure_quic(&config);
    if let Err(error) = quic_config {
        panic!("Error when configuring QUIC: {:?}", error);
    }

    let endpoint = Endpoint::server(quic_config.unwrap(), config.address.parse().unwrap()).unwrap();
    info!("Iggy QUIC server has started on: {:?}", config.address);
    listener::start(endpoint, system)
}

fn configure_quic(config: &QuicConfig) -> Result<quinn::ServerConfig, Box<dyn Error>> {
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let certificate_der = certificate.serialize_der().unwrap();
    let private_key = certificate.serialize_private_key_der();
    let private_key = rustls::PrivateKey(private_key);
    let cert_chain = vec![rustls::Certificate(certificate_der)];
    let mut server_config = quinn::ServerConfig::with_single_cert(cert_chain, private_key)?;
    let mut transport = quinn::TransportConfig::default();
    transport.initial_mtu(config.initial_mtu);
    transport.send_window(config.send_window);
    transport.receive_window(VarInt::try_from(config.receive_window).unwrap());
    transport.datagram_send_buffer_size(config.datagram_send_buffer_size);
    transport.max_concurrent_bidi_streams(VarInt::try_from(config.max_concurrent_bidi_streams)?);
    server_config.transport_config(Arc::new(transport));
    Ok(server_config)
}
