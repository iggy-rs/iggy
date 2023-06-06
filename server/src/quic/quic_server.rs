use crate::quic::listener;
use crate::server_config::QuicConfig;
use anyhow::Result;
use quinn::{Endpoint, IdleTimeout, VarInt};
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
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
    listener::start(endpoint, system);
    info!("Iggy QUIC server has started on: {:?}", config.address);
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
    transport.receive_window(VarInt::try_from(config.receive_window)?);
    transport.datagram_send_buffer_size(config.datagram_send_buffer_size);
    transport.max_concurrent_bidi_streams(VarInt::try_from(config.max_concurrent_bidi_streams)?);
    if config.keep_alive_interval > 0 {
        transport.keep_alive_interval(Some(Duration::from_millis(config.keep_alive_interval)));
    }
    if config.max_idle_timeout > 0 {
        let max_idle_timeout =
            IdleTimeout::try_from(Duration::from_millis(config.max_idle_timeout))?;
        transport.max_idle_timeout(Some(max_idle_timeout));
    }

    server_config.transport_config(Arc::new(transport));
    Ok(server_config)
}
