use crate::configs::tcp::TcpConfig;
use crate::streaming::systems::system::System;
use crate::tcp::{tcp_listener, tcp_tls_listener};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

pub fn start(config: TcpConfig, system: Arc<RwLock<System>>) {
    let server_name = if config.tls.enabled {
        "Iggy TCP TLS"
    } else {
        "Iggy TCP"
    };
    info!("Initializing {server_name} server...");
    match config.tls.enabled {
        true => {
            tcp_tls_listener::start(&config.address, config.tls, system);
        }
        false => {
            tcp_listener::start(&config.address, system);
        }
    }
    info!("{server_name} server has started on: {:?}", config.address);
}
