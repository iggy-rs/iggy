use crate::server_config::TcpConfig;
use crate::tcp::listener;
use std::sync::Arc;
use streaming::systems::system::System;
use tokio::sync::RwLock;
use tracing::info;

pub fn start(config: TcpConfig, system: Arc<RwLock<System>>) {
    info!("Initializing Iggy TCP server...");
    listener::start(&config.address, system);
    info!("Iggy TCP server has started on: {:?}", config.address);
}
