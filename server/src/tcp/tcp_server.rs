use crate::configs::tcp::TcpConfig;
use crate::streaming::systems::system::SharedSystem;
use crate::tcp::{tcp_listener, tcp_tls_listener};
use tracing::info;

/// Starts the TCP server.
/// Returns the address the server is listening on.
pub async fn start(config: TcpConfig, system: SharedSystem) {
    let server_name = if config.tls.enabled {
        "Iggy TCP TLS"
    } else {
        "Iggy TCP"
    };
    info!("Initializing {server_name} server...");
    if config.tls.enabled {
        tcp_tls_listener::start(&config.address, config.tls, system).await
    } else {
        tcp_listener::start(&config.address, system).await
    };
    info!("{server_name} server has started on: {:?}", &config.address);
}
