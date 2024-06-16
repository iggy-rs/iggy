use crate::configs::tcp::TcpConfig;
use crate::streaming::systems::system::SharedSystem;
use crate::tcp::{tcp_listener, tcp_tls_listener};
use crate::tpc::shard::shard::IggyShard;
use std::net::SocketAddr;
use tracing::info;

/// Starts the TCP server.
/// Returns the address the server is listening on.
pub async fn start(shard: IggyShard) -> SocketAddr {
    /*
    let server_name = if config.tls.enabled {
        "Iggy TCP TLS"
    } else {
        "Iggy TCP"
    };
    info!("Initializing {server_name} server...");
    let addr = match config.tls.enabled {
        true => tcp_tls_listener::start(&config.address, config.tls, system).await,
        false => tcp_listener::start(&config.address, system).await,
    };
    */
    let server_name = "Iggy TCP";
    let addr = tcp_listener::start("127.0.0.1:2137", shard).await;
    info!("{server_name} server has started on: {:?}", addr);
    addr
}
