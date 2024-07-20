use std::rc::Rc;

use crate::tcp::{tcp_listener, tcp_tls_listener};
use crate::tpc::shard::shard::IggyShard;
use iggy::error::IggyError;

/// Starts the TCP server.
pub async fn spawn_tcp_server(shard: Rc<IggyShard>) -> Result<(), IggyError> {
    let server_name = if shard.config.tcp.tls.enabled {
        "Iggy TCP TLS"
    } else {
        "Iggy TCP"
    };
    let server_name = server_name.to_owned();
    let result = match shard.config.tcp.tls.enabled {
        //true => tcp_tls_listener::start(server_name, shard).await,
        true => unimplemented!(),
        false => tcp_listener::start(server_name, shard).await,
    };
    result
}
