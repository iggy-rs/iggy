use std::rc::Rc;

use iggy::error::IggyError;

use crate::tpc::shard::shard::IggyShard;

pub mod connection_handler;
pub mod sender;
pub mod tcp_listener;
mod tcp_sender;
pub mod tcp_server;
pub mod tcp_tls_listener;
pub mod tcp_tls_sender;


pub(crate) async fn persist_tcp_address(shard: &Rc<IggyShard>, local_addr: String) -> Result<(), IggyError> {
    let mut current_config = shard.config.clone();
    current_config.tcp.address = local_addr;
    let runtime_path = shard.config.system.get_runtime_path();
    let current_config_path = format!("{}/current_config.toml", runtime_path);
    let current_config_content =
        toml::to_string(&current_config).expect("Cannot serialize current_config");
    monoio::fs::write(
        current_config_path,
        current_config_content.as_bytes().to_vec(),
    )
    .await
    .0?;
    drop(current_config);
    Ok(())
}