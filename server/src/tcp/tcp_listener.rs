use crate::tcp::connection_handler::{handle_connection, handle_error};
use crate::tcp::persist_tcp_address;
use crate::tcp::tcp_sender::TcpSender;
use crate::tpc::shard::shard::IggyShard;
use iggy::error::IggyError;
use monoio::net::TcpListener;
use std::rc::Rc;
use tracing::{error, info};

pub async fn start(server_name: String, shard: Rc<IggyShard>) -> Result<(), IggyError> {
    let address = shard.config.tcp.clone().address;
    monoio::spawn(async move {
        let listener =
            TcpListener::bind(address).expect(format!("Unable to start {server_name}.").as_ref());

        let local_addr = listener
            .local_addr()
            .expect("Failed to get local address for TCP listener");
        info!("{server_name} server has started on: {:?}", local_addr);
        // This is required for the integration tests. Client requires information about address to connect.
        // Since we bind to port 0 when creating server in order to get a random non-used port,
        // we have to store the address in the default_config.toml file.
        // TODO(numinex) -- uncomment this line when we are ready to test the integration tests.
        //persist_tcp_address(&shard, local_addr.to_string()).await?;
        loop {
            match listener.accept().await {
                Ok((stream, address)) => {
                    info!("Accepted new TCP connection: {}", address);
                    let shard = shard.clone();
                    let mut sender = TcpSender { stream };
                    monoio::spawn(async move {
                        if let Err(error) = handle_connection(address, &mut sender, shard).await {
                            handle_error(error);
                            // Delete client and session
                            //system.read().delete_client(&address).await;
                        }
                    });
                }
                Err(error) => error!("Unable to accept TCP socket, error: {}", error),
            }
        }
    })
    .await
}
