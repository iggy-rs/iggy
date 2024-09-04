use crate::streaming::clients::client_manager::Transport;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use crate::tcp::connection_handler::{handle_connection, handle_error};
use crate::tcp::tcp_sender::TcpSender;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tracing::{error, info};

pub async fn start(address: &str, system: SharedSystem) -> SocketAddr {
    let address = address.to_string();
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        let listener = TcpListener::bind(&address)
            .await
            .expect("Unable to start TCP TLS server.");

        let local_addr = listener
            .local_addr()
            .expect("Failed to get local address for TCP listener");

        tx.send(local_addr).unwrap_or_else(|_| {
            panic!(
                "Failed to send the local address {:?} for TCP listener",
                local_addr
            )
        });

        loop {
            match listener.accept().await {
                Ok((stream, address)) => {
                    info!("Accepted new TCP connection: {address}");
                    let client_id = system
                        .read()
                        .await
                        .add_client(&address, Transport::Tcp)
                        .await;

                    let session = Session::from_client_id(client_id, address);
                    info!("Created new session: {session}");
                    let system = system.clone();
                    let mut sender = TcpSender { stream };
                    tokio::spawn(async move {
                        if let Err(error) =
                            handle_connection(session, &mut sender, system.clone()).await
                        {
                            handle_error(error);
                            system.read().await.delete_client(client_id).await;
                        }
                    });
                }
                Err(error) => error!("Unable to accept TCP socket, error: {error}"),
            }
        }
    });
    match rx.await {
        Ok(addr) => addr,
        Err(_) => panic!("Failed to get the local address for TCP listener"),
    }
}
