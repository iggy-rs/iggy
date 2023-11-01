use crate::streaming::systems::system::SharedSystem;
use crate::tcp::connection_handler::{handle_connection, handle_error};
use crate::tcp::tcp_sender::TcpSender;
use tokio::net::TcpListener;
use tracing::{error, info};

pub fn start(address: &str, system: SharedSystem) {
    let address = address.to_string();
    tokio::spawn(async move {
        let listener = TcpListener::bind(address.clone()).await;
        if listener.is_err() {
            panic!("Unable to start TCP server on addr {}.", address);
        }

        let listener = listener.unwrap();
        loop {
            match listener.accept().await {
                Ok((stream, address)) => {
                    info!("Accepted new TCP connection: {}", address);
                    let system = system.clone();
                    let mut sender = TcpSender { stream };
                    tokio::spawn(async move {
                        if let Err(error) =
                            handle_connection(&address, &mut sender, system.clone()).await
                        {
                            handle_error(error);
                            system.read().await.delete_client(&address).await;
                        }
                    });
                }
                Err(error) => error!("Unable to accept TCP socket, error: {}", error),
            }
        }
    });
}
