use crate::configs::tcp::TcpTlsConfig;
use crate::streaming::clients::client_manager::Transport;
use crate::streaming::systems::system::SharedSystem;
use crate::tcp::connection_handler::{handle_connection, handle_error};
use crate::tcp::tcp_tls_sender::TcpTlsSender;
use std::net::SocketAddr;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_native_tls::native_tls;
use tokio_native_tls::native_tls::Identity;
use tracing::{error, info};

pub(crate) async fn start(address: &str, config: TcpTlsConfig, system: SharedSystem) -> SocketAddr {
    let address = address.to_string();
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        let certificate = std::fs::read(config.certificate.clone());
        if certificate.is_err() {
            panic!("Unable to read certificate file.");
        }

        let identity = Identity::from_pkcs12(&certificate.unwrap(), &config.password);
        if identity.is_err() {
            panic!("Unable to create identity from certificate.");
        }

        let acceptor = tokio_native_tls::TlsAcceptor::from(
            native_tls::TlsAcceptor::builder(identity.unwrap())
                .build()
                .unwrap(),
        );

        let listener = TcpListener::bind(&address)
            .await
            .expect("Unable to start TCP TLS server.");

        let local_addr = listener
            .local_addr()
            .expect("Failed to get local address for TCP TLS listener");

        tx.send(local_addr).unwrap_or_else(|_| {
            panic!(
                "Failed to send the local address {:?} for TCP TLS listener",
                local_addr
            )
        });

        loop {
            match listener.accept().await {
                Ok((stream, address)) => {
                    info!("Accepted new TCP TLS connection: {}", address);
                    let session = system
                        .read()
                        .await
                        .add_client(&address, Transport::Tcp)
                        .await;

                    let client_id = session.client_id;
                    let acceptor = acceptor.clone();
                    let stream = acceptor.accept(stream).await.unwrap();
                    let system = system.clone();
                    let mut sender = TcpTlsSender { stream };
                    tokio::spawn(async move {
                        if let Err(error) =
                            handle_connection(session, &mut sender, system.clone()).await
                        {
                            handle_error(error);
                            system.read().await.delete_client(client_id).await;
                            if let Err(error) = sender.stream.shutdown().await {
                                error!("Failed to shutdown TCP stream for client: {client_id}, address: {address}. {error}");
                            } else {
                                info!("Successfully closed TCP stream for client: {client_id}, address: {address}.");
                            }
                        }
                    });
                }
                Err(error) => error!("Unable to accept TCP TLS socket. {error}"),
            }
        }
    });
    match rx.await {
        Ok(addr) => addr,
        Err(_) => panic!("Failed to get the local address for TCP TLS listener."),
    }
}
