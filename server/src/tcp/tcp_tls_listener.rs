use futures::channel::oneshot;
use monoio::net::TcpListener;
use monoio_native_tls::TlsAcceptor;
use native_tls::Identity;
use std::net::SocketAddr;

use crate::configs::tcp::TcpTlsConfig;
use crate::streaming::systems::system::SharedSystem;
use crate::tcp::connection_handler::{handle_connection, handle_error};
use crate::tcp::tcp_tls_sender::TcpTlsSender;
use tracing::{error, info};

pub(crate) async fn start(address: &str, config: TcpTlsConfig, system: SharedSystem) -> SocketAddr {
    let address = address.to_string();
    let (tx, rx) = oneshot::channel();
    monoio::spawn(async move {
        let certificate = std::fs::read(config.certificate.clone());
        if certificate.is_err() {
            panic!("Unable to read certificate file.");
        }

        let certificate = certificate.unwrap();
        let identity = Identity::from_pkcs12(&certificate, &config.password);
        if identity.is_err() {
            panic!("Unable to create identity from certificate.");
        }

        let identity = identity.unwrap();
        let raw_acceptor = native_tls::TlsAcceptor::new(identity).unwrap();
        let acceptor = TlsAcceptor::from(raw_acceptor);
        let listener = TcpListener::bind(&address).expect("Unable to start TCP TLS server.");

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
                    let acceptor = acceptor.clone();
                    let stream = acceptor.accept(stream).await.unwrap();
                    let system = system.clone();
                    let mut sender = TcpTlsSender { stream };
                    monoio::spawn(async move {
                        if let Err(error) =
                            handle_connection(address, &mut sender, system.clone()).await
                        {
                            handle_error(error);
                            system.read().delete_client(&address).await;
                        }
                    });
                }
                Err(error) => error!("Unable to accept TCP TLS socket, error: {}", error),
            }
        }
    });
    match rx.await {
        Ok(addr) => addr,
        Err(_) => panic!("Failed to get the local address for TCP TLS listener"),
    }
}
