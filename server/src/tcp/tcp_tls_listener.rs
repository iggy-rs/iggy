use crate::tcp::connection_handler::{handle_connection, handle_error};
use crate::tcp::persist_tcp_address;
use crate::tcp::tcp_tls_sender::TcpTlsSender;
use crate::tpc::shard::shard::IggyShard;
use iggy::error::IggyError;
use monoio::net::TcpListener;
use monoio_native_tls::TlsAcceptor;
use native_tls::Identity;
use std::rc::Rc;
use tracing::{error, info};

pub(crate) async fn start(server_name: String, shard: Rc<IggyShard>) -> Result<(), IggyError> {
    /*
    let address = shard.config.tcp.clone().address;
    let tls_config = shard.config.tcp.tls;
    monoio::spawn(async move {
        let certificate = std::fs::read(tls_config.certificate.clone());
        if certificate.is_err() {
            panic!("Unable to read certificate file.");
        }

        let certificate = certificate?;
        let identity = Identity::from_pkcs12(&certificate, &tls_config.password);
        if identity.is_err() {
            panic!("Unable to create identity from certificate.");
        }

        let identity = identity?;
        let raw_acceptor = native_tls::TlsAcceptor::new(identity)?;
        let acceptor = TlsAcceptor::from(raw_acceptor);
        let listener =
            TcpListener::bind(address).expect(format!("Unable to start {server_name}.").as_ref());

        let local_addr = listener
            .local_addr()
            .expect("Failed to get local address for TCP TLS listener");
        info!("{server_name} server has started on: {:?}", local_addr);
        // This is required for the integration tests client to know where to connect to.
        // Since we bind to port 0 when creating server in order to get a random non-used port,
        // we have to store the address in the default_config.toml file.
        if let Err(e) = persist_tcp_address(&shard, local_addr.to_string()).await {
            return e;
        }

        loop {
            match listener.accept().await {
                Ok((stream, address)) => {
                    info!("Accepted new TCP TLS connection: {}", address);
                    let acceptor = acceptor.clone();
                    let stream = acceptor.accept(stream).await.unwrap();
                    let shard = shard.clone();
                    let mut sender = TcpTlsSender { stream };
                    monoio::spawn(async move {
                        if let Err(error) =
                            handle_connection(address, &mut sender, shard).await
                        {
                            handle_error(error);
                            //system.read().delete_client(&address).await;
                        }
                    });
                }
                Err(error) => error!("Unable to accept TCP TLS socket, error: {}", error),
            }
        }
    }).await;
    */
    Ok(())
}
