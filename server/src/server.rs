use crate::server_command::ServerCommand;
use crate::server_config::ServerConfig;
use crate::server_error::ServerError;
use anyhow::Result;
use flume::{Receiver, Sender};
use quinn::Endpoint;
use std::error::Error;
use std::sync::Arc;
use streaming::system::System;
use tracing::{error, info};

pub struct ServerSystem {
    pub server: Server,
    pub system_receiver: SystemReceiver,
}

pub struct Server {
    pub endpoint: Endpoint,
    pub sender: Arc<Sender<ServerCommand>>,
    pub config: ServerConfig,
}

pub struct SystemReceiver {
    pub system: System,
    pub receiver: Receiver<ServerCommand>,
}

impl ServerSystem {
    pub async fn init(config: ServerConfig) -> Result<ServerSystem, ServerError> {
        info!("Initializing {} server...", config.name);
        let quic_config = configure_quic();
        if let Err(error) = quic_config {
            error!("Error when configuring QUIC: {:?}", error);
            return Err(ServerError::CannotStartServer);
        }
        let endpoint = Endpoint::server(quic_config.unwrap(), config.address.parse().unwrap())?;
        let (sender, receiver) = flume::unbounded::<ServerCommand>();

        let mut system = System::create(config.system.clone());
        system.init().await?;

        let server = Server {
            endpoint,
            sender: Arc::new(sender),
            config,
        };
        let system_receiver = SystemReceiver { system, receiver };

        Ok(ServerSystem {
            server,
            system_receiver,
        })
    }

    pub async fn start(self) -> Result<(), ServerError> {
        info!(
            "{} server has started on: {:?}",
            self.server.config.name, self.server.config.address
        );
        self.server.handle_shutdown();
        self.server.start_watcher();
        self.server.start_channel(self.system_receiver);
        self.server.start_listener().await?;
        Ok(())
    }
}

fn configure_quic() -> Result<quinn::ServerConfig, Box<dyn Error>> {
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let certificate_der = certificate.serialize_der().unwrap();
    let private_key = certificate.serialize_private_key_der();
    let private_key = rustls::PrivateKey(private_key);
    let cert_chain = vec![rustls::Certificate(certificate_der)];
    let server_config = quinn::ServerConfig::with_single_cert(cert_chain, private_key)?;
    Ok(server_config)
}
