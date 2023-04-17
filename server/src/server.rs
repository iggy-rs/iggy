use crate::server_command::ServerCommand;
use crate::server_config::ServerConfig;
use crate::server_error::ServerError;
use anyhow::Result;
use std::sync::Arc;
use streaming::system::System;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tracing::info;

pub struct ServerSystem {
    pub server: Server,
    pub system_receiver: SystemReceiver,
}

pub struct Server {
    pub socket: Arc<UdpSocket>,
    pub sender: mpsc::Sender<ServerCommand>,
    pub config: ServerConfig,
}

pub struct SystemReceiver {
    pub system: System,
    pub receiver: mpsc::Receiver<ServerCommand>,
}

impl ServerSystem {
    pub async fn init(config: ServerConfig) -> Result<ServerSystem, ServerError> {
        info!("Initializing {} server...", config.name);
        let socket = UdpSocket::bind(config.address.clone()).await?;
        let socket = Arc::new(socket);
        let (sender, receiver) = mpsc::channel::<ServerCommand>(1024);

        let system = System::init(config.system.clone()).await?;
        let server = Server {
            socket,
            sender,
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
