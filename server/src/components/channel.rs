use crate::command;
use crate::server::{Server, SystemReceiver};
use crate::server_command::ServerCommand;
use std::process;
use tracing::{error, info};

impl Server {
    pub fn start_channel(&self, mut system_receiver: SystemReceiver) {
        let name = self.config.name.clone();
        let socket = self.socket.clone();
        tokio::spawn(async move {
            while let Some(server_command) = system_receiver.receiver.recv().await {
                match server_command {
                    ServerCommand::HandleRequest(bytes, address) => {
                        if let Err(error) =
                            command::handle(&bytes, &socket, address, &mut system_receiver.system)
                                .await
                        {
                            command::handle_error(error, &socket, address).await;
                        }
                    }
                    ServerCommand::SaveMessages => {
                        if system_receiver.system.persist_messages().await.is_err() {
                            error!("Couldn't save buffered messages on disk.");
                        }
                    }
                    ServerCommand::Shutdown => {
                        if system_receiver.system.persist_messages().await.is_err() {
                            error!("Couldn't save buffered messages on disk.");
                            process::exit(1);
                        }
                        info!("{} server has shutdown successfully.", name);
                        process::exit(0);
                    }
                }
            }
        });
    }
}
