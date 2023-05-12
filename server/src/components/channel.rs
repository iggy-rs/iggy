use crate::command;
use crate::sender::Sender;
use crate::server::{Server, SystemReceiver};
use crate::server_command::ServerCommand;
use std::process;
use tracing::{error, info};

impl Server {
    pub fn start_channel(&self, mut system_receiver: SystemReceiver) {
        let name = self.config.name.clone();
        tokio::spawn(async move {
            loop {
                let server_command = system_receiver.receiver.recv_async().await;
                if server_command.is_err() {
                    error!("Error when receiving command: {:?}", server_command.err());
                    continue;
                }

                let server_command = server_command.unwrap();
                match server_command {
                    ServerCommand::HandleRequest((send, recv)) => {
                        let request = recv.read_to_end(1024 * 1024 * 1024).await;
                        if request.is_err() {
                            error!("Error when reading the request: {:?}", request);
                            continue;
                        }

                        let result = command::handle(
                            &request.unwrap(),
                            &mut Sender { send },
                            &mut system_receiver.system,
                        )
                        .await;
                        if result.is_err() {
                            error!("Error when handling the request: {:?}", result);
                            continue;
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
