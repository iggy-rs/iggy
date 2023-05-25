use crate::server_command::ServerCommand;
use flume::Receiver;
use std::process;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::Mutex;
use tracing::{error, info};

pub fn start(system: Arc<Mutex<System>>, receiver: Receiver<ServerCommand>) {
    tokio::spawn(async move {
        loop {
            let server_command = receiver.recv_async().await;
            if server_command.is_err() {
                error!("Error when receiving command: {:?}", server_command.err());
                continue;
            }

            let server_command = server_command.unwrap();
            match server_command {
                ServerCommand::SaveMessages => {
                    let mut system = system.lock().await;
                    if system.persist_messages().await.is_err() {
                        error!("Couldn't save buffered messages on disk.");
                    }
                }
                ServerCommand::Shutdown => {
                    let mut system = system.lock().await;
                    if system.persist_messages().await.is_err() {
                        error!("Couldn't save buffered messages on disk.");
                        process::exit(1);
                    }
                    info!("Iggy server has shutdown successfully.");
                    process::exit(0);
                }
            }
        }
    });
}
