use crate::server_command::ServerCommand;
use flume::Receiver;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;
use tracing::{error, info};

pub fn start(system: Arc<RwLock<System>>, receiver: Receiver<ServerCommand>) {
    tokio::spawn(async move {
        loop {
            let server_command = receiver.recv_async().await;
            if server_command.is_err() {
                error!("Error when receiving command: {:?}", server_command.err());
                continue;
            }

            let server_command = server_command.unwrap();
            match server_command {
                ServerCommand::SaveMessages(sync) => {
                    let system = system.read().await;
                    if system.persist_messages(sync).await.is_err() {
                        error!("Couldn't save buffered messages on disk.");
                        return;
                    }
                    info!("Buffered messages saved on disk.");
                }
            }
        }
    });
}
