use crate::configs::server::MessageSaverConfig;
use crate::configs::server::ServerConfig;
use crate::streaming::persistence::persister::*;
use crate::streaming::segments::storage::FileSegmentStorage;
use crate::streaming::systems::system::System;
use async_trait::async_trait;
use flume::{Receiver, Sender};
use std::{sync::Arc, time::Duration};
use tokio::sync::RwLock;
use tokio::time;
use tracing::{error, info, warn};

use crate::channels::server_command::ServerCommand;

pub struct MessagesSaver {
    enforce_fsync: bool,
    interval: Duration,
    sender: Sender<SaveMessagesCommand>,
}

#[derive(Debug, Default, Clone)]
pub struct SaveMessagesCommand {
    pub enforce_fsync: bool,
}

#[derive(Debug, Default, Clone)]
pub struct SaveMessagesExecutor;

impl MessagesSaver {
    pub fn new(config: &MessageSaverConfig, sender: Sender<SaveMessagesCommand>) -> Self {
        Self {
            enforce_fsync: config.enforce_fsync,
            interval: Duration::from_secs(config.interval),
            sender,
        }
    }

    pub fn start(&self) {
        if !self.enforce_fsync {
            info!("Message saver is disabled.");
            return;
        }

        let enforce_fsync = self.enforce_fsync;
        let interval = self.interval;
        let sender = self.sender.clone();
        info!(
            "Message saver is enabled, buffered messages will be automatically saved every: {:?}, enforce sync: {:?}.",
            interval, enforce_fsync
        );

        tokio::spawn(async move {
            let mut interval_timer = time::interval(interval);
            loop {
                interval_timer.tick().await;
                let command = SaveMessagesCommand { enforce_fsync };
                if sender.send(command).is_err() {
                    error!("Failed to send SaveMessagesCommand");
                }
            }
        });
    }
}

#[async_trait]
impl ServerCommand<SaveMessagesCommand> for SaveMessagesExecutor {
    async fn execute(&mut self, system: &Arc<RwLock<System>>, command: SaveMessagesCommand) {
        let persister: Arc<dyn Persister> = if command.enforce_fsync {
            Arc::new(FileWithSyncPersister)
        } else {
            Arc::new(FilePersister)
        };

        let storage = Arc::new(FileSegmentStorage::new(persister));
        if system
            .write()
            .await
            .persist_messages(storage)
            .await
            .is_err()
        {
            error!("Couldn't save buffered messages on disk.");
        }
        info!("Buffered messages saved on disk.");
    }

    fn start_command_sender(
        &mut self,
        _system: Arc<RwLock<System>>,
        config: &ServerConfig,
        sender: Sender<SaveMessagesCommand>,
    ) {
        let messages_saver = MessagesSaver::new(&config.message_saver, sender);
        messages_saver.start();
    }

    fn start_command_consumer(
        mut self,
        system: Arc<RwLock<System>>,
        _config: &ServerConfig,
        receiver: Receiver<SaveMessagesCommand>,
    ) {
        tokio::spawn(async move {
            let system = system.clone();
            while let Ok(command) = receiver.recv_async().await {
                self.execute(&system, command).await;
            }
            warn!("Server command handler stopped receiving commands.");
        });
    }
}
