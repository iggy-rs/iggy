use crate::channels::server_command::ServerCommand;
use crate::configs::server::MessageSaverConfig;
use crate::configs::server::ServerConfig;
use crate::streaming::persistence::persister::*;
use crate::streaming::segments::storage::FileSegmentStorage;
use crate::streaming::systems::system::SharedSystem;
use async_trait::async_trait;
use flume::{Receiver, Sender};
use iggy::utils::duration::IggyDuration;
use std::sync::Arc;
use tokio::time;
use tracing::{error, info, warn};

pub struct MessagesSaver {
    enforce_fsync: bool,
    interval: IggyDuration,
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
            interval: config.interval,
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
            "Message saver is enabled, buffered messages will be automatically saved every: {:?}, enforce fsync: {:?}.",
            interval, enforce_fsync
        );

        tokio::spawn(async move {
            let mut interval_timer = time::interval(interval.get_duration());
            loop {
                interval_timer.tick().await;
                let command = SaveMessagesCommand { enforce_fsync };
                sender.send(command).unwrap_or_else(|error| {
                    error!("Failed to send SaveMessagesCommand. Error: {}", error);
                });
            }
        });
    }
}

#[async_trait]
impl ServerCommand<SaveMessagesCommand> for SaveMessagesExecutor {
    async fn execute(&mut self, system: &SharedSystem, command: SaveMessagesCommand) {
        let persister: Arc<dyn Persister> = if command.enforce_fsync {
            Arc::new(FileWithSyncPersister)
        } else {
            Arc::new(FilePersister)
        };

        let storage = Arc::new(FileSegmentStorage::new(persister));
        system
            .write()
            .persist_messages(storage)
            .await
            .unwrap_or_else(|error| {
                error!("Couldn't save buffered messages on disk. Error: {}", error);
            });
        info!("Buffered messages saved on disk.");
    }

    fn start_command_sender(
        &mut self,
        _system: SharedSystem,
        config: &ServerConfig,
        sender: Sender<SaveMessagesCommand>,
    ) {
        let messages_saver = MessagesSaver::new(&config.message_saver, sender);
        messages_saver.start();
    }

    fn start_command_consumer(
        mut self,
        system: SharedSystem,
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
