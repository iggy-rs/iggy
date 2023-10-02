use crate::streaming::persistence::persister::*;
use crate::streaming::segments::storage::FileSegmentStorage;
use crate::streaming::systems::system::System;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info};

use crate::channels::executor::ExecutableServerCommand;

#[derive(Debug, Default, Clone)]
pub struct SaveMessagesCommand {
    pub enforce_fsync: bool,
}

#[derive(Debug, Default)]
pub struct SaveMessagesExecutor;

#[async_trait]
impl ExecutableServerCommand for SaveMessagesExecutor {
    type Command = SaveMessagesCommand;

    async fn execute(&mut self, system: Arc<RwLock<System>>, command: SaveMessagesCommand) {
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
}
