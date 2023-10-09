use crate::channels::commands::save_messages::SaveMessagesCommand;
use crate::configs::server::MessageSaverConfig;
use flume::Sender;
use std::time::Duration;
use tokio::time;
use tracing::{error, info};

pub struct MessagesSaver {
    enforce_fsync: bool,
    interval: Duration,
    sender: Sender<SaveMessagesCommand>,
}

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
