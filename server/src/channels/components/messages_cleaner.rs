use flume::Sender;
use std::time::Duration;
use tokio::time;
use tracing::{error, info};

use crate::channels::commands::clean_messages::CleanMessagesCommand;
use crate::configs::server::MessageCleanerConfig;

pub struct MessagesCleaner {
    enabled: bool,
    interval: Duration,
    sender: Sender<CleanMessagesCommand>,
}

impl MessagesCleaner {
    pub fn new(config: &MessageCleanerConfig, sender: Sender<CleanMessagesCommand>) -> Self {
        Self {
            enabled: config.enabled,
            interval: Duration::from_secs(config.interval),
            sender,
        }
    }

    pub fn start(&self) {
        if !self.enabled {
            info!("Message cleaner is disabled.");
            return;
        }

        let interval = self.interval;
        let sender = self.sender.clone();
        info!(
            "Message cleaner is enabled, expired messages will be deleted every: {:?}.",
            interval
        );

        tokio::spawn(async move {
            let mut interval_timer = time::interval(interval);
            loop {
                interval_timer.tick().await;
                if sender.send(CleanMessagesCommand).is_err() {
                    error!("Failed to send CleanMessagesCommand");
                }
            }
        });
    }
}
