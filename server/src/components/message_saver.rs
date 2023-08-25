use crate::server_command::ServerCommand;
use crate::server_config::MessageSaverConfig;
use flume::Sender;
use std::time::Duration;
use tokio::{task, time};
use tracing::info;

pub fn start(config: MessageSaverConfig, sender: Sender<ServerCommand>) {
    if !config.enabled {
        info!("Message saver is disabled.");
        return;
    }

    if config.interval == 0 {
        panic!("Message saver interval must be greater than 0.")
    }

    let duration = Duration::from_secs(config.interval);
    task::spawn(async move {
        let mut interval = time::interval(duration);
        info!(
            "Message saver is enabled, buffered messages will be automatically saved every: {:?}, enforce sync: {:?}.",
            duration, config.enforce_fsync
        );
        interval.tick().await;
        loop {
            interval.tick().await;
            sender
                .send_async(ServerCommand::SaveMessages(config.enforce_fsync))
                .await
                .unwrap();
        }
    });
}
