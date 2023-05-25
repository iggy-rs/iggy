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

    let duration = Duration::from_secs(config.interval);
    task::spawn(async move {
        let mut interval = time::interval(duration);
        info!(
            "Buffered messages will be automatically saved every: {:?}.",
            duration
        );
        interval.tick().await;
        loop {
            interval.tick().await;
            sender
                .send_async(ServerCommand::SaveMessages)
                .await
                .unwrap();
        }
    });
}
