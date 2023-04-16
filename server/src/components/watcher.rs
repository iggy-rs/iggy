use crate::server::Server;
use crate::server_command::ServerCommand;
use std::time::Duration;
use tokio::{task, time};
use tracing::info;

impl Server {
    pub fn start_watcher(&self) {
        if !self.config.watcher.enabled {
            info!("Watcher is disabled.");
            return;
        }

        let duration = Duration::from_secs(self.config.watcher.interval);
        let sender = self.sender.clone();
        task::spawn(async move {
            let mut interval = time::interval(duration);
            info!(
                "Buffered messages will be automatically saved every: {:?}.",
                duration
            );
            interval.tick().await;
            loop {
                interval.tick().await;
                sender.send(ServerCommand::SaveMessages).await.unwrap();
            }
        });
    }
}
