use crate::channels::server_command::ServerCommand;
use crate::streaming::systems::system::SharedSystem;
use async_trait::async_trait;
use flume::Sender;
use iggy::utils::duration::IggyDuration;
use tokio::time;
use tracing::{error, info};

pub struct StateArchiver {
    enabled: bool,
    interval: IggyDuration,
    sender: Sender<ArchiveStateCommand>,
}

#[derive(Debug, Default, Clone)]
pub struct ArchiveStateCommand;

#[derive(Debug, Default, Clone)]
pub struct ArchiveStateExecutor;

impl StateArchiver {
    pub fn new(enabled: bool, interval: IggyDuration, sender: Sender<ArchiveStateCommand>) -> Self {
        Self {
            enabled,
            interval,
            sender,
        }
    }

    pub fn start(&self) {
        if !self.enabled {
            info!("State archiver is disabled.");
            return;
        }

        let interval = self.interval;
        let sender = self.sender.clone();
        info!("State archiver is enabled, state will be archived every: {interval}.");
        tokio::spawn(async move {
            let mut interval_timer = time::interval(interval.get_duration());
            loop {
                interval_timer.tick().await;
                sender.send(ArchiveStateCommand).unwrap_or_else(|err| {
                    error!("Failed to send ArchiveStateCommand. Error: {}", err);
                });
            }
        });
    }
}

#[async_trait]
impl ServerCommand<ArchiveStateCommand> for ArchiveStateExecutor {
    async fn execute(&mut self, system: &SharedSystem, _command: ArchiveStateCommand) {
        let system = system.read();
        info!("Archiving state...");
    }

    fn start_command_sender(
        &mut self,
        _system: SharedSystem,
        config: &crate::configs::server::ServerConfig,
        sender: Sender<ArchiveStateCommand>,
    ) {
        let state_archiver = StateArchiver::new(
            config.archiver.archive_state,
            config.archiver.archive_state_interval,
            sender,
        );
        state_archiver.start();
    }

    fn start_command_consumer(
        mut self,
        system: SharedSystem,
        _config: &crate::configs::server::ServerConfig,
        receiver: flume::Receiver<ArchiveStateCommand>,
    ) {
        tokio::spawn(async move {
            let system = system.clone();
            while let Ok(command) = receiver.recv_async().await {
                self.execute(&system, command).await;
            }
            info!("State archiver receiver stopped.");
        });
    }
}
