use crate::channels::server_command::ServerCommand;
use crate::configs::server::HeartbeatConfig;
use crate::streaming::systems::system::SharedSystem;
use async_trait::async_trait;
use flume::Sender;
use iggy::locking::IggySharedMutFn;
use iggy::utils::duration::IggyDuration;
use iggy::utils::timestamp::IggyTimestamp;
use tokio::time;
use tracing::{debug, error, info};

pub struct VerifyHeartbeats {
    interval: IggyDuration,
    sender: Sender<VerifyHeartbeatsCommand>,
}

#[derive(Debug, Default, Clone)]
pub struct VerifyHeartbeatsCommand;

#[derive(Debug, Default, Clone)]
pub struct VerifyHeartbeatsExecutor;

impl VerifyHeartbeats {
    pub fn new(config: &HeartbeatConfig, sender: Sender<VerifyHeartbeatsCommand>) -> Self {
        Self {
            interval: config.interval,
            sender,
        }
    }

    pub fn start(&self) {
        let interval = self.interval;
        let sender = self.sender.clone();
        info!("Heartbeats will be verified every: {interval}.");
        tokio::spawn(async move {
            let mut interval_timer = time::interval(interval.get_duration());
            loop {
                interval_timer.tick().await;
                debug!("Verifying heartbeats...");
                sender
                    .send(VerifyHeartbeatsCommand)
                    .unwrap_or_else(|error| {
                        error!("Failed to send VerifyHeartbeats. Error: {}", error);
                    });
            }
        });
    }
}

#[async_trait]
impl ServerCommand<VerifyHeartbeatsCommand> for VerifyHeartbeatsExecutor {
    async fn execute(&mut self, system: &SharedSystem, _command: VerifyHeartbeatsCommand) {
        let system = system.read().await;
        let now = IggyTimestamp::now();
        let client_manager = system.client_manager.read().await;
        // TODO: Verify heartbeats
    }

    fn start_command_sender(
        &mut self,
        _system: SharedSystem,
        config: &crate::configs::server::ServerConfig,
        sender: Sender<VerifyHeartbeatsCommand>,
    ) {
        let verify_heartbeats = VerifyHeartbeats::new(&config.heartbeat, sender);
        verify_heartbeats.start();
    }

    fn start_command_consumer(
        mut self,
        system: SharedSystem,
        _config: &crate::configs::server::ServerConfig,
        receiver: flume::Receiver<VerifyHeartbeatsCommand>,
    ) {
        tokio::spawn(async move {
            let system = system.clone();
            while let Ok(command) = receiver.recv_async().await {
                self.execute(&system, command).await;
            }
            info!("Heartbeats verifier receiver stopped.");
        });
    }
}
