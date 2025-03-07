use crate::channels::server_command::BackgroundServerCommand;
use crate::configs::server::HeartbeatConfig;
use crate::streaming::systems::system::SharedSystem;
use flume::Sender;
use iggy::locking::IggySharedMutFn;
use iggy::utils::duration::IggyDuration;
use iggy::utils::timestamp::IggyTimestamp;
use tokio::time;
use tracing::{debug, error, info, instrument, warn};

const MAX_THRESHOLD: f64 = 1.2;

pub struct VerifyHeartbeats {
    enabled: bool,
    interval: IggyDuration,
    sender: Sender<VerifyHeartbeatsCommand>,
}

#[derive(Debug, Default, Clone)]
pub struct VerifyHeartbeatsCommand {
    interval: IggyDuration,
}

#[derive(Debug, Default, Clone)]
pub struct VerifyHeartbeatsExecutor;

impl VerifyHeartbeats {
    pub fn new(config: &HeartbeatConfig, sender: Sender<VerifyHeartbeatsCommand>) -> Self {
        Self {
            enabled: config.enabled,
            interval: config.interval,
            sender,
        }
    }

    pub fn start(&self) {
        if !self.enabled {
            info!("Heartbeats verification is disabled.");
            return;
        }

        let interval = self.interval;
        let max_interval = IggyDuration::from((MAX_THRESHOLD * interval.as_micros() as f64) as u64);
        let sender = self.sender.clone();
        info!(
            "Heartbeats will be verified every: {interval}. Max allowed interval: {max_interval}."
        );
        tokio::spawn(async move {
            let mut interval_timer = time::interval(interval.get_duration());
            loop {
                interval_timer.tick().await;
                debug!("Verifying heartbeats...");
                sender
                    .send(VerifyHeartbeatsCommand {
                        interval: max_interval,
                    })
                    .unwrap_or_else(|error| {
                        error!("Failed to send VerifyHeartbeats. Error: {}", error);
                    });
            }
        });
    }
}

impl BackgroundServerCommand<VerifyHeartbeatsCommand> for VerifyHeartbeatsExecutor {
    #[instrument(skip_all, name = "trace_verify_heartbeats")]
    async fn execute(&mut self, system: &SharedSystem, command: VerifyHeartbeatsCommand) {
        let system = system.read().await;
        let clients;
        {
            let client_manager = system.client_manager.read().await;
            clients = client_manager.get_clients();
        }

        let now = IggyTimestamp::now();
        let heartbeat_to = IggyTimestamp::from(now.as_micros() - command.interval.as_micros());
        debug!("Verifying heartbeats at: {now}, max allowed timestamp: {heartbeat_to}");
        let mut stale_clients = Vec::new();
        for client in clients {
            let client = client.read().await;
            if client.last_heartbeat.as_micros() < heartbeat_to.as_micros() {
                warn!(
                    "Stale client session: {}, last heartbeat at: {}, max allowed timestamp: {heartbeat_to}",
                    client.session,
                    client.last_heartbeat,
                );
                client.session.set_stale();
                stale_clients.push(client.session.client_id);
            } else {
                debug!(
                    "Valid heartbeat at: {} for client session: {}, max allowed timestamp: {heartbeat_to}",
                    client.last_heartbeat,
                    client.session,
                );
            }
        }

        if stale_clients.is_empty() {
            return;
        }

        let count = stale_clients.len();
        info!("Removing {count} stale clients...");
        for client_id in stale_clients {
            system.delete_client(client_id).await;
        }
        info!("Removed {count} stale clients.");
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
