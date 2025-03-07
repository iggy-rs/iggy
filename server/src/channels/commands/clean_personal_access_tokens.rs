use crate::channels::server_command::BackgroundServerCommand;
use crate::configs::server::PersonalAccessTokenCleanerConfig;
use crate::streaming::systems::system::SharedSystem;
use flume::Sender;
use iggy::utils::duration::IggyDuration;
use iggy::utils::timestamp::IggyTimestamp;
use tokio::time;
use tracing::{debug, error, info, instrument};

pub struct PersonalAccessTokenCleaner {
    enabled: bool,
    interval: IggyDuration,
    sender: Sender<CleanPersonalAccessTokensCommand>,
}

#[derive(Debug, Default, Clone)]
pub struct CleanPersonalAccessTokensCommand;

#[derive(Debug, Default, Clone)]
pub struct CleanPersonalAccessTokensExecutor;

impl PersonalAccessTokenCleaner {
    pub fn new(
        config: &PersonalAccessTokenCleanerConfig,
        sender: Sender<CleanPersonalAccessTokensCommand>,
    ) -> Self {
        Self {
            enabled: config.enabled,
            interval: config.interval,
            sender,
        }
    }

    pub fn start(&self) {
        if !self.enabled {
            info!("Personal access token cleaner is disabled.");
            return;
        }

        let interval = self.interval;
        let sender = self.sender.clone();
        info!("Personal access token cleaner is enabled, expired tokens will be deleted every: {interval}.");
        tokio::spawn(async move {
            let mut interval_timer = time::interval(interval.get_duration());
            loop {
                interval_timer.tick().await;
                sender
                    .send(CleanPersonalAccessTokensCommand)
                    .unwrap_or_else(|error| {
                        error!(
                            "Failed to send CleanPersonalAccessTokensCommand. Error: {}",
                            error
                        );
                    });
            }
        });
    }
}

impl BackgroundServerCommand<CleanPersonalAccessTokensCommand>
    for CleanPersonalAccessTokensExecutor
{
    #[instrument(skip_all, name = "trace_clean_personal_access_tokens")]
    async fn execute(&mut self, system: &SharedSystem, _command: CleanPersonalAccessTokensCommand) {
        // TODO: System write lock, investigate if it's necessary.
        let mut system = system.write().await;
        let now = IggyTimestamp::now();
        let mut deleted_tokens_count = 0;
        for (_, user) in system.users.iter_mut() {
            let expired_tokens = user
                .personal_access_tokens
                .values()
                .filter(|token| token.is_expired(now))
                .map(|token| token.token.clone())
                .collect::<Vec<_>>();

            for token in expired_tokens {
                debug!(
                    "Personal access token: {token} for user with ID: {} is expired.",
                    user.id
                );
                deleted_tokens_count += 1;
                user.personal_access_tokens.remove(&token);
                debug!(
                    "Deleted personal access token: {token} for user with ID: {}.",
                    user.id
                );
            }
        }
        info!("Deleted {deleted_tokens_count} expired personal access tokens.");
    }

    fn start_command_sender(
        &mut self,
        _system: SharedSystem,
        config: &crate::configs::server::ServerConfig,
        sender: Sender<CleanPersonalAccessTokensCommand>,
    ) {
        let personal_access_token_cleaner =
            PersonalAccessTokenCleaner::new(&config.personal_access_token.cleaner, sender);
        personal_access_token_cleaner.start();
    }

    fn start_command_consumer(
        mut self,
        system: SharedSystem,
        _config: &crate::configs::server::ServerConfig,
        receiver: flume::Receiver<CleanPersonalAccessTokensCommand>,
    ) {
        tokio::spawn(async move {
            let system = system.clone();
            while let Ok(command) = receiver.recv_async().await {
                self.execute(&system, command).await;
            }
            info!("Personal access token cleaner receiver stopped.");
        });
    }
}
