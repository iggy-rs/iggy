use crate::channels::server_command::ServerCommand;
use crate::configs::server::PersonalAccessTokenCleanerConfig;
use crate::streaming::systems::system::SharedSystem;
use async_trait::async_trait;
use flume::Sender;
use iggy::utils::duration::IggyDuration;
use iggy::utils::timestamp::TimeStamp;
use tokio::time;
use tracing::{debug, error, info};

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
        info!(
            "Personal access token cleaner is enabled, expired tokens will be deleted every: {:?}.",
            interval
        );

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

#[async_trait]
impl ServerCommand<CleanPersonalAccessTokensCommand> for CleanPersonalAccessTokensExecutor {
    async fn execute(&mut self, system: &SharedSystem, _command: CleanPersonalAccessTokensCommand) {
        let system = system.read();
        let tokens = system.storage.personal_access_token.load_all().await;
        if tokens.is_err() {
            error!("Failed to load personal access tokens: {:?}", tokens);
            return;
        }

        let tokens = tokens.unwrap();
        if tokens.is_empty() {
            debug!("No personal access tokens to delete.");
            return;
        }

        let now = TimeStamp::now().to_micros();
        let expired_tokens = tokens
            .into_iter()
            .filter(|token| token.is_expired(now))
            .collect::<Vec<_>>();

        if expired_tokens.is_empty() {
            debug!("No expired personal access tokens to delete.");
            return;
        }

        let expired_tokens_count = expired_tokens.len();
        let mut deleted_tokens_count = 0;
        debug!("Found {expired_tokens_count} expired personal access tokens.");
        for token in expired_tokens {
            let result = system
                .storage
                .personal_access_token
                .delete_for_user(token.user_id, &token.name)
                .await;
            if result.is_err() {
                error!(
                    "Failed to delete personal access token: {} for user with ID: {}. Error: {:?}",
                    token.name,
                    token.user_id,
                    result.err().unwrap()
                );
                continue;
            }

            deleted_tokens_count += 1;
            debug!(
                "Deleted personal access token: {} for user with ID: {}.",
                token.name, token.user_id
            );
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
