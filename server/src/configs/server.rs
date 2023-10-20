use crate::configs::config_provider::ConfigProvider;
use crate::configs::http::HttpConfig;
use crate::configs::quic::QuicConfig;
use crate::configs::system::SystemConfig;
use crate::configs::tcp::TcpConfig;
use crate::server_error::ServerError;
use iggy::validatable::Validatable;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Deserialize, Serialize)]
pub struct ServerConfig {
    pub message_cleaner: MessageCleanerConfig,
    pub message_saver: MessageSaverConfig,
    pub personal_access_token: PersonalAccessTokenConfig,
    pub system: Arc<SystemConfig>,
    pub quic: QuicConfig,
    pub tcp: TcpConfig,
    pub http: HttpConfig,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MessageCleanerConfig {
    pub enabled: bool,
    pub interval: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MessageSaverConfig {
    pub enabled: bool,
    pub enforce_fsync: bool,
    pub interval: u64,
}

#[derive(Debug, Deserialize, Serialize, Copy, Clone)]
pub struct PersonalAccessTokenConfig {
    pub max_tokens_per_user: u32,
    pub cleaner: PersonalAccessTokenCleanerConfig,
}

#[derive(Debug, Deserialize, Serialize, Copy, Clone)]
pub struct PersonalAccessTokenCleanerConfig {
    pub enabled: bool,
    pub interval: u64,
}

impl ServerConfig {
    pub async fn load(config_provider: &dyn ConfigProvider) -> Result<ServerConfig, ServerError> {
        let server_config = config_provider.load_config().await?;
        server_config.validate()?;
        Ok(server_config)
    }
}
