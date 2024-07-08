use crate::archiver::ArchiverKind;
use crate::configs::config_provider::ConfigProvider;
use crate::configs::http::HttpConfig;
use crate::configs::quic::QuicConfig;
use crate::configs::system::SystemConfig;
use crate::configs::tcp::TcpConfig;
use crate::server_error::ServerError;
use iggy::utils::duration::IggyDuration;
use iggy::validatable::Validatable;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::DisplayFromStr;
use std::sync::Arc;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ServerConfig {
    pub archiver: ArchiverConfig,
    pub message_cleaner: MessageCleanerConfig,
    pub message_saver: MessageSaverConfig,
    pub personal_access_token: PersonalAccessTokenConfig,
    pub system: Arc<SystemConfig>,
    pub quic: QuicConfig,
    pub tcp: TcpConfig,
    pub http: HttpConfig,
}

// TODO: Refactor config, add message cleaner.
#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ArchiverConfig {
    pub enabled: bool,
    pub archive_messages: bool,
    pub archive_state: bool,
    #[serde_as(as = "DisplayFromStr")]
    pub archive_state_interval: IggyDuration,
    pub kind: ArchiverKind,
    pub disk: Option<DiskArchiverConfig>,
    pub s3: Option<S3ArchiverConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DiskArchiverConfig {
    pub path: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct S3ArchiverConfig {
    pub key_id: String,
    pub access_key: String,
    pub region: String,
    pub bucket: String,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MessageCleanerConfig {
    pub enabled: bool,
    #[serde_as(as = "DisplayFromStr")]
    pub interval: IggyDuration,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MessageSaverConfig {
    pub enabled: bool,
    pub enforce_fsync: bool,
    #[serde_as(as = "DisplayFromStr")]
    pub interval: IggyDuration,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PersonalAccessTokenConfig {
    pub max_tokens_per_user: u32,
    pub cleaner: PersonalAccessTokenCleanerConfig,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PersonalAccessTokenCleanerConfig {
    pub enabled: bool,
    #[serde_as(as = "DisplayFromStr")]
    pub interval: IggyDuration,
}

impl ServerConfig {
    pub async fn load(config_provider: &dyn ConfigProvider) -> Result<ServerConfig, ServerError> {
        let server_config = config_provider.load_config().await?;
        server_config.validate()?;
        Ok(server_config)
    }
}
