use crate::server_error::ServerError;
use figment::{
    providers::{Env, Format, Json},
    Error, Figment,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use streaming::config::SystemConfig;
use streaming::segments::segment;
use tracing::{error, info};

#[derive(Debug, Deserialize, Serialize)]
pub struct ServerConfig {
    pub message_saver: MessageSaverConfig,
    pub system: Arc<SystemConfig>,
    pub quic: QuicConfig,
    pub http: HttpConfig,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct QuicConfig {
    pub enabled: bool,
    pub address: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct HttpConfig {
    pub enabled: bool,
    pub address: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MessageSaverConfig {
    pub enabled: bool,
    pub interval: u64,
}

impl ServerConfig {
    pub fn load(path: &str) -> Result<ServerConfig, ServerError> {
        let config: Result<ServerConfig, Error> = Figment::new()
            .merge(Env::prefixed("IGGY_"))
            .join(Json::file(path))
            .extract();

        if config.is_err() {
            return Err(ServerError::CannotLoadConfiguration);
        }

        let config = config.unwrap();
        Self::validate_config(&config)?;
        let config_json = serde_json::to_string_pretty(&config).unwrap();
        info!("Config loaded from path: '{}'\n{}", path, config_json);

        Ok(config)
    }

    fn validate_config(config: &ServerConfig) -> Result<(), ServerError> {
        let partition_config = &config.system.stream.topic.partition;
        if partition_config.segment.size_bytes > segment::MAX_SIZE_BYTES {
            error!(
                "Segment configuration -> size cannot be greater than: {} bytes.",
                segment::MAX_SIZE_BYTES
            );
            return Err(ServerError::InvalidConfiguration);
        }

        if partition_config.messages_buffer > 0
            && !Self::is_power_of_two(partition_config.messages_buffer)
        {
            error!("Segment configuration -> messages buffer must be a power of two.");
            return Err(ServerError::InvalidConfiguration);
        }

        Ok(())
    }

    fn is_power_of_two(n: u32) -> bool {
        if n == 0 {
            return false;
        }

        (n & (n - 1)) == 0
    }
}
