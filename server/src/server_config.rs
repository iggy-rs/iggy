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
    pub name: String,
    pub address: String,
    pub watcher: WatcherConfig,
    pub system: Arc<SystemConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct WatcherConfig {
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
        let segment_config = &config.system.stream.topic.partition.segment;
        if segment_config.size_bytes > segment::MAX_SIZE_BYTES {
            error!(
                "Segment configuration -> size cannot be greater than: {} bytes.",
                segment::MAX_SIZE_BYTES
            );
            return Err(ServerError::InvalidConfiguration);
        }

        if segment_config.messages_required_to_save > segment_config.messages_buffer {
            error!("Segment configuration -> messages required to save cannot be greater than messages buffer.");
            return Err(ServerError::InvalidConfiguration);
        }

        if !Self::is_power_of_two(segment_config.messages_buffer) {
            error!("Segment configuration -> messages buffer must be a power of two.");
            return Err(ServerError::InvalidConfiguration);
        }

        if !Self::is_power_of_two(segment_config.messages_required_to_save) {
            error!("Segment configuration -> messages required to save must be a power of two.");
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
