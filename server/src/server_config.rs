use crate::server_error::ServerError;
use figment::{
    providers::{Env, Format, Json},
    Error, Figment,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use streaming::config::SystemConfig;
use tracing::info;

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
        let config_json = serde_json::to_string_pretty(&config).unwrap();
        info!("Config loaded from path: '{}'\n{}", path, config_json);

        Ok(config)
    }
}
