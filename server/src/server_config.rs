use figment::{
    providers::{Env, Format, Json},
    Error, Figment,
};
use serde::{Deserialize, Serialize};
use streaming::config::SystemConfig;
use tracing::info;

#[derive(Debug, Deserialize, Serialize)]
pub struct ServerConfig {
    pub address: String,
    pub system: SystemConfig,
}

pub fn load(path: &str) -> ServerConfig {
    let config: Result<ServerConfig, Error> = Figment::new()
        .merge(Env::prefixed("IGGY_"))
        .join(Json::file(path))
        .extract();

    if config.is_err() {
        panic!("Cannot load config: {}", config.err().unwrap())
    }

    let config = config.unwrap();
    let config_json = serde_json::to_string_pretty(&config).unwrap();
    info!("Config loaded from path: '{}'\n{}", path, config_json);

    config
}
