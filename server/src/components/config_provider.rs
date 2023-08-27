use crate::server_config::ServerConfig;
use crate::server_error::ServerError;
use async_trait::async_trait;
use figment::providers::{Env, Format, Json, Toml};
use figment::Figment;
use std::env;
use tracing::{error, info};

const DEFAULT_CONFIG_PROVIDER: &str = "file";
const DEFAULT_CONFIG_PATH: &str = "configs/server.toml";

#[async_trait]
pub trait ConfigProvider {
    async fn load_config(&self) -> Result<ServerConfig, ServerError>;
}

#[derive(Debug)]
pub struct FileConfigProvider {
    path: String,
}

pub fn resolve(config_provider_type: &str) -> Result<Box<dyn ConfigProvider>, ServerError> {
    match config_provider_type {
        DEFAULT_CONFIG_PROVIDER => {
            let path =
                env::var("IGGY_CONFIG_PATH").unwrap_or_else(|_| DEFAULT_CONFIG_PATH.to_string());
            Ok(Box::new(FileConfigProvider::new(path)))
        }
        _ => Err(ServerError::InvalidConfigurationProvider(
            config_provider_type.to_string(),
        )),
    }
}

impl FileConfigProvider {
    pub fn new(path: String) -> FileConfigProvider {
        FileConfigProvider { path }
    }
}

#[async_trait]
impl ConfigProvider for FileConfigProvider {
    async fn load_config(&self) -> Result<ServerConfig, ServerError> {
        info!("Loading config from path: '{}'...", self.path);
        let config_builder = Figment::new();
        let extension = self.path.split('.').last().unwrap_or("");
        let config_builder = match extension {
            "json" => config_builder.merge(Json::file(&self.path)),
            "toml" => config_builder.merge(Toml::file(&self.path)),
            _ => {
                error!("Cannot load configuration: invalid file extension, only .json and .toml are supported.");
                return Err(ServerError::CannotLoadConfiguration);
            }
        };

        let config: Result<ServerConfig, figment::Error> = config_builder
            .merge(Env::prefixed("IGGY_").split("_"))
            .extract();
        if config.is_err() {
            return Err(ServerError::CannotLoadConfiguration);
        }

        let config = config.unwrap();
        info!("Config loaded from path: '{}'", self.path);
        Ok(config)
    }
}
