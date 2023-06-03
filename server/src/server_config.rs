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
    pub max_concurrent_bidi_streams: u64,
    pub datagram_send_buffer_size: usize,
    pub initial_mtu: u16,
    pub send_window: u64,
    pub receive_window: u64,
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

impl Default for ServerConfig {
    fn default() -> ServerConfig {
        ServerConfig {
            message_saver: MessageSaverConfig::default(),
            system: Arc::new(SystemConfig::default()),
            quic: QuicConfig::default(),
            http: HttpConfig::default(),
        }
    }
}

impl Default for QuicConfig {
    fn default() -> QuicConfig {
        QuicConfig {
            enabled: true,
            address: "127.0.0.1:8080".to_string(),
            max_concurrent_bidi_streams: 10000,
            datagram_send_buffer_size: 100000,
            initial_mtu: 10000,
            send_window: 100000,
            receive_window: 100000,
        }
    }
}

impl Default for HttpConfig {
    fn default() -> HttpConfig {
        HttpConfig {
            enabled: true,
            address: "127.0.0.1:3000".to_string(),
        }
    }
}

impl Default for MessageSaverConfig {
    fn default() -> MessageSaverConfig {
        MessageSaverConfig {
            enabled: true,
            interval: 1000,
        }
    }
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
