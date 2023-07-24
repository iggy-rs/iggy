use crate::components::config_provider::ConfigProvider;
use crate::server_error::ServerError;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use streaming::config::SystemConfig;
use streaming::segments::segment;
use tracing::error;

#[derive(Debug, Deserialize, Serialize)]
pub struct ServerConfig {
    pub message_saver: MessageSaverConfig,
    pub system: Arc<SystemConfig>,
    pub quic: QuicConfig,
    pub tcp: TcpConfig,
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
    pub keep_alive_interval: u64,
    pub max_idle_timeout: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TcpConfig {
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
    pub enforce_sync: bool,
    pub interval: u64,
}

impl Default for ServerConfig {
    fn default() -> ServerConfig {
        ServerConfig {
            message_saver: MessageSaverConfig::default(),
            system: Arc::new(SystemConfig::default()),
            quic: QuicConfig::default(),
            tcp: TcpConfig::default(),
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
            keep_alive_interval: 5000,
            max_idle_timeout: 10000,
        }
    }
}

impl Default for TcpConfig {
    fn default() -> TcpConfig {
        TcpConfig {
            enabled: true,
            address: "127.0.0.1:8090".to_string(),
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
            enforce_sync: true,
            interval: 1000,
        }
    }
}

impl ServerConfig {
    pub async fn load(config_provider: &dyn ConfigProvider) -> Result<ServerConfig, ServerError> {
        let config = config_provider.load_config().await?;
        Self::validate_config(&config)?;
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
