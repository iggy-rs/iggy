use crate::configs::http::{
    HttpConfig, HttpCorsConfig, HttpJwtConfig, HttpMetricsConfig, HttpTlsConfig,
};
use crate::configs::quic::{QuicCertificateConfig, QuicConfig};
use crate::configs::server::{
    MessageCleanerConfig, MessageSaverConfig, PersonalAccessTokenCleanerConfig,
    PersonalAccessTokenConfig, ServerConfig,
};
use crate::configs::system::{
    CacheConfig, DatabaseConfig, EncryptionConfig, LoggingConfig, PartitionConfig, SegmentConfig,
    StreamConfig, SystemConfig, TopicConfig,
};
use crate::configs::tcp::{TcpConfig, TcpTlsConfig};
use std::sync::Arc;

impl Default for ServerConfig {
    fn default() -> ServerConfig {
        ServerConfig {
            message_cleaner: MessageCleanerConfig::default(),
            message_saver: MessageSaverConfig::default(),
            personal_access_token: PersonalAccessTokenConfig::default(),
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
            certificate: QuicCertificateConfig::default(),
        }
    }
}

impl Default for QuicCertificateConfig {
    fn default() -> QuicCertificateConfig {
        QuicCertificateConfig {
            self_signed: true,
            cert_file: "certs/iggy_cert.pem".to_string(),
            key_file: "certs/iggy_key.pem".to_string(),
        }
    }
}

impl Default for TcpConfig {
    fn default() -> TcpConfig {
        TcpConfig {
            enabled: true,
            address: "127.0.0.1:8090".to_string(),
            tls: TcpTlsConfig::default(),
        }
    }
}

impl Default for HttpConfig {
    fn default() -> HttpConfig {
        HttpConfig {
            enabled: true,
            address: "127.0.0.1:3000".to_string(),
            cors: HttpCorsConfig::default(),
            jwt: HttpJwtConfig::default(),
            metrics: HttpMetricsConfig::default(),
            tls: HttpTlsConfig::default(),
        }
    }
}

impl Default for MessageCleanerConfig {
    fn default() -> MessageCleanerConfig {
        MessageCleanerConfig {
            enabled: true,
            interval: 60,
        }
    }
}

impl Default for MessageSaverConfig {
    fn default() -> MessageSaverConfig {
        MessageSaverConfig {
            enabled: true,
            enforce_fsync: true,
            interval: 30,
        }
    }
}

impl Default for PersonalAccessTokenConfig {
    fn default() -> PersonalAccessTokenConfig {
        PersonalAccessTokenConfig {
            max_tokens_per_user: 100,
            cleaner: PersonalAccessTokenCleanerConfig::default(),
        }
    }
}

impl Default for PersonalAccessTokenCleanerConfig {
    fn default() -> PersonalAccessTokenCleanerConfig {
        PersonalAccessTokenCleanerConfig {
            enabled: true,
            interval: 60,
        }
    }
}

impl Default for SystemConfig {
    fn default() -> SystemConfig {
        SystemConfig {
            path: "local_data".to_string(),
            database: DatabaseConfig::default(),
            logging: LoggingConfig::default(),
            cache: CacheConfig::default(),
            stream: StreamConfig::default(),
            encryption: EncryptionConfig::default(),
            topic: TopicConfig::default(),
            partition: PartitionConfig::default(),
            segment: SegmentConfig::default(),
        }
    }
}

impl Default for DatabaseConfig {
    fn default() -> DatabaseConfig {
        DatabaseConfig {
            path: "database".to_string(),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> LoggingConfig {
        LoggingConfig {
            path: "logs".to_string(),
            level: "info".to_string(),
            max_size_megabytes: 200,
            retention_days: 7,
        }
    }
}

impl Default for CacheConfig {
    fn default() -> CacheConfig {
        CacheConfig {
            enabled: true,
            size: "2 GB".parse().unwrap(),
        }
    }
}

impl Default for StreamConfig {
    fn default() -> StreamConfig {
        StreamConfig {
            path: "streams".to_string(),
        }
    }
}

impl Default for TopicConfig {
    fn default() -> TopicConfig {
        TopicConfig {
            path: "topics".to_string(),
        }
    }
}

impl Default for PartitionConfig {
    fn default() -> PartitionConfig {
        PartitionConfig {
            path: "partitions".to_string(),
            messages_required_to_save: 1000,
            deduplicate_messages: false,
            enforce_fsync: false,
            validate_checksum: false,
        }
    }
}

impl Default for SegmentConfig {
    fn default() -> SegmentConfig {
        SegmentConfig {
            message_expiry: 0,
            size_bytes: 1024 * 1024 * 1024,
            cache_indexes: true,
            cache_time_indexes: true,
        }
    }
}
