use crate::configs::http::{
    HttpConfig, HttpCorsConfig, HttpJwtConfig, HttpMetricsConfig, HttpTlsConfig,
};
use crate::configs::quic::{QuicCertificateConfig, QuicConfig};
use crate::configs::server::{
    MessageCleanerConfig, MessageSaverConfig, PersonalAccessTokenCleanerConfig,
    PersonalAccessTokenConfig, ServerConfig,
};
use crate::configs::system::{
    CacheConfig, CompatibilityConfig, CompressionConfig, DatabaseConfig, EncryptionConfig,
    LoggingConfig, MessageDeduplicationConfig, PartitionConfig, RetentionPolicyConfig,
    RuntimeConfig, SegmentConfig, StreamConfig, SystemConfig, TopicConfig,
};
use crate::configs::tcp::{TcpConfig, TcpTlsConfig};
use std::sync::Arc;

use super::system::BackupConfig;

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
            datagram_send_buffer_size: "100KB".parse().unwrap(),
            initial_mtu: "10KB".parse().unwrap(),
            send_window: "100KB".parse().unwrap(),
            receive_window: "100KB".parse().unwrap(),
            keep_alive_interval: "5s".parse().unwrap(),
            max_idle_timeout: "10s".parse().unwrap(),
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

impl Default for HttpJwtConfig {
    fn default() -> HttpJwtConfig {
        HttpJwtConfig {
            algorithm: "HS256".to_string(),
            issuer: "iggy".to_string(),
            audience: "iggy".to_string(),
            valid_issuers: vec!["iggy".to_string()],
            valid_audiences: vec!["iggy".to_string()],
            access_token_expiry: "1h".parse().unwrap(),
            refresh_token_expiry: "1d".parse().unwrap(),
            clock_skew: "5s".parse().unwrap(),
            not_before: "0s".parse().unwrap(),
            encoding_secret: "top_secret$iggy.rs$_jwt_HS256_key#!".to_string(),
            decoding_secret: "top_secret$iggy.rs$_jwt_HS256_key#!".to_string(),
            use_base64_secret: false,
        }
    }
}

impl Default for MessageCleanerConfig {
    fn default() -> MessageCleanerConfig {
        MessageCleanerConfig {
            enabled: true,
            interval: "1m".parse().unwrap(),
        }
    }
}

impl Default for MessageSaverConfig {
    fn default() -> MessageSaverConfig {
        MessageSaverConfig {
            enabled: true,
            enforce_fsync: true,
            interval: "30s".parse().unwrap(),
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
            interval: "1m".parse().unwrap(),
        }
    }
}

impl Default for SystemConfig {
    fn default() -> SystemConfig {
        SystemConfig {
            path: "local_data".to_string(),
            backup: BackupConfig::default(),
            database: DatabaseConfig::default(),
            runtime: RuntimeConfig::default(),
            logging: LoggingConfig::default(),
            cache: CacheConfig::default(),
            retention_policy: RetentionPolicyConfig::default(),
            stream: StreamConfig::default(),
            encryption: EncryptionConfig::default(),
            topic: TopicConfig::default(),
            partition: PartitionConfig::default(),
            segment: SegmentConfig::default(),
            compression: CompressionConfig::default(),
            message_deduplication: MessageDeduplicationConfig::default(),
        }
    }
}

impl Default for BackupConfig {
    fn default() -> BackupConfig {
        BackupConfig {
            path: "backup".to_string(),
            compatibility: CompatibilityConfig::default(),
        }
    }
}

impl Default for CompatibilityConfig {
    fn default() -> Self {
        CompatibilityConfig {
            path: "compatibility".to_string(),
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

impl Default for RuntimeConfig {
    fn default() -> RuntimeConfig {
        RuntimeConfig {
            path: "runtime".to_string(),
        }
    }
}

impl Default for CompressionConfig {
    fn default() -> Self {
        CompressionConfig {
            allow_override: false,
            default_algorithm: "none".parse().unwrap(),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> LoggingConfig {
        LoggingConfig {
            path: "logs".to_string(),
            level: "info".to_string(),
            max_size: "200 MB".parse().unwrap(),
            retention: "7 days".parse().unwrap(),
            sysinfo_print_interval: "10s".parse().unwrap(),
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

impl Default for RetentionPolicyConfig {
    fn default() -> RetentionPolicyConfig {
        RetentionPolicyConfig {
            message_expiry: "0".parse().unwrap(),
            max_topic_size: "10 GB".parse().unwrap(),
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
            enforce_fsync: false,
            validate_checksum: false,
        }
    }
}

impl Default for SegmentConfig {
    fn default() -> SegmentConfig {
        SegmentConfig {
            size: "1 GB".parse().unwrap(),
            cache_indexes: true,
            cache_time_indexes: true,
        }
    }
}

impl Default for MessageDeduplicationConfig {
    fn default() -> MessageDeduplicationConfig {
        MessageDeduplicationConfig {
            enabled: false,
            max_entries: 1000,
            expiry: "1m".parse().unwrap(),
        }
    }
}
