use crate::configs::quic::{QuicCertificateConfig, QuicConfig};
use crate::configs::{
    http::{HttpConfig, HttpCorsConfig, HttpJwtConfig, HttpMetricsConfig, HttpTlsConfig},
    resource_quota::MemoryResourceQuota,
    server::{MessageCleanerConfig, MessageSaverConfig, ServerConfig},
    system::{
        CacheConfig, DatabaseConfig, EncryptionConfig, LoggingConfig, PartitionConfig,
        SegmentConfig, StreamConfig, SystemConfig, TopicConfig,
    },
    tcp::{TcpConfig, TcpTlsConfig},
};
use std::fmt::{Display, Formatter};

impl Display for HttpConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, address: {}, cors: {}, jwt: {}, metrics: {}, tls: {} }}",
            self.enabled, self.address, self.cors, self.jwt, self.metrics, self.tls
        )
    }
}

impl Display for HttpCorsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
          f,
          "{{ enabled: {}, allowed_methods: {:?}, allowed_origins: {:?}, allowed_headers: {:?}, exposed_headers: {:?}, allow_credentials: {}, allow_private_network: {} }}",
          self.enabled, self.allowed_methods, self.allowed_origins, self.allowed_headers, self.exposed_headers, self.allow_credentials, self.allow_private_network
      )
    }
}

impl Display for HttpJwtConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ algorithm: {}, audience: {}, expiry: {}, use_base64_secret: {} }}",
            self.algorithm, self.audience, self.access_token_expiry, self.use_base64_secret
        )
    }
}

impl Display for HttpMetricsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, endpoint: {} }}",
            self.enabled, self.endpoint
        )
    }
}

impl Display for HttpTlsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, cert_file: {}, key_file: {} }}",
            self.enabled, self.cert_file, self.key_file
        )
    }
}

impl Display for QuicConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
          f,
          "{{ enabled: {}, address: {}, max_concurrent_bidi_streams: {}, datagram_send_buffer_size: {}, initial_mtu: {}, send_window: {}, receive_window: {}, keep_alive_interval: {}, max_idle_timeout: {}, certificate: {} }}",
          self.enabled,
          self.address,
          self.max_concurrent_bidi_streams,
          self.datagram_send_buffer_size,
          self.initial_mtu,
          self.send_window,
          self.receive_window,
          self.keep_alive_interval,
          self.max_idle_timeout,
          self.certificate
      )
    }
}

impl Display for QuicCertificateConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ self_signed: {}, cert_file: {}, key_file: {} }}",
            self.self_signed, self.cert_file, self.key_file
        )
    }
}

impl Display for MemoryResourceQuota {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MemoryResourceQuota::Bytes(byte) => write!(f, "{}", byte),
            MemoryResourceQuota::Percentage(percentage) => write!(f, "{}%", percentage),
        }
    }
}

impl Display for ServerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ message_cleaner: {}, message_saver: {}, system: {}, quic: {}, tcp: {}, http: {} }}",
            self.message_cleaner, self.message_saver, self.system, self.quic, self.tcp, self.http
        )
    }
}

impl Display for MessageCleanerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, interval: {} }}",
            self.enabled, self.interval
        )
    }
}

impl Display for MessageSaverConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, enforce_fsync: {}, interval: {} }}",
            self.enabled, self.enforce_fsync, self.interval
        )
    }
}

impl Display for DatabaseConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ path: {} }}", self.path)
    }
}

impl Display for CacheConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ enabled: {}, size: {} }}", self.enabled, self.size)
    }
}

impl Display for EncryptionConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ enabled: {} }}", self.enabled)
    }
}

impl Display for StreamConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ path: {} }}", self.path)
    }
}

impl Display for TopicConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ path: {} }}", self.path)
    }
}

impl Display for PartitionConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
          f,
          "{{ path: {}, messages_required_to_save: {}, deduplicate_messages: {}, enforce_fsync: {}, validate_checksum: {} }}",
          self.path,
          self.messages_required_to_save,
          self.deduplicate_messages,
          self.enforce_fsync,
          self.validate_checksum
      )
    }
}

impl Display for SegmentConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ message_expiry: {}, size_bytes: {}, cache_indexes: {}, cache_time_indexes: {} }}",
            self.message_expiry, self.size_bytes, self.cache_indexes, self.cache_time_indexes
        )
    }
}

impl Display for LoggingConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ path: {}, level: {}, max_size_megabytes: {}, retention_days: {} }}",
            self.path, self.level, self.max_size_megabytes, self.retention_days
        )
    }
}

impl Display for TcpConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, address: {}, tls: {} }}",
            self.enabled, self.address, self.tls
        )
    }
}

impl Display for TcpTlsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, certificate: {} }}",
            self.enabled, self.certificate
        )
    }
}

impl Display for SystemConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
          f,
          "{{ path: {}, database: {}, logging: {}, cache: {}, stream: {}, topic: {}, partition: {}, segment: {}, encryption: {} }}",
          self.path,
          self.database,
          self.logging,
          self.cache,
          self.stream,
          self.topic,
          self.partition,
          self.segment,
          self.encryption
      )
    }
}
