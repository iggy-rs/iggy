use crate::configs::quic::{QuicCertificateConfig, QuicConfig};
use crate::configs::server::{
    ArchiverConfig, DataMaintenanceConfig, DiskArchiverConfig, HeartbeatConfig,
    MessagesMaintenanceConfig, S3ArchiverConfig, StateMaintenanceConfig, TelemetryConfig,
    TelemetryLogsConfig, TelemetryTracesConfig,
};
use crate::configs::system::MessageDeduplicationConfig;
use crate::configs::{
    http::{HttpConfig, HttpCorsConfig, HttpJwtConfig, HttpMetricsConfig, HttpTlsConfig},
    resource_quota::MemoryResourceQuota,
    server::{MessageSaverConfig, ServerConfig},
    system::{
        CacheConfig, CompressionConfig, EncryptionConfig, LoggingConfig, PartitionConfig,
        SegmentConfig, StreamConfig, SystemConfig, TopicConfig,
    },
    tcp::{TcpConfig, TcpTlsConfig},
};
use std::fmt::{Display, Formatter};

impl Display for HttpConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, address: {}, max_request_size: {}, cors: {}, jwt: {}, metrics: {}, tls: {} }}",
            self.enabled, self.address, self.max_request_size, self.cors, self.jwt, self.metrics, self.tls
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

impl Display for CompressionConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ allowed_override: {}, default_algorithm: {} }}",
            self.allow_override, self.default_algorithm
        )
    }
}

impl Display for DataMaintenanceConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ archiver: {}, messages: {}, state: {} }}",
            self.archiver, self.messages, self.state
        )
    }
}

impl Display for ArchiverConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let disk = self
            .disk
            .as_ref()
            .map_or("none".to_string(), |disk| disk.to_string());
        let s3 = self
            .s3
            .as_ref()
            .map_or("none".to_string(), |s3| s3.to_string());
        write!(
            f,
            "{{ enabled: {}, kind: {}, disk: {disk}, s3: {s3} }}",
            self.enabled, self.kind,
        )
    }
}

impl Display for DiskArchiverConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ path: {} }}", self.path)
    }
}

impl Display for S3ArchiverConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ key_id: {}, bucket: {}, endpoint: {}. region: {} }}",
            self.key_id,
            self.bucket,
            self.endpoint.as_deref().unwrap_or_default(),
            self.region.as_deref().unwrap_or_default()
        )
    }
}

impl Display for MessagesMaintenanceConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ archiver_enabled: {}, cleaner_enabled: {}, interval: {} }}",
            self.archiver_enabled, self.cleaner_enabled, self.interval
        )
    }
}

impl Display for StateMaintenanceConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ archiver_enabled: {}, overwrite: {}, interval: {} }}",
            self.archiver_enabled, self.overwrite, self.interval
        )
    }
}

impl Display for ServerConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ data_maintenance: {}, message_saver: {}, heartbeat: {}, system: {}, quic: {}, tcp: {}, http: {}, telemetry: {} }}",
            self.data_maintenance, self.message_saver, self.heartbeat, self.system, self.quic, self.tcp, self.http, self.telemetry
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

impl Display for CacheConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ enabled: {}, size: {} }}", self.enabled, self.size)
    }
}

impl Display for HeartbeatConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, interval: {} }}",
            self.enabled, self.interval
        )
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
        write!(
            f,
            "{{ path: {}, max_size: {}, delete_oldest_segments: {} }}",
            self.path, self.max_size, self.delete_oldest_segments
        )
    }
}

impl Display for PartitionConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
          f,
          "{{ path: {}, messages_required_to_save: {}, enforce_fsync: {}, validate_checksum: {} }}",
          self.path,
          self.messages_required_to_save,
          self.enforce_fsync,
          self.validate_checksum
      )
    }
}

impl Display for MessageDeduplicationConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, max_entries: {:?}, expiry: {:?} }}",
            self.enabled, self.max_entries, self.expiry
        )
    }
}

impl Display for SegmentConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ size_bytes: {}, cache_indexes: {}, cache_time_indexes: {}, message_expiry: {}, archive_expired: {} }}",
            self.size, self.cache_indexes, self.cache_time_indexes, self.message_expiry, self.archive_expired
        )
    }
}

impl Display for LoggingConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ path: {}, level: {}, max_size: {}, retention: {} }}",
            self.path,
            self.level,
            self.max_size.as_human_string_with_zero_as_unlimited(),
            self.retention
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

impl Display for TelemetryConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, service_name: {}, logs: {}, traces: {} }}",
            self.enabled, self.service_name, self.logs, self.traces
        )
    }
}

impl Display for TelemetryLogsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ transport: {}, endpoint: {} }}",
            self.transport, self.endpoint
        )
    }
}

impl Display for TelemetryTracesConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ transport: {}, endpoint: {} }}",
            self.transport, self.endpoint
        )
    }
}

impl Display for SystemConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
          f,
          "{{ path: {}, logging: {}, cache: {}, stream: {}, topic: {}, partition: {}, segment: {}, encryption: {} }}",
          self.path,
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
