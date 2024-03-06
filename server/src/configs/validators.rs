extern crate sysinfo;

use super::server::{MessageCleanerConfig, MessageSaverConfig};
use super::system::CompressionConfig;
use crate::configs::server::{PersonalAccessTokenConfig, ServerConfig};
use crate::configs::system::{CacheConfig, RetentionPolicyConfig, SegmentConfig};
use crate::server_error::ServerError;
use crate::streaming::segments::segment;
use iggy::compression::compression_algorithm::CompressionAlgorithm;
use iggy::utils::byte_size::IggyByteSize;
use iggy::validatable::Validatable;
use sysinfo::System;
use tracing::{info, warn};

impl Validatable<ServerError> for ServerConfig {
    fn validate(&self) -> Result<(), ServerError> {
        self.system.segment.validate()?;
        self.system.cache.validate()?;
        self.system.retention_policy.validate()?;
        self.system.compression.validate()?;
        self.personal_access_token.validate()?;

        let max_topic_size = self.system.retention_policy.max_topic_size.as_bytes_u64();
        let segment_size = self.system.segment.size.as_bytes_u64();

        if max_topic_size <= segment_size {
            return Err(ServerError::InvalidConfiguration(format!("RetentionPolicy configuration -> max_topic_size: {max_topic_size} must be greater than segment size:{segment_size}.")));
        }

        Ok(())
    }
}

impl Validatable<ServerError> for CompressionConfig {
    fn validate(&self) -> Result<(), ServerError> {
        let compression_alg = &self.default_algorithm;
        if *compression_alg != CompressionAlgorithm::None {
            // TODO(numinex): Change this message once server side compression is fully developed.
            warn!(
                "Server started with server-side compression enabled, using algorithm: {}, this feature is not implemented yet!",
                compression_alg
            );
        }

        Ok(())
    }
}

impl Validatable<ServerError> for CacheConfig {
    fn validate(&self) -> Result<(), ServerError> {
        let limit_bytes = self.size.clone().into();
        let mut sys = System::new_all();
        sys.refresh_all();
        sys.refresh_processes();
        let total_memory = sys.total_memory();
        let free_memory = sys.free_memory();
        let cache_percentage = (limit_bytes as f64 / total_memory as f64) * 100.0;

        let pretty_cache_limit = IggyByteSize::from(limit_bytes).as_human_string();
        let pretty_total_memory = IggyByteSize::from(total_memory).as_human_string();
        let pretty_free_memory = IggyByteSize::from(free_memory).as_human_string();

        if limit_bytes > total_memory {
            return Err(ServerError::InvalidConfiguration(format!(
                "Requested cache size exceeds 100% of total memory. Requested: {} ({:.2}% of total memory: {}).",
                pretty_cache_limit, cache_percentage, pretty_total_memory
            )));
        }

        if limit_bytes > (total_memory as f64 * 0.75) as u64 {
            warn!(
                "Cache configuration -> cache size exceeds 75% of total memory. Set to: {} ({:.2}% of total memory: {}).",
                pretty_cache_limit, cache_percentage, pretty_total_memory
            );
        }

        info!(
            "Cache configuration -> cache size set to {} ({:.2}% of total memory: {}, free memory: {}).",
            pretty_cache_limit, cache_percentage, pretty_total_memory, pretty_free_memory
        );

        Ok(())
    }
}

impl Validatable<ServerError> for RetentionPolicyConfig {
    fn validate(&self) -> Result<(), ServerError> {
        if self.max_topic_size.as_bytes_u64() == 0 {
            return Err(ServerError::InvalidConfiguration(format!("RetentionPolicy configuration -> message_expiry cannot be set to 0 ({}), please use \"unlimited\"", self.message_expiry.as_secs())));
        }
        Ok(())
    }
}

impl Validatable<ServerError> for SegmentConfig {
    fn validate(&self) -> Result<(), ServerError> {
        if self.size.as_bytes_u64() as u32 > segment::MAX_SIZE_BYTES {
            return Err(ServerError::InvalidConfiguration(format!(
                "Segment configuration -> size cannot be greater than: {} bytes.",
                segment::MAX_SIZE_BYTES
            )));
        }

        if self.size.as_bytes_u64() < segment::MIN_SIZE_BYTES as u64 {
            return Err(ServerError::InvalidConfiguration(format!(
                "Segment configuration -> size cannot be less than: {} bytes.",
                segment::MIN_SIZE_BYTES
            )));
        }

        Ok(())
    }
}

impl Validatable<ServerError> for MessageSaverConfig {
    fn validate(&self) -> Result<(), ServerError> {
        if self.enabled && self.interval.is_zero() {
            return Err(ServerError::InvalidConfiguration(
                "Message saver interval size cannot be zero, it must be greater than 0."
                    .to_string(),
            ));
        }

        Ok(())
    }
}

impl Validatable<ServerError> for MessageCleanerConfig {
    fn validate(&self) -> Result<(), ServerError> {
        if self.enabled && self.interval.is_zero() {
            return Err(ServerError::InvalidConfiguration(
                "Message cleaner interval size cannot be zero, it must be greater than 0."
                    .to_string(),
            ));
        }

        Ok(())
    }
}

impl Validatable<ServerError> for PersonalAccessTokenConfig {
    fn validate(&self) -> Result<(), ServerError> {
        if self.max_tokens_per_user == 0 {
            return Err(ServerError::InvalidConfiguration(
                "Max tokens per user cannot be zero, it must be greater than 0.".to_string(),
            ));
        }

        if self.cleaner.enabled && self.cleaner.interval.is_zero() {
            return Err(ServerError::InvalidConfiguration(
                "Personal access token cleaner interval cannot be zero, it must be greater than 0."
                    .to_string(),
            ));
        }

        Ok(())
    }
}
