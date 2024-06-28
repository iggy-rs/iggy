extern crate sysinfo;

use super::server::{MessageCleanerConfig, MessageSaverConfig};
use super::system::CompressionConfig;
use crate::configs::server::{PersonalAccessTokenConfig, ServerConfig};
use crate::configs::system::{CacheConfig, RetentionPolicyConfig, SegmentConfig};
use crate::server_error::ServerError;
use crate::streaming::segments::segment;
use iggy::compression::compression_algorithm::CompressionAlgorithm;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::expiry::IggyExpiry;
use iggy::utils::topic_size::MaxTopicSize;
use iggy::validatable::Validatable;
use sysinfo::System;
use tracing::{error, info, warn};

impl Validatable<ServerError> for ServerConfig {
    fn validate(&self) -> Result<(), ServerError> {
        self.system.segment.validate()?;
        self.system.cache.validate()?;
        self.system.retention_policy.validate()?;
        self.system.compression.validate()?;
        self.personal_access_token.validate()?;

        let topic_size = match self.system.retention_policy.max_topic_size {
            MaxTopicSize::Custom(size) => Ok(size.as_bytes_u64()),
            MaxTopicSize::Unlimited => Ok(u64::MAX),
            MaxTopicSize::ServerDefault => {
                error!("Max topic size cannot be set to server default.");
                Err(ServerError::InvalidConfiguration)
            }
        }?;

        if let IggyExpiry::ServerDefault = self.system.retention_policy.message_expiry {
            error!("Message expiry cannot be set to server default.");
            return Err(ServerError::InvalidConfiguration);
        }

        if self.http.enabled {
            if let IggyExpiry::ServerDefault = self.http.jwt.access_token_expiry {
                error!("Access token expiry cannot be set to server default.");
                return Err(ServerError::InvalidConfiguration);
            }
        }

        if topic_size < self.system.segment.size.as_bytes_u64() {
            error!("Max topic size cannot be lower than segment size. Max topic size: {}, segment size: {}.",topic_size, self.system.segment.size);
            return Err(ServerError::InvalidConfiguration);
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
            return Err(ServerError::CacheConfigValidationFailure(format!(
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
        Ok(())
    }
}

impl Validatable<ServerError> for SegmentConfig {
    fn validate(&self) -> Result<(), ServerError> {
        if self.size.as_bytes_u64() as u32 > segment::MAX_SIZE_BYTES {
            error!(
                "Segment configuration -> size cannot be greater than: {} bytes.",
                segment::MAX_SIZE_BYTES
            );
            return Err(ServerError::InvalidConfiguration);
        }

        Ok(())
    }
}

impl Validatable<ServerError> for MessageSaverConfig {
    fn validate(&self) -> Result<(), ServerError> {
        if self.enabled && self.interval.is_zero() {
            error!("Message saver interval size cannot be zero, it must be greater than 0.");
            return Err(ServerError::InvalidConfiguration);
        }

        Ok(())
    }
}

impl Validatable<ServerError> for MessageCleanerConfig {
    fn validate(&self) -> Result<(), ServerError> {
        if self.enabled && self.interval.is_zero() {
            error!("Message cleaner interval size cannot be zero, it must be greater than 0.");
            return Err(ServerError::InvalidConfiguration);
        }

        Ok(())
    }
}

impl Validatable<ServerError> for PersonalAccessTokenConfig {
    fn validate(&self) -> Result<(), ServerError> {
        if self.max_tokens_per_user == 0 {
            error!("Max tokens per user cannot be zero, it must be greater than 0.");
            return Err(ServerError::InvalidConfiguration);
        }

        if self.cleaner.enabled && self.cleaner.interval.is_zero() {
            error!(
                "Personal access token cleaner interval cannot be zero, it must be greater than 0."
            );
            return Err(ServerError::InvalidConfiguration);
        }

        Ok(())
    }
}
