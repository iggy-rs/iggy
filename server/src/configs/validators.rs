extern crate sysinfo;

use super::server::{
    ArchiverConfig, DataMaintenanceConfig, MessageSaverConfig, MessagesMaintenanceConfig,
    StateMaintenanceConfig, TelemetryConfig,
};
use super::system::CompressionConfig;
use crate::archiver::ArchiverKind;
use crate::configs::server::{PersonalAccessTokenConfig, ServerConfig};
use crate::configs::system::{CacheConfig, SegmentConfig};
use crate::server_error::ServerConfigError;
use crate::streaming::segments::segment;
use iggy::compression::compression_algorithm::CompressionAlgorithm;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::expiry::IggyExpiry;
use iggy::utils::topic_size::MaxTopicSize;
use iggy::validatable::Validatable;
use sysinfo::{Pid, ProcessesToUpdate, System};
use tracing::{info, warn};

impl Validatable<ServerConfigError> for ServerConfig {
    fn validate(&self) -> Result<(), ServerConfigError> {
        self.data_maintenance.validate()?;
        self.personal_access_token.validate()?;
        self.system.segment.validate()?;
        self.system.cache.validate()?;
        self.system.compression.validate()?;
        self.telemetry.validate()?;

        let topic_size = match self.system.topic.max_size {
            MaxTopicSize::Custom(size) => Ok(size.as_bytes_u64()),
            MaxTopicSize::Unlimited => Ok(u64::MAX),
            MaxTopicSize::ServerDefault => Err(ServerConfigError::InvalidConfiguration),
        }?;

        if let IggyExpiry::ServerDefault = self.system.segment.message_expiry {
            return Err(ServerConfigError::InvalidConfiguration);
        }

        if self.http.enabled {
            if let IggyExpiry::ServerDefault = self.http.jwt.access_token_expiry {
                return Err(ServerConfigError::InvalidConfiguration);
            }
        }

        if topic_size < self.system.segment.size.as_bytes_u64() {
            return Err(ServerConfigError::InvalidConfiguration);
        }

        Ok(())
    }
}

impl Validatable<ServerConfigError> for CompressionConfig {
    fn validate(&self) -> Result<(), ServerConfigError> {
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

impl Validatable<ServerConfigError> for TelemetryConfig {
    fn validate(&self) -> Result<(), ServerConfigError> {
        if !self.enabled {
            return Ok(());
        }

        if self.service_name.trim().is_empty() {
            return Err(ServerConfigError::InvalidConfiguration);
        }

        if self.logs.endpoint.is_empty() {
            return Err(ServerConfigError::InvalidConfiguration);
        }

        if self.traces.endpoint.is_empty() {
            return Err(ServerConfigError::InvalidConfiguration);
        }

        Ok(())
    }
}

impl Validatable<ServerConfigError> for CacheConfig {
    fn validate(&self) -> Result<(), ServerConfigError> {
        let limit_bytes = self.size.clone().into();
        let mut sys = System::new_all();
        sys.refresh_all();
        sys.refresh_processes(
            ProcessesToUpdate::Some(&[Pid::from_u32(std::process::id())]),
            true,
        );
        let total_memory = sys.total_memory();
        let free_memory = sys.free_memory();
        let cache_percentage = (limit_bytes as f64 / total_memory as f64) * 100.0;

        let pretty_cache_limit = IggyByteSize::from(limit_bytes).as_human_string();
        let pretty_total_memory = IggyByteSize::from(total_memory).as_human_string();
        let pretty_free_memory = IggyByteSize::from(free_memory).as_human_string();

        if limit_bytes > total_memory {
            return Err(ServerConfigError::CacheConfigValidationFailure);
        }

        if limit_bytes > (total_memory as f64 * 0.75) as u64 {
            warn!(
                "Cache configuration -> cache size exceeds 75% of total memory. Set to: {} ({:.2}% of total memory: {}).",
                pretty_cache_limit, cache_percentage, pretty_total_memory
            );
        }

        if self.enabled {
            info!(
            "Cache configuration -> cache size set to {} ({:.2}% of total memory: {}, free memory: {}).",
            pretty_cache_limit, cache_percentage, pretty_total_memory, pretty_free_memory
        );
        } else {
            info!("Cache configuration -> cache is disabled.");
        }

        Ok(())
    }
}

impl Validatable<ServerConfigError> for SegmentConfig {
    fn validate(&self) -> Result<(), ServerConfigError> {
        if self.size.as_bytes_u64() as u32 > segment::MAX_SIZE_BYTES {
            return Err(ServerConfigError::InvalidConfiguration);
        }

        Ok(())
    }
}

impl Validatable<ServerConfigError> for MessageSaverConfig {
    fn validate(&self) -> Result<(), ServerConfigError> {
        if self.enabled && self.interval.is_zero() {
            return Err(ServerConfigError::InvalidConfiguration);
        }

        Ok(())
    }
}

impl Validatable<ServerConfigError> for DataMaintenanceConfig {
    fn validate(&self) -> Result<(), ServerConfigError> {
        self.archiver.validate()?;
        self.messages.validate()?;
        self.state.validate()?;
        Ok(())
    }
}

impl Validatable<ServerConfigError> for ArchiverConfig {
    fn validate(&self) -> Result<(), ServerConfigError> {
        if !self.enabled {
            return Ok(());
        }

        return match self.kind {
            ArchiverKind::Disk => {
                if self.disk.is_none() {
                    return Err(ServerConfigError::InvalidConfiguration);
                }

                let disk = self.disk.as_ref().unwrap();
                if disk.path.is_empty() {
                    return Err(ServerConfigError::InvalidConfiguration);
                }
                Ok(())
            }
            ArchiverKind::S3 => {
                if self.s3.is_none() {
                    return Err(ServerConfigError::InvalidConfiguration);
                }

                let s3 = self.s3.as_ref().unwrap();
                if s3.key_id.is_empty() {
                    return Err(ServerConfigError::InvalidConfiguration);
                }

                if s3.key_secret.is_empty() {
                    return Err(ServerConfigError::InvalidConfiguration);
                }

                if s3.endpoint.is_none() && s3.region.is_none() {
                    return Err(ServerConfigError::InvalidConfiguration);
                }

                if s3.endpoint.as_deref().unwrap_or_default().is_empty()
                    && s3.region.as_deref().unwrap_or_default().is_empty()
                {
                    return Err(ServerConfigError::InvalidConfiguration);
                }

                if s3.bucket.is_empty() {
                    return Err(ServerConfigError::InvalidConfiguration);
                }
                Ok(())
            }
        };
    }
}

impl Validatable<ServerConfigError> for MessagesMaintenanceConfig {
    fn validate(&self) -> Result<(), ServerConfigError> {
        if self.archiver_enabled && self.interval.is_zero() {
            return Err(ServerConfigError::InvalidConfiguration);
        }

        Ok(())
    }
}

impl Validatable<ServerConfigError> for StateMaintenanceConfig {
    fn validate(&self) -> Result<(), ServerConfigError> {
        if self.archiver_enabled && self.interval.is_zero() {
            return Err(ServerConfigError::InvalidConfiguration);
        }

        Ok(())
    }
}

impl Validatable<ServerConfigError> for PersonalAccessTokenConfig {
    fn validate(&self) -> Result<(), ServerConfigError> {
        if self.max_tokens_per_user == 0 {
            return Err(ServerConfigError::InvalidConfiguration);
        }

        if self.cleaner.enabled && self.cleaner.interval.is_zero() {
            return Err(ServerConfigError::InvalidConfiguration);
        }

        Ok(())
    }
}
