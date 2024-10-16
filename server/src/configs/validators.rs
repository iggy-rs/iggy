extern crate sysinfo;

use super::server::{
    ArchiverConfig, DataMaintenanceConfig, MessageSaverConfig, MessagesMaintenanceConfig,
    StateMaintenanceConfig, TelemetryConfig,
};
use super::system::CompressionConfig;
use crate::archiver::ArchiverKind;
use crate::configs::server::{PersonalAccessTokenConfig, ServerConfig};
use crate::configs::system::{CacheConfig, SegmentConfig};
use crate::server_error::ServerError;
use crate::streaming::segments::segment;
use iggy::compression::compression_algorithm::CompressionAlgorithm;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::expiry::IggyExpiry;
use iggy::utils::topic_size::MaxTopicSize;
use iggy::validatable::Validatable;
use sysinfo::{Pid, ProcessesToUpdate, System};
use tracing::{info, warn};

impl Validatable<ServerError> for ServerConfig {
    fn validate(&self) -> Result<(), ServerError> {
        self.data_maintenance.validate()?;
        self.personal_access_token.validate()?;
        self.system.segment.validate()?;
        self.system.cache.validate()?;
        self.system.compression.validate()?;
        self.telemetry.validate()?;

        let topic_size = match self.system.topic.max_size {
            MaxTopicSize::Custom(size) => Ok(size.as_bytes_u64()),
            MaxTopicSize::Unlimited => Ok(u64::MAX),
            MaxTopicSize::ServerDefault => Err(ServerError::InvalidConfiguration(
                "Max topic size cannot be set to server default.".into(),
            )),
        }?;

        if let IggyExpiry::ServerDefault = self.system.segment.message_expiry {
            return Err(ServerError::InvalidConfiguration(
                "Message expiry cannot be set to server default.".into(),
            ));
        }

        if self.http.enabled {
            if let IggyExpiry::ServerDefault = self.http.jwt.access_token_expiry {
                return Err(ServerError::InvalidConfiguration(
                    "Access token expiry cannot be set to server default.".into(),
                ));
            }
        }

        if topic_size < self.system.segment.size.as_bytes_u64() {
            return Err(ServerError::InvalidConfiguration(format!(
                "Max topic size cannot be lower than segment size. Max topic size: {}, segment size: {}.",
                topic_size,
                self.system.segment.size
            )));
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

impl Validatable<ServerError> for TelemetryConfig {
    fn validate(&self) -> Result<(), ServerError> {
        if !self.enabled {
            return Ok(());
        }

        if self.service_name.trim().is_empty() {
            return Err(ServerError::InvalidConfiguration(
                "Telemetry service name cannot be empty.".into(),
            ));
        }

        if self.logs.endpoint.is_empty() {
            return Err(ServerError::InvalidConfiguration(
                "Telemetry logs endpoint cannot be empty.".into(),
            ));
        }

        if self.traces.endpoint.is_empty() {
            return Err(ServerError::InvalidConfiguration(
                "Telemetry traces endpoint cannot be empty.".into(),
            ));
        }

        Ok(())
    }
}

impl Validatable<ServerError> for CacheConfig {
    fn validate(&self) -> Result<(), ServerError> {
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

impl Validatable<ServerError> for SegmentConfig {
    fn validate(&self) -> Result<(), ServerError> {
        if self.size.as_bytes_u64() as u32 > segment::MAX_SIZE_BYTES {
            return Err(ServerError::InvalidConfiguration(format!(
                "Segment size cannot be greater than: {} bytes.",
                segment::MAX_SIZE_BYTES
            )));
        }

        Ok(())
    }
}

impl Validatable<ServerError> for MessageSaverConfig {
    fn validate(&self) -> Result<(), ServerError> {
        if self.enabled && self.interval.is_zero() {
            return Err(ServerError::InvalidConfiguration(
                "Message saver interval size cannot be zero, it must be greater than 0.".into(),
            ));
        }

        Ok(())
    }
}

impl Validatable<ServerError> for DataMaintenanceConfig {
    fn validate(&self) -> Result<(), ServerError> {
        self.archiver.validate()?;
        self.messages.validate()?;
        self.state.validate()?;
        Ok(())
    }
}

impl Validatable<ServerError> for ArchiverConfig {
    fn validate(&self) -> Result<(), ServerError> {
        if !self.enabled {
            return Ok(());
        }

        return match self.kind {
            ArchiverKind::Disk => {
                if self.disk.is_none() {
                    return Err(ServerError::InvalidConfiguration(
                        "Disk archiver configuration is missing.".into(),
                    ));
                }

                let disk = self.disk.as_ref().unwrap();
                if disk.path.is_empty() {
                    return Err(ServerError::InvalidConfiguration(
                        "Disk archiver path cannot be empty.".into(),
                    ));
                }
                Ok(())
            }
            ArchiverKind::S3 => {
                if self.s3.is_none() {
                    return Err(ServerError::InvalidConfiguration(
                        "S3 archiver configuration is missing.".into(),
                    ));
                }

                let s3 = self.s3.as_ref().unwrap();
                if s3.key_id.is_empty() {
                    return Err(ServerError::InvalidConfiguration(
                        "S3 archiver key id cannot be empty.".into(),
                    ));
                }

                if s3.key_secret.is_empty() {
                    return Err(ServerError::InvalidConfiguration(
                        "S3 archiver key secret cannot be empty.".into(),
                    ));
                }

                if s3.endpoint.is_none() && s3.region.is_none() {
                    return Err(ServerError::InvalidConfiguration(
                        "S3 archiver endpoint or region must be set.".into(),
                    ));
                }

                if s3.endpoint.as_deref().unwrap_or_default().is_empty()
                    && s3.region.as_deref().unwrap_or_default().is_empty()
                {
                    return Err(ServerError::InvalidConfiguration(
                        "S3 archiver region or endpoint cannot be empty.".into(),
                    ));
                }

                if s3.bucket.is_empty() {
                    return Err(ServerError::InvalidConfiguration(
                        "S3 archiver bucket cannot be empty.".into(),
                    ));
                }
                Ok(())
            }
        };
    }
}

impl Validatable<ServerError> for MessagesMaintenanceConfig {
    fn validate(&self) -> Result<(), ServerError> {
        if self.archiver_enabled && self.interval.is_zero() {
            return Err(ServerError::InvalidConfiguration(
                "Message maintenance interval size cannot be zero, it must be greater than 0."
                    .into(),
            ));
        }

        Ok(())
    }
}

impl Validatable<ServerError> for StateMaintenanceConfig {
    fn validate(&self) -> Result<(), ServerError> {
        if self.archiver_enabled && self.interval.is_zero() {
            return Err(ServerError::InvalidConfiguration(
                "State maintenance interval size cannot be zero, it must be greater than 0.".into(),
            ));
        }

        Ok(())
    }
}

impl Validatable<ServerError> for PersonalAccessTokenConfig {
    fn validate(&self) -> Result<(), ServerError> {
        if self.max_tokens_per_user == 0 {
            return Err(ServerError::InvalidConfiguration(
                "Max tokens per user cannot be zero, it must be greater than 0.".into(),
            ));
        }

        if self.cleaner.enabled && self.cleaner.interval.is_zero() {
            return Err(ServerError::InvalidConfiguration(
                "Personal access token cleaner interval cannot be zero, it must be greater than 0."
                    .into(),
            ));
        }

        Ok(())
    }
}
