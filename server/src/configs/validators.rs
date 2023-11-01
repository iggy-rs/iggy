extern crate sysinfo;

use super::server::{MessageCleanerConfig, MessageSaverConfig};
use crate::configs::server::{PersonalAccessTokenConfig, ServerConfig};
use crate::configs::system::{CacheConfig, SegmentConfig};
use crate::server_error::ServerError;
use crate::streaming::segments::segment;
use byte_unit::{Byte, ByteUnit};
use iggy::validatable::Validatable;
use sysinfo::SystemExt;
use tracing::{error, info, warn};

impl Validatable<ServerError> for ServerConfig {
    fn validate(&self) -> Result<(), ServerError> {
        self.system.segment.validate()?;
        self.system.cache.validate()?;
        self.personal_access_token.validate()?;

        Ok(())
    }
}

impl Validatable<ServerError> for CacheConfig {
    fn validate(&self) -> Result<(), ServerError> {
        let limit_bytes = self.size.clone().into();
        let mut sys = sysinfo::System::new_all();
        sys.refresh_system();
        sys.refresh_processes();
        let total_memory = sys.total_memory();
        let free_memory = sys.free_memory();
        let cache_percentage = (limit_bytes as f64 / total_memory as f64) * 100.0;

        let pretty_cache_limit =
            Byte::from_bytes(limit_bytes as u128).get_adjusted_unit(ByteUnit::MB);
        let pretty_total_memory =
            Byte::from_bytes(total_memory as u128).get_adjusted_unit(ByteUnit::MB);
        let pretty_free_memory =
            Byte::from_bytes(free_memory as u128).get_adjusted_unit(ByteUnit::MB);

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

impl Validatable<ServerError> for SegmentConfig {
    fn validate(&self) -> Result<(), ServerError> {
        if self.size_bytes > segment::MAX_SIZE_BYTES {
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
        if self.enabled && self.interval == 0 {
            error!("Message saver interval size cannot be zero, it must be greater than 0.");
            return Err(ServerError::InvalidConfiguration);
        }

        Ok(())
    }
}

impl Validatable<ServerError> for MessageCleanerConfig {
    fn validate(&self) -> Result<(), ServerError> {
        if self.enabled && self.interval == 0 {
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

        if self.cleaner.enabled && self.cleaner.interval == 0 {
            error!(
                "Personal access token cleaner interval cannot be zero, it must be greater than 0."
            );
            return Err(ServerError::InvalidConfiguration);
        }

        Ok(())
    }
}
