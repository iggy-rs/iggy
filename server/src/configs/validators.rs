use crate::configs::server::ServerConfig;
use crate::configs::system::{CacheConfig, SegmentConfig};
use crate::configs::utils::is_power_of_two;
use crate::server_error::ServerError;
use crate::streaming::segments::segment;
use iggy::validatable::Validatable;
use tracing::error;

impl Validatable<ServerError> for ServerConfig {
    fn validate(&self) -> Result<(), ServerError> {
        self.system.segment.validate()?;
        self.system.cache.validate()?;

        Ok(())
    }
}

impl Validatable<ServerError> for CacheConfig {
    fn validate(&self) -> Result<(), ServerError> {
        if self.messages_amount > 0 && !is_power_of_two(self.messages_amount) {
            error!("Cache configuration -> messages buffer must be a power of two.");
            return Err(ServerError::CacheConfigValidationFailure);
        }

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
