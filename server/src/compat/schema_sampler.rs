use crate::compat::binary_schema::BinarySchema;
use crate::server_error::ServerError;

pub trait BinarySchemaSampler: Send + Sync {
    async fn try_sample(&self) -> Result<BinarySchema, ServerError>;
}
