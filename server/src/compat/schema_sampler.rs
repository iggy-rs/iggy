use crate::compat::binary_schema::BinarySchema;
use async_trait::async_trait;
use crate::server_error::ServerError;

#[async_trait]
pub trait BinarySchemaSampler: Send + Sync {
    async fn try_sample(&self) -> Result<BinarySchema, ServerError>;
}
