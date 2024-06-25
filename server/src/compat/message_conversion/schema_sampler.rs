use crate::compat::message_conversion::binary_schema::BinarySchema;
use crate::server_error::ServerError;
use async_trait::async_trait;

#[async_trait]
pub trait BinarySchemaSampler: Send + Sync {
    async fn try_sample(&self) -> Result<BinarySchema, ServerError>;
}
