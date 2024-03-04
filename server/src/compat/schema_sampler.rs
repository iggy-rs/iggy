use crate::compat::binary_schema::BinarySchema;
use async_trait::async_trait;
use iggy::error::IggyError;

#[async_trait]
pub trait BinarySchemaSampler: Send + Sync {
    async fn try_sample(&self) -> Result<BinarySchema, IggyError>;
}
