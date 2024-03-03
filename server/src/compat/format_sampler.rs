use async_trait::async_trait;
use iggy::error::IggyError;

#[async_trait]
pub trait BinaryFormatSampler: Send + Sync {
    async fn sample(&self) -> Result<(), IggyError>;
}
