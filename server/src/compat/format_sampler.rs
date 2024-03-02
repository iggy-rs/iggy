use async_trait::async_trait;

#[async_trait]
pub trait BinaryFormatSampler {
    async fn sample(&self);
}
