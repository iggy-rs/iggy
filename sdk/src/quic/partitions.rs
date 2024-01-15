use crate::binary;
use crate::client::PartitionClient;
use crate::error::Error;
use crate::partitions::create_partitions::CreatePartitions;
use crate::partitions::delete_partitions::DeletePartitions;
use crate::quic::client::QuicClient;
use async_trait::async_trait;

#[async_trait]
impl PartitionClient for QuicClient {
    async fn create_partitions(&self, command: &CreatePartitions) -> Result<(), Error> {
        binary::partitions::create_partitions(self, command).await
    }

    async fn delete_partitions(&self, command: &DeletePartitions) -> Result<(), Error> {
        binary::partitions::delete_partitions(self, command).await
    }
}
