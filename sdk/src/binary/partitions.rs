use crate::binary::binary_client::BinaryClient;
use crate::binary::fail_if_not_authenticated;
use crate::bytes_serializable::BytesSerializable;
use crate::client::PartitionClient;
use crate::command::{CREATE_PARTITIONS_CODE, DELETE_PARTITIONS_CODE};
use crate::error::IggyError;
use crate::partitions::create_partitions::CreatePartitions;
use crate::partitions::delete_partitions::DeletePartitions;

#[async_trait::async_trait]
impl<B: BinaryClient> PartitionClient for B {
    async fn create_partitions(&self, command: &CreatePartitions) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(CREATE_PARTITIONS_CODE, &command.as_bytes())
            .await?;
        Ok(())
    }

    async fn delete_partitions(&self, command: &DeletePartitions) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(DELETE_PARTITIONS_CODE, &command.as_bytes())
            .await?;
        Ok(())
    }
}
