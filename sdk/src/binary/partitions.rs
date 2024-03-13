use crate::binary::binary_client::{BinaryClient, BinaryClientV2};
use crate::binary::{fail_if_not_authenticated, BinaryTransport};
use crate::bytes_serializable::BytesSerializable;
use crate::client::PartitionClient;
use crate::client_v2::PartitionClientV2;
use crate::command::{CREATE_PARTITIONS_CODE, DELETE_PARTITIONS_CODE};
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::partitions::create_partitions::CreatePartitions;
use crate::partitions::delete_partitions::DeletePartitions;

#[async_trait::async_trait]
impl<B: BinaryClient> PartitionClient for B {
    async fn create_partitions(&self, command: &CreatePartitions) -> Result<(), IggyError> {
        create_partitions(self, command).await
    }

    async fn delete_partitions(&self, command: &DeletePartitions) -> Result<(), IggyError> {
        delete_partitions(self, command).await
    }
}

#[async_trait::async_trait]
impl<B: BinaryClientV2> PartitionClientV2 for B {
    async fn create_partitions(
        &self,
        stream_id: Identifier,
        topic_id: Identifier,
        partitions_count: u32,
    ) -> Result<(), IggyError> {
        create_partitions(
            self,
            &CreatePartitions {
                stream_id,
                topic_id,
                partitions_count,
            },
        )
        .await
    }

    async fn delete_partitions(
        &self,
        stream_id: Identifier,
        topic_id: Identifier,
        partitions_count: u32,
    ) -> Result<(), IggyError> {
        delete_partitions(
            self,
            &DeletePartitions {
                stream_id,
                topic_id,
                partitions_count,
            },
        )
        .await
    }
}

async fn create_partitions<T: BinaryTransport>(
    transport: &T,
    command: &CreatePartitions,
) -> Result<(), IggyError> {
    fail_if_not_authenticated(transport).await?;
    transport
        .send_with_response(CREATE_PARTITIONS_CODE, command.as_bytes())
        .await?;
    Ok(())
}

async fn delete_partitions<T: BinaryTransport>(
    transport: &T,
    command: &DeletePartitions,
) -> Result<(), IggyError> {
    fail_if_not_authenticated(transport).await?;
    transport
        .send_with_response(DELETE_PARTITIONS_CODE, command.as_bytes())
        .await?;
    Ok(())
}
