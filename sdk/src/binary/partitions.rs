#[allow(deprecated)]
use crate::binary::binary_client::BinaryClient;
use crate::binary::{fail_if_not_authenticated, BinaryTransport};
use crate::bytes_serializable::BytesSerializable;
use crate::client::PartitionClient;
use crate::command::{CREATE_PARTITIONS_CODE, DELETE_PARTITIONS_CODE};
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::partitions::create_partitions::CreatePartitions;
use crate::partitions::delete_partitions::DeletePartitions;

#[async_trait::async_trait]
impl<B: BinaryClient> PartitionClient for B {
    async fn create_partitions(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<(), IggyError> {
        create_partitions(
            self,
            &CreatePartitions {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
                partitions_count,
            },
        )
        .await
    }

    async fn delete_partitions(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<(), IggyError> {
        delete_partitions(
            self,
            &DeletePartitions {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
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
