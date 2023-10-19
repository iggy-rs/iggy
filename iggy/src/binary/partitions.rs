use crate::binary::binary_client::BinaryClient;
use crate::binary::fail_if_not_authenticated;
use crate::bytes_serializable::BytesSerializable;
use crate::command::{CREATE_PARTITIONS_CODE, DELETE_PARTITIONS_CODE};
use crate::error::Error;
use crate::partitions::create_partitions::CreatePartitions;
use crate::partitions::delete_partitions::DeletePartitions;

pub async fn create_partitions(
    client: &dyn BinaryClient,
    command: &CreatePartitions,
) -> Result<(), Error> {
    fail_if_not_authenticated(client).await?;
    client
        .send_with_response(CREATE_PARTITIONS_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

pub async fn delete_partitions(
    client: &dyn BinaryClient,
    command: &DeletePartitions,
) -> Result<(), Error> {
    fail_if_not_authenticated(client).await?;
    client
        .send_with_response(DELETE_PARTITIONS_CODE, &command.as_bytes())
        .await?;
    Ok(())
}
