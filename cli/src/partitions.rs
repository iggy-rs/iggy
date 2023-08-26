use iggy::client::Client;
use iggy::client_error::ClientError;
use iggy::partitions::create_partitions::CreatePartitions;
use iggy::partitions::delete_partitions::DeletePartitions;

pub async fn create_partitions(
    command: &CreatePartitions,
    client: &dyn Client,
) -> Result<(), ClientError> {
    client.create_partitions(command).await?;
    Ok(())
}

pub async fn delete_partitions(
    command: &DeletePartitions,
    client: &dyn Client,
) -> Result<(), ClientError> {
    client.delete_partitions(command).await?;
    Ok(())
}
