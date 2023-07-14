use iggy::client::Client;
use iggy::client_error::ClientError;
use iggy::consumer_groups::create_consumer_group::CreateConsumerGroup;
use iggy::consumer_groups::delete_consumer_group::DeleteConsumerGroup;
use iggy::consumer_groups::get_consumer_group::GetConsumerGroup;
use iggy::consumer_groups::get_consumer_groups::GetConsumerGroups;
use iggy::consumer_groups::join_consumer_group::JoinConsumerGroup;
use iggy::consumer_groups::leave_consumer_group::LeaveConsumerGroup;
use tracing::info;

pub async fn get_consumer_group(
    command: &GetConsumerGroup,
    client: &dyn Client,
) -> Result<(), ClientError> {
    let consumer_group = client.get_consumer_group(command).await?;
    info!("Consumer group: {:#?}", consumer_group);
    Ok(())
}

pub async fn get_consumer_groups(
    command: &GetConsumerGroups,
    client: &dyn Client,
) -> Result<(), ClientError> {
    let consumer_groups = client.get_consumer_groups(command).await?;
    if consumer_groups.is_empty() {
        info!("No consumer groups found");
        return Ok(());
    }

    info!("Consumer groups: {:#?}", consumer_groups);
    Ok(())
}

pub async fn create_consumer_group(
    command: &CreateConsumerGroup,
    client: &dyn Client,
) -> Result<(), ClientError> {
    client.create_consumer_group(command).await?;
    Ok(())
}

pub async fn delete_consumer_group(
    command: &DeleteConsumerGroup,
    client: &dyn Client,
) -> Result<(), ClientError> {
    client.delete_consumer_group(command).await?;
    Ok(())
}

pub async fn join_consumer_group(
    command: &JoinConsumerGroup,
    client: &dyn Client,
) -> Result<(), ClientError> {
    client.join_consumer_group(command).await?;
    Ok(())
}

pub async fn leave_consumer_group(
    command: &LeaveConsumerGroup,
    client: &dyn Client,
) -> Result<(), ClientError> {
    client.leave_consumer_group(command).await?;
    Ok(())
}
