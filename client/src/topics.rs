use sdk::client::Client;
use sdk::client_error::ClientError;
use sdk::topics::create_topic::CreateTopic;
use sdk::topics::delete_topic::DeleteTopic;
use sdk::topics::get_topic::GetTopic;
use sdk::topics::get_topics::GetTopics;
use tracing::info;

pub async fn get_topic(command: &GetTopic, client: &dyn Client) -> Result<(), ClientError> {
    let topic = client.get_topic(command).await?;
    info!("Topic: {:#?}", topic);
    Ok(())
}

pub async fn get_topics(command: &GetTopics, client: &dyn Client) -> Result<(), ClientError> {
    let topics = client.get_topics(command).await?;
    if topics.is_empty() {
        info!("No topics found");
        return Ok(());
    }

    info!("Topics: {:#?}", topics);
    Ok(())
}

pub async fn create_topic(command: &CreateTopic, client: &dyn Client) -> Result<(), ClientError> {
    client.create_topic(command).await?;
    Ok(())
}

pub async fn delete_topic(command: &DeleteTopic, client: &dyn Client) -> Result<(), ClientError> {
    client.delete_topic(command).await?;
    Ok(())
}
