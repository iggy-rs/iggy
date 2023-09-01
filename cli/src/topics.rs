use iggy::client::Client;
use iggy::client_error::ClientError;
use iggy::topics::create_topic::CreateTopic;
use iggy::topics::delete_topic::DeleteTopic;
use iggy::topics::get_topic::GetTopic;
use iggy::topics::get_topics::GetTopics;
use iggy::topics::update_topic::UpdateTopic;
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

pub async fn update_topic(command: &UpdateTopic, client: &dyn Client) -> Result<(), ClientError> {
    client.update_topic(command).await?;
    Ok(())
}
