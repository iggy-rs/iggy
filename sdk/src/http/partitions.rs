use crate::client::PartitionClient;
use crate::error::IggyError;
use crate::http::client::HttpClient;
use crate::partitions::create_partitions::CreatePartitions;
use crate::partitions::delete_partitions::DeletePartitions;
use async_trait::async_trait;

#[async_trait]
impl PartitionClient for HttpClient {
    async fn create_partitions(&self, command: &CreatePartitions) -> Result<(), IggyError> {
        self.post(
            &get_path(
                &command.stream_id.as_string(),
                &command.topic_id.as_string(),
            ),
            &command,
        )
        .await?;
        Ok(())
    }

    async fn delete_partitions(&self, command: &DeletePartitions) -> Result<(), IggyError> {
        self.delete_with_query(
            &get_path(
                &command.stream_id.as_string(),
                &command.topic_id.as_string(),
            ),
            &command,
        )
        .await?;
        Ok(())
    }
}

fn get_path(stream_id: &str, topic_id: &str) -> String {
    format!("streams/{stream_id}/topics/{topic_id}/partitions")
}
