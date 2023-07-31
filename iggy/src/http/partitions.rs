use crate::client::PartitionClient;
use crate::error::Error;
use crate::http::client::HttpClient;
use crate::partitions::create_partitions::CreatePartitions;
use crate::partitions::delete_partitions::DeletePartitions;
use async_trait::async_trait;

#[async_trait]
impl PartitionClient for HttpClient {
    async fn create_partitions(&self, command: &CreatePartitions) -> Result<(), Error> {
        self.post(&get_path(command.stream_id, command.stream_id), &command)
            .await?;
        Ok(())
    }

    async fn delete_partitions(&self, command: &DeletePartitions) -> Result<(), Error> {
        self.delete_with_query(&get_path(command.stream_id, command.topic_id), &command)
            .await?;
        Ok(())
    }
}

fn get_path(stream_id: u32, topic_id: u32) -> String {
    format!("streams/{}/topics/{}/partitions", stream_id, topic_id)
}
