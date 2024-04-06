use crate::client::PartitionClient;
use crate::error::IggyError;
use crate::http::client::HttpClient;
use crate::http::HttpTransport;
use crate::identifier::Identifier;
use crate::partitions::create_partitions::CreatePartitions;
use crate::partitions::delete_partitions::DeletePartitions;
use async_trait::async_trait;

#[async_trait]
impl PartitionClient for HttpClient {
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

async fn create_partitions<T: HttpTransport>(
    transport: &T,
    command: &CreatePartitions,
) -> Result<(), IggyError> {
    transport
        .post(
            &get_path(
                &command.stream_id.as_cow_str(),
                &command.topic_id.as_cow_str(),
            ),
            &command,
        )
        .await?;
    Ok(())
}

async fn delete_partitions<T: HttpTransport>(
    transport: &T,
    command: &DeletePartitions,
) -> Result<(), IggyError> {
    transport
        .delete_with_query(
            &get_path(
                &command.stream_id.as_cow_str(),
                &command.topic_id.as_cow_str(),
            ),
            &command,
        )
        .await?;
    Ok(())
}

fn get_path(stream_id: &str, topic_id: &str) -> String {
    format!("streams/{stream_id}/topics/{topic_id}/partitions")
}
