use crate::client::ConsumerOffsetClient;
use crate::consumer_offsets::get_consumer_offset::GetConsumerOffset;
use crate::consumer_offsets::store_consumer_offset::StoreConsumerOffset;
use crate::error::Error;
use crate::http::client::HttpClient;
use crate::models::consumer_offset_info::ConsumerOffsetInfo;
use async_trait::async_trait;

#[async_trait]
impl ConsumerOffsetClient for HttpClient {
    async fn store_consumer_offset(&self, command: &StoreConsumerOffset) -> Result<(), Error> {
        self.put(
            &get_path(
                &command.stream_id.as_string(),
                &command.topic_id.as_string(),
            ),
            &command,
        )
        .await?;
        Ok(())
    }

    async fn get_consumer_offset(
        &self,
        command: &GetConsumerOffset,
    ) -> Result<ConsumerOffsetInfo, Error> {
        let response = self
            .get_with_query(
                &get_path(
                    &command.stream_id.as_string(),
                    &command.topic_id.as_string(),
                ),
                &command,
            )
            .await?;
        let offset = response.json().await?;
        Ok(offset)
    }
}

fn get_path(stream_id: &str, topic_id: &str) -> String {
    format!("streams/{}/topics/{}/consumer-offsets", stream_id, topic_id)
}
