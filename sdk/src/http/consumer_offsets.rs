use crate::client::ConsumerOffsetClient;
use crate::client_v2::ConsumerOffsetClientV2;
use crate::consumer::Consumer;
use crate::consumer_offsets::get_consumer_offset::GetConsumerOffset;
use crate::consumer_offsets::store_consumer_offset::StoreConsumerOffset;
use crate::error::IggyError;
use crate::http::client::HttpClient;
use crate::http::HttpTransport;
use crate::identifier::Identifier;
use crate::models::consumer_offset_info::ConsumerOffsetInfo;
use async_trait::async_trait;

#[async_trait]
impl ConsumerOffsetClient for HttpClient {
    async fn store_consumer_offset(&self, command: &StoreConsumerOffset) -> Result<(), IggyError> {
        store_consumer_offset(self, command).await
    }

    async fn get_consumer_offset(
        &self,
        command: &GetConsumerOffset,
    ) -> Result<ConsumerOffsetInfo, IggyError> {
        get_consumer_offset(self, command).await
    }
}

#[async_trait]
impl ConsumerOffsetClientV2 for HttpClient {
    async fn store_consumer_offset(
        &self,
        consumer: Consumer,
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: Option<u32>,
        offset: u64,
    ) -> Result<(), IggyError> {
        store_consumer_offset(
            self,
            &StoreConsumerOffset {
                consumer,
                stream_id,
                topic_id,
                partition_id,
                offset,
            },
        )
        .await
    }

    async fn get_consumer_offset(
        &self,
        consumer: Consumer,
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: Option<u32>,
    ) -> Result<ConsumerOffsetInfo, IggyError> {
        get_consumer_offset(
            self,
            &GetConsumerOffset {
                consumer,
                stream_id,
                topic_id,
                partition_id,
            },
        )
        .await
    }
}

async fn store_consumer_offset<T: HttpTransport>(
    transport: &T,
    command: &StoreConsumerOffset,
) -> Result<(), IggyError> {
    transport
        .put(
            &get_path(
                &command.stream_id.as_cow_str(),
                &command.topic_id.as_cow_str(),
            ),
            &command,
        )
        .await?;
    Ok(())
}

async fn get_consumer_offset<T: HttpTransport>(
    transport: &T,
    command: &GetConsumerOffset,
) -> Result<ConsumerOffsetInfo, IggyError> {
    let response = transport
        .get_with_query(
            &get_path(
                &command.stream_id.as_cow_str(),
                &command.topic_id.as_cow_str(),
            ),
            &command,
        )
        .await?;
    let offset = response.json().await?;
    Ok(offset)
}

fn get_path(stream_id: &str, topic_id: &str) -> String {
    format!("streams/{stream_id}/topics/{topic_id}/consumer-offsets")
}
