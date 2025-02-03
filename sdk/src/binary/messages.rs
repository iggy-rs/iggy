use tokio::time::Instant;
use tracing::{error, warn};

use crate::binary::binary_client::BinaryClient;
use crate::binary::{fail_if_not_authenticated, mapper};
use crate::client::MessageClient;
use crate::command::{POLL_MESSAGES_CODE, SEND_MESSAGES_CODE};
use crate::consumer::Consumer;
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::messages::flush_unsaved_buffer::FlushUnsavedBuffer;
use crate::messages::poll_messages::PollingStrategy;
use crate::messages::send_messages::{Message, Partitioning};
use crate::messages::{poll_messages, send_messages};
use crate::models::batch::{IggyBatch, IggyHeader, IGGY_BATCH_OVERHEAD};
use crate::models::messages::{ArchivedIggyMessage, IggyMessage};

#[async_trait::async_trait]
impl<B: BinaryClient> MessageClient for B {
    async fn poll_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        consumer: &Consumer,
        strategy: &PollingStrategy,
        count: u32,
        auto_commit: bool,
    ) -> Result<Vec<IggyMessage>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_raw_with_response(
                POLL_MESSAGES_CODE,
                poll_messages::as_bytes(
                    stream_id,
                    topic_id,
                    partition_id,
                    consumer,
                    strategy,
                    count,
                    auto_commit,
                ),
            )
            .await?;
        let start = Instant::now();
        let header = IggyHeader::from_bytes(&response[..IGGY_BATCH_OVERHEAD as usize]);
        let msg_count = header.last_offset_delta as usize + 1;
        let mut messages = Vec::with_capacity(msg_count);
        let batch_payload = &response[IGGY_BATCH_OVERHEAD as usize..];
        let mut position = 0;
        let mut count = 0;
        while position < batch_payload.len() {
            let length =
                u64::from_le_bytes(batch_payload[position..position + 8].try_into().unwrap());
            let length = length as usize;
            position += 8;
            let message = unsafe {
                rkyv::access_unchecked::<ArchivedIggyMessage>(
                    &batch_payload[position..length + position],
                )
            };
            let message = rkyv::deserialize::<IggyMessage, rkyv::rancor::Error>(message).unwrap();
            messages.push(message);
            position += length;
            count += 1;
        }
        let elapsed = start.elapsed().as_micros();
        error!("deserializing batch of {} messages took: {} us", count, elapsed);
        Ok(messages)
    }

    async fn send_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        messages: Vec<Message>,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        let messages = messages.into_iter().map(Into::into).collect();
        let bytes = send_messages::as_bytes(stream_id, topic_id, partitioning, messages);
        self.send_rkyv_with_response(SEND_MESSAGES_CODE, bytes)
            .await?;
        Ok(())
    }

    async fn flush_unsaved_buffer(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: u32,
        fsync: bool,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&FlushUnsavedBuffer {
            stream_id: stream_id.clone(),
            topic_id: topic_id.clone(),
            partition_id,
            fsync,
        })
        .await?;
        Ok(())
    }
}
