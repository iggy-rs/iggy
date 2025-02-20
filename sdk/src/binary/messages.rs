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
use crate::models::messages::IggyMessage;

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
        let header = IggyHeader::from_bytes(&response[..IGGY_BATCH_OVERHEAD as usize]);
        let msg_count = header.last_offset_delta as usize + 1;
        let batch_payload = &response[IGGY_BATCH_OVERHEAD as usize..];
        let mut messages = Vec::with_capacity(msg_count);
        let mut position = 0;
        while position < batch_payload.len() {
            let offset_delta =
                u32::from_le_bytes(batch_payload[position..position + 4].try_into().unwrap());
            let timestamp_delta = u32::from_le_bytes(
                batch_payload[position + 4..position + 8]
                    .try_into()
                    .unwrap(),
            );
            let length = u64::from_le_bytes(
                batch_payload[position + 8..position + 16]
                    .try_into()
                    .unwrap(),
            );
            position += 16;
            let id = u128::from_le_bytes(batch_payload[position..position + 16].try_into().unwrap());
            let payload = batch_payload[position + 16..position + length as usize].to_vec();
            position += length as usize;
            let message = IggyMessage {
                id,
                offset_delta,
                timestamp_delta,
                length: payload.len() as u64,
                payload,
                headers: None
            };
            messages.push(message);
        }
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
