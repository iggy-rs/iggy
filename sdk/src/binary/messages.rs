use crate::binary::binary_client::{BinaryClient, BinaryClientNext};
use crate::binary::{fail_if_not_authenticated, mapper, BinaryTransport};
use crate::bytes_serializable::BytesSerializable;
use crate::client::MessageClient;
use crate::command::{POLL_MESSAGES_CODE, SEND_MESSAGES_CODE};
use crate::consumer::Consumer;
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::messages::poll_messages::{PollMessages, PollingStrategy};
use crate::messages::send_messages::{Message, Partitioning, SendMessages};
use crate::models::messages::PolledMessages;
use crate::next_client::MessageClientNext;

#[async_trait::async_trait]
impl<B: BinaryClient> MessageClient for B {
    async fn poll_messages(&self, command: &PollMessages) -> Result<PolledMessages, IggyError> {
        poll_messages(self, command).await
    }

    async fn send_messages(&self, command: &mut SendMessages) -> Result<(), IggyError> {
        send_messages(self, command).await
    }
}

#[async_trait::async_trait]
impl<B: BinaryClientNext> MessageClientNext for B {
    async fn poll_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        consumer: &Consumer,
        strategy: &PollingStrategy,
        count: u32,
        auto_commit: bool,
    ) -> Result<PolledMessages, IggyError> {
        poll_messages(
            self,
            &PollMessages {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
                partition_id,
                consumer: consumer.clone(),
                strategy: *strategy,
                count,
                auto_commit,
            },
        )
        .await
    }

    async fn send_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        messages: &mut [Message],
    ) -> Result<(), IggyError> {
        send_messages(
            self,
            &mut SendMessages {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
                partitioning: partitioning.clone(),
                messages: messages.to_vec(), // TODO: Improve this in a final version
            },
        )
        .await
    }
}

async fn poll_messages<T: BinaryTransport>(
    transport: &T,
    command: &PollMessages,
) -> Result<PolledMessages, IggyError> {
    fail_if_not_authenticated(transport).await?;
    let response = transport
        .send_with_response(POLL_MESSAGES_CODE, command.as_bytes())
        .await?;
    mapper::map_polled_messages(response)
}

async fn send_messages<T: BinaryTransport>(
    transport: &T,
    command: &mut SendMessages,
) -> Result<(), IggyError> {
    fail_if_not_authenticated(transport).await?;
    transport
        .send_with_response(SEND_MESSAGES_CODE, command.as_bytes())
        .await?;
    Ok(())
}
