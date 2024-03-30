use crate::client::MessageClient;
use crate::consumer::Consumer;
use crate::error::IggyError;
use crate::http::client::HttpClient;
use crate::http::HttpTransport;
use crate::identifier::Identifier;
use crate::messages::poll_messages::{PollMessages, PollingStrategy};
use crate::messages::send_messages::{Message, Partitioning, SendMessages};
use crate::models::messages::PolledMessages;
use crate::next_client::MessageClientNext;
use async_trait::async_trait;

#[async_trait]
impl MessageClient for HttpClient {
    async fn poll_messages(&self, command: &PollMessages) -> Result<PolledMessages, IggyError> {
        poll_messages(self, command).await
    }

    async fn send_messages(&self, command: &mut SendMessages) -> Result<(), IggyError> {
        send_messages(self, command).await
    }
}

#[async_trait]
impl MessageClientNext for HttpClient {
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
                messages: messages.to_vec(),
            },
        )
        .await
    }
}

async fn poll_messages<T: HttpTransport>(
    transport: &T,
    command: &PollMessages,
) -> Result<PolledMessages, IggyError> {
    let response = transport
        .get_with_query(
            &get_path(
                &command.stream_id.as_cow_str(),
                &command.topic_id.as_cow_str(),
            ),
            &command,
        )
        .await?;
    let messages = response.json().await?;
    Ok(messages)
}

async fn send_messages<T: HttpTransport>(
    transport: &T,
    command: &mut SendMessages,
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

fn get_path(stream_id: &str, topic_id: &str) -> String {
    format!("streams/{stream_id}/topics/{topic_id}/messages")
}
