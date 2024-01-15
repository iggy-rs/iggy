use crate::binary;
use crate::client::ConsumerGroupClient;
use crate::consumer_groups::create_consumer_group::CreateConsumerGroup;
use crate::consumer_groups::delete_consumer_group::DeleteConsumerGroup;
use crate::consumer_groups::get_consumer_group::GetConsumerGroup;
use crate::consumer_groups::get_consumer_groups::GetConsumerGroups;
use crate::consumer_groups::join_consumer_group::JoinConsumerGroup;
use crate::consumer_groups::leave_consumer_group::LeaveConsumerGroup;
use crate::error::Error;
use crate::models::consumer_group::{ConsumerGroup, ConsumerGroupDetails};
use crate::quic::client::QuicClient;
use async_trait::async_trait;

#[async_trait]
impl ConsumerGroupClient for QuicClient {
    async fn get_consumer_group(
        &self,
        command: &GetConsumerGroup,
    ) -> Result<ConsumerGroupDetails, Error> {
        binary::consumer_groups::get_group(self, command).await
    }

    async fn get_consumer_groups(
        &self,
        command: &GetConsumerGroups,
    ) -> Result<Vec<ConsumerGroup>, Error> {
        binary::consumer_groups::get_groups(self, command).await
    }

    async fn create_consumer_group(&self, command: &CreateConsumerGroup) -> Result<(), Error> {
        binary::consumer_groups::create_group(self, command).await
    }

    async fn delete_consumer_group(&self, command: &DeleteConsumerGroup) -> Result<(), Error> {
        binary::consumer_groups::delete_group(self, command).await
    }

    async fn join_consumer_group(&self, command: &JoinConsumerGroup) -> Result<(), Error> {
        binary::consumer_groups::join_group(self, command).await
    }

    async fn leave_consumer_group(&self, command: &LeaveConsumerGroup) -> Result<(), Error> {
        binary::consumer_groups::leave_group(self, command).await
    }
}
