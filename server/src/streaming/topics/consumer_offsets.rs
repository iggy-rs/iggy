use crate::streaming::polling_consumer::PollingConsumer;
use crate::streaming::topics::topic::Topic;
use crate::streaming::topics::COMPONENT;
use error_set::ErrContext;
use iggy::consumer::Consumer;
use iggy::error::IggyError;
use iggy::locking::IggySharedMutFn;
use iggy::models::consumer_offset_info::ConsumerOffsetInfo;

impl Topic {
    pub async fn store_consumer_offset(
        &self,
        consumer: Consumer,
        offset: u64,
        partition_id: Option<u32>,
        client_id: u32,
    ) -> Result<(), IggyError> {
        let Some((polling_consumer, partition_id)) = self
            .resolve_consumer_with_partition_id(&consumer, client_id, partition_id, false)
            .await
            .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to resolve consumer with partition id, consumer ID: {}, client ID: {}, partition ID: {:?}", consumer.id, client_id, partition_id))? else {
            return Err(IggyError::ConsumerOffsetNotFound(client_id));
        };

        let partition = self.get_partition(partition_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get partition with id: {partition_id}")
        })?;
        let partition = partition.read().await;
        partition
            .store_consumer_offset(polling_consumer, offset)
            .await
            .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to store consumer offset, consumer: {polling_consumer}, offset: {offset}"))
    }

    pub async fn store_consumer_offset_internal(
        &self,
        consumer: PollingConsumer,
        offset: u64,
        partition_id: u32,
    ) -> Result<(), IggyError> {
        let partition = self.get_partition(partition_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get partition with id: {partition_id}")
        })?;
        let partition = partition.read().await;
        partition.store_consumer_offset(consumer, offset).await.with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to store consumer offset, consumer: {consumer}, offset: {offset}"))
    }

    pub async fn get_consumer_offset(
        &self,
        consumer: &Consumer,
        partition_id: Option<u32>,
        client_id: u32,
    ) -> Result<Option<ConsumerOffsetInfo>, IggyError> {
        let Some((polling_consumer, partition_id)) = self
            .resolve_consumer_with_partition_id(consumer, client_id, partition_id, false)
            .await
            .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to resolve consumer offset for consumer: {consumer}, client ID: {client_id}, partition ID: {:#?}", partition_id))? else {
            return Ok(None);
        };

        let partition = self.get_partition(partition_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get partition with ID: {partition_id}")
        })?;
        let partition = partition.read().await;
        let offset = partition
            .get_consumer_offset(polling_consumer)
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to get consumer offset for consumer: {polling_consumer}"
                )
            })?;
        let Some(offset) = offset else {
            return Ok(None);
        };

        Ok(Some(ConsumerOffsetInfo {
            partition_id: partition.partition_id,
            current_offset: partition.current_offset,
            stored_offset: offset,
        }))
    }

    pub async fn delete_consumer_offset(
        &self,
        consumer: Consumer,
        partition_id: Option<u32>,
        client_id: u32,
    ) -> Result<(), IggyError> {
        let Some((polling_consumer, partition_id)) = self
            .resolve_consumer_with_partition_id(&consumer, client_id, partition_id, false)
            .await
            .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to resolve consumer with partition id, consumer ID: {}, client ID: {}, partition ID: {:?}", consumer.id, client_id, partition_id))? else {
            return Err(IggyError::ConsumerOffsetNotFound(client_id));
        };

        let partition = self.get_partition(partition_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get partition with id: {partition_id}")
        })?;
        let mut partition = partition.write().await;
        partition
            .delete_consumer_offset(polling_consumer)
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to delete consumer offset for consumer: {polling_consumer}, in topic with ID: {}, partition ID: {partition_id}",
                    self.topic_id
                )
            })
    }
}
