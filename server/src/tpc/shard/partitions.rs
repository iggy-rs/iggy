use std::borrow::BorrowMut;

use iggy::error::IggyError;
use iggy::identifier::Identifier;
use iggy::models::resource_namespace::IggyResourceNamespace;
use iggy::utils::hash::hash_resource_namespace;
use tracing::error;

use super::shard::{IggyShard, ShardInfo};

impl IggyShard {
    pub async fn create_partitions(
        &self,
        client_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
        should_persist: bool,
    ) -> Result<(), IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        let stream = self.get_stream(stream_id)?;
        let topic = stream.get_topic(topic_id)?;
        self.permissioner
            .borrow()
            .create_partitions(user_id, stream.stream_id, topic.topic_id)?;

        let mut stream = self.get_stream_mut(stream_id)?;
        let topic = stream.borrow_mut().get_topic_mut(topic_id)?;
        let partition_ids = topic
            .add_persisted_partitions(partitions_count, should_persist)
            .await?;
        for partition_id in partition_ids {
            let shards_count = self.get_available_shards_count();
            let hash = hash_resource_namespace(stream_id, topic_id, partition_id);
            let shard_id = hash % shards_count;
            error!("Shard ID: {}", shard_id);
            let resource_ns =
                IggyResourceNamespace::new(stream_id.clone(), topic_id.clone(), partition_id);
            let shard_info = ShardInfo {
                id: shard_id as u16,
            };
            self.insert_shart_table_record(resource_ns, shard_info);
        }

        topic.reassign_consumer_groups().await;
        self.metrics.increment_partitions(partitions_count);
        self.metrics.increment_segments(partitions_count);
        Ok(())
    }

    pub async fn delete_partitions(
        &self,
        client_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<(), IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        let stream = self.get_stream(stream_id)?;
        let topic = stream.get_topic(topic_id)?;
        self.permissioner
            .borrow()
            .delete_partitions(user_id, stream.stream_id, topic.topic_id)?;

        let mut stream = self.get_stream_mut(stream_id)?;
        let topic = stream.borrow_mut().get_topic_mut(topic_id)?;
        let partitions = topic.delete_persisted_partitions(partitions_count).await?;
        topic.reassign_consumer_groups().await;
        if let Some(partitions) = partitions {
            self.metrics.decrement_partitions(partitions_count);
            self.metrics.decrement_segments(partitions.segments_count);
            self.metrics.decrement_messages(partitions.messages_count);
        }
        Ok(())
    }
}
