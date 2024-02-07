use crate::streaming::topics::consumer_group::ConsumerGroup;
use crate::streaming::topics::topic::Topic;
use iggy::error::IggyError;
use iggy::locking::IggySharedMutFn;
use tokio::sync::RwLock;

impl Topic {
    pub async fn load(&mut self) -> Result<(), IggyError> {
        let storage = self.storage.clone();
        storage.topic.load(self).await?;
        let consumer_groups = storage.topic.load_consumer_groups(self).await?;
        for consumer_group in consumer_groups {
            self.consumer_groups_ids.insert(
                consumer_group.name.clone(),
                consumer_group.consumer_group_id,
            );
            self.consumer_groups.insert(
                consumer_group.consumer_group_id,
                RwLock::new(ConsumerGroup::new(
                    self.topic_id,
                    consumer_group.consumer_group_id,
                    &consumer_group.name,
                    self.get_partitions_count(),
                )),
            );
        }
        Ok(())
    }

    pub async fn persist(&self) -> Result<(), IggyError> {
        self.storage.topic.save(self).await
    }

    pub async fn delete(&self) -> Result<(), IggyError> {
        for partition in self.get_partitions() {
            let partition = partition.read().await;
            partition.delete().await?;
        }

        self.storage.topic.delete(self).await
    }

    pub async fn persist_messages(&self) -> Result<(), IggyError> {
        for partition in self.get_partitions() {
            let mut partition = partition.write().await;
            for segment in partition.get_segments_mut() {
                segment.persist_messages().await?;
            }
        }

        Ok(())
    }

    pub async fn purge(&self) -> Result<(), IggyError> {
        for partition in self.get_partitions() {
            let mut partition = partition.write().await;
            partition.purge().await?;
        }
        Ok(())
    }
}
