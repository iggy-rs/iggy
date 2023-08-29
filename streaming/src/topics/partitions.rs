use crate::partitions::partition::Partition;
use crate::topics::topic::Topic;
use iggy::error::Error;
use tokio::sync::RwLock;

const MAX_PARTITIONS_COUNT: u32 = 100000;

impl Topic {
    pub fn add_partitions(&mut self, count: u32) -> Result<Vec<u32>, Error> {
        if count == 0 {
            return Ok(vec![]);
        }

        let current_partitions_count = self.partitions.len() as u32;
        if current_partitions_count + count > MAX_PARTITIONS_COUNT {
            return Err(Error::TooManyPartitions);
        }

        let mut partition_ids = Vec::with_capacity(count as usize);
        for partition_id in current_partitions_count + 1..=current_partitions_count + count {
            let partition = Partition::create(
                self.stream_id,
                self.topic_id,
                partition_id,
                true,
                self.config.clone(),
                self.storage.clone(),
                self.message_expiry,
            );
            self.partitions.insert(partition_id, RwLock::new(partition));
            partition_ids.push(partition_id)
        }

        Ok(partition_ids)
    }

    pub async fn add_persisted_partitions(&mut self, count: u32) -> Result<Vec<u32>, Error> {
        let partition_ids = self.add_partitions(count)?;
        for partition_id in &partition_ids {
            let partition = self.partitions.get(partition_id).unwrap();
            let partition = partition.read().await;
            partition.persist().await?;
        }
        Ok(partition_ids)
    }

    pub async fn delete_persisted_partitions(&mut self, mut count: u32) -> Result<Vec<u32>, Error> {
        if count == 0 {
            return Ok(vec![]);
        }

        let current_partitions_count = self.partitions.len() as u32;
        if count > current_partitions_count {
            count = current_partitions_count;
        }

        let mut partition_ids = Vec::with_capacity(count as usize);
        for partition_id in current_partitions_count - count + 1..=current_partitions_count {
            let partition = self.partitions.remove(&partition_id).unwrap();
            let partition = partition.read().await;
            partition.delete().await?;
            partition_ids.push(partition_id)
        }
        Ok(partition_ids)
    }
}
