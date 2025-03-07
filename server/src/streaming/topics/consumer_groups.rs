use crate::streaming::topics::consumer_group::ConsumerGroup;
use crate::streaming::topics::topic::Topic;
use crate::streaming::topics::COMPONENT;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::identifier::Identifier;
use iggy::locking::IggySharedMutFn;
use std::sync::atomic::Ordering;
use tokio::sync::RwLock;
use tracing::info;

impl Topic {
    pub async fn reassign_consumer_groups(&mut self) {
        if self.consumer_groups.is_empty() {
            return;
        }

        let partitions_count = self.partitions.len() as u32;
        info!(
            "Reassigning consumer groups for topic with ID: {} for stream with ID with {}, partitions count: {}",
            self.topic_id, self.stream_id, partitions_count
        );
        for (_, consumer_group) in self.consumer_groups.iter_mut() {
            let mut consumer_group = consumer_group.write().await;
            consumer_group.reassign_partitions(partitions_count).await;
        }
    }

    pub fn get_consumer_groups(&self) -> Vec<&RwLock<ConsumerGroup>> {
        self.consumer_groups.values().collect()
    }

    pub fn get_consumer_group(
        &self,
        identifier: &Identifier,
    ) -> Result<&RwLock<ConsumerGroup>, IggyError> {
        match identifier {
            Identifier::Numeric(id) => self.get_consumer_group_by_id(*id),
            Identifier::String(id) => {
                self.get_consumer_group_by_name(&String::from_utf8_lossy(id.as_bytes()))
            }
        }
    }

    pub fn try_get_consumer_group(
        &self,
        identifier: &Identifier,
    ) -> Result<Option<&RwLock<ConsumerGroup>>, IggyError> {
        match identifier {
            Identifier::Numeric(id) => Ok(self.consumer_groups.get(id)),
            Identifier::String(id) => {
                Ok(self.try_get_consumer_group_by_name(&String::from_utf8_lossy(id.as_bytes())))
            }
        }
    }

    fn try_get_consumer_group_by_name(&self, name: &str) -> Option<&RwLock<ConsumerGroup>> {
        self.consumer_groups_ids
            .get(name)
            .and_then(|id| self.consumer_groups.get(id))
    }

    pub fn get_consumer_group_by_name(
        &self,
        name: &str,
    ) -> Result<&RwLock<ConsumerGroup>, IggyError> {
        let group_id = self.consumer_groups_ids.get(name);
        if group_id.is_none() {
            return Err(IggyError::ConsumerGroupNameNotFound(
                name.to_string(),
                self.name.to_owned(),
            ));
        }

        self.get_consumer_group_by_id(*group_id.unwrap())
    }

    pub fn get_consumer_group_by_id(&self, id: u32) -> Result<&RwLock<ConsumerGroup>, IggyError> {
        let consumer_group = self.consumer_groups.get(&id);
        if consumer_group.is_none() {
            return Err(IggyError::ConsumerGroupIdNotFound(id, self.topic_id));
        }

        Ok(consumer_group.unwrap())
    }

    pub async fn create_consumer_group(
        &mut self,
        group_id: Option<u32>,
        name: &str,
    ) -> Result<&RwLock<ConsumerGroup>, IggyError> {
        if self.consumer_groups_ids.contains_key(name) {
            return Err(IggyError::ConsumerGroupNameAlreadyExists(
                name.to_owned(),
                self.topic_id,
            ));
        }

        let mut id;
        if group_id.is_none() {
            id = self
                .current_consumer_group_id
                .fetch_add(1, Ordering::SeqCst);
            loop {
                if self.consumer_groups.contains_key(&id) {
                    if id == u32::MAX {
                        return Err(IggyError::ConsumerGroupIdAlreadyExists(id, self.topic_id));
                    }
                    id = self
                        .current_consumer_group_id
                        .fetch_add(1, Ordering::SeqCst);
                } else {
                    break;
                }
            }
        } else {
            id = group_id.unwrap();
        }

        if self.consumer_groups.contains_key(&id) {
            return Err(IggyError::ConsumerGroupIdAlreadyExists(id, self.topic_id));
        }

        let consumer_group =
            ConsumerGroup::new(self.topic_id, id, name, self.partitions.len() as u32);
        self.consumer_groups.insert(id, RwLock::new(consumer_group));
        self.consumer_groups_ids.insert(name.to_owned(), id);
        info!(
            "Created consumer group with ID: {} for topic with ID: {} and stream with ID: {}.",
            id, self.topic_id, self.stream_id
        );
        self.get_consumer_group_by_id(id)
    }

    pub async fn delete_consumer_group(
        &mut self,
        id: &Identifier,
    ) -> Result<RwLock<ConsumerGroup>, IggyError> {
        let group_id;
        {
            let consumer_group = self.get_consumer_group(id).with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get consumer group with id: {id}")
            })?;
            let consumer_group = consumer_group.read().await;
            group_id = consumer_group.group_id;
        }

        let consumer_group = self.consumer_groups.remove(&group_id);
        if consumer_group.is_none() {
            return Err(IggyError::ConsumerGroupIdNotFound(group_id, self.topic_id));
        }
        let consumer_group = consumer_group.unwrap();
        {
            let consumer_group = consumer_group.read().await;
            let group_id = consumer_group.group_id;
            self.consumer_groups_ids.remove(&consumer_group.name);
            let current_group_id = self.current_consumer_group_id.load(Ordering::SeqCst);
            if current_group_id > group_id {
                self.current_consumer_group_id
                    .store(group_id, Ordering::SeqCst);
            }

            for (_, partition) in self.partitions.iter() {
                let partition = partition.read().await;
                if let Some((_, offset)) = partition.consumer_group_offsets.remove(&group_id) {
                    self.storage
                        .partition
                        .delete_consumer_offset(&offset.path)
                        .await?;
                }
            }

            info!(
                "Deleted consumer group with ID: {} from topic with ID: {} and stream with ID: {}.",
                id, self.topic_id, self.stream_id
            );
        }

        Ok(consumer_group)
    }

    pub async fn join_consumer_group(
        &self,
        group_id: &Identifier,
        member_id: u32,
    ) -> Result<(), IggyError> {
        let consumer_group = self.get_consumer_group(group_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get consumer group with id: {group_id}")
        })?;
        let mut consumer_group = consumer_group.write().await;
        consumer_group.add_member(member_id).await;
        info!(
            "Member with ID: {} has joined consumer group with ID: {} for topic with ID: {} and stream with ID: {}.",
            member_id, group_id, self.topic_id, self.stream_id
        );
        Ok(())
    }

    pub async fn leave_consumer_group(
        &self,
        group_id: &Identifier,
        member_id: u32,
    ) -> Result<(), IggyError> {
        let consumer_group = self.get_consumer_group(group_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get consumer group with id: {group_id}")
        })?;
        let mut consumer_group = consumer_group.write().await;
        consumer_group.delete_member(member_id).await;
        info!(
            "Member with ID: {} has left consumer group with ID: {} for topic with ID: {} and stream with ID: {}.",
            member_id, group_id, self.topic_id, self.stream_id
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::system::SystemConfig;
    use crate::streaming::persistence::persister::{FileWithSyncPersister, PersisterKind};
    use crate::streaming::storage::SystemStorage;
    use iggy::compression::compression_algorithm::CompressionAlgorithm;
    use iggy::utils::expiry::IggyExpiry;
    use iggy::utils::topic_size::MaxTopicSize;
    use std::sync::atomic::{AtomicU32, AtomicU64};
    use std::sync::Arc;

    #[tokio::test]
    async fn should_be_created_given_valid_parameters() {
        let group_id = 1;
        let name = "test";
        let mut topic = get_topic().await;
        let topic_id = topic.topic_id;
        let result = topic.create_consumer_group(Some(group_id), name).await;
        assert!(result.is_ok());
        {
            let created_consumer_group = result.unwrap().read().await;
            assert_eq!(created_consumer_group.group_id, group_id);
            assert_eq!(created_consumer_group.name, name);
            assert_eq!(created_consumer_group.topic_id, topic_id);
        }

        assert_eq!(topic.consumer_groups.len(), 1);
        let consumer_group = topic
            .get_consumer_group(&Identifier::numeric(group_id).unwrap())
            .unwrap();
        let consumer_group = consumer_group.read().await;
        assert_eq!(consumer_group.group_id, group_id);
        assert_eq!(consumer_group.name, name);
        assert_eq!(consumer_group.topic_id, topic_id);
        assert_eq!(
            consumer_group.partitions_count,
            topic.partitions.len() as u32
        );
    }

    #[tokio::test]
    async fn should_not_be_created_given_already_existing_group_with_same_id() {
        let group_id = 1;
        let name = "test";
        let mut topic = get_topic().await;
        let result = topic.create_consumer_group(Some(group_id), name).await;
        assert!(result.is_ok());
        assert_eq!(topic.consumer_groups.len(), 1);
        let result = topic.create_consumer_group(Some(group_id), "test2").await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, IggyError::ConsumerGroupIdAlreadyExists(_, _)));
        assert_eq!(topic.consumer_groups.len(), 1);
    }

    #[tokio::test]
    async fn should_not_be_created_given_already_existing_group_with_same_name() {
        let group_id = 1;
        let name = "test";
        let mut topic = get_topic().await;
        let result = topic.create_consumer_group(Some(group_id), name).await;
        assert!(result.is_ok());
        assert_eq!(topic.consumer_groups.len(), 1);
        let group_id = group_id + 1;
        let result = topic.create_consumer_group(Some(group_id), name).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(
            err,
            IggyError::ConsumerGroupNameAlreadyExists(_, _)
        ));
        assert_eq!(topic.consumer_groups.len(), 1);
    }

    #[tokio::test]
    async fn should_be_deleted_given_already_existing_group_with_same_id() {
        let group_id = 1;
        let name = "test";
        let mut topic = get_topic().await;
        let result = topic.create_consumer_group(Some(group_id), name).await;
        assert!(result.is_ok());
        assert_eq!(topic.consumer_groups.len(), 1);
        let result = topic
            .delete_consumer_group(&Identifier::numeric(group_id).unwrap())
            .await;
        assert!(result.is_ok());
        assert!(topic.consumer_groups.is_empty());
    }

    #[tokio::test]
    async fn should_not_be_deleted_given_non_existing_group_with_same_id() {
        let group_id = 1;
        let name = "test";
        let mut topic = get_topic().await;
        let result = topic.create_consumer_group(Some(group_id), name).await;
        assert!(result.is_ok());
        assert_eq!(topic.consumer_groups.len(), 1);
        let group_id = group_id + 1;
        let result = topic
            .delete_consumer_group(&Identifier::numeric(group_id).unwrap())
            .await;
        assert!(result.is_err());
        assert_eq!(topic.consumer_groups.len(), 1);
    }

    #[tokio::test]
    async fn should_be_joined_by_new_member() {
        let group_id = 1;
        let name = "test";
        let member_id = 1;
        let mut topic = get_topic().await;
        topic
            .create_consumer_group(Some(group_id), name)
            .await
            .unwrap();
        let result = topic
            .join_consumer_group(&Identifier::numeric(group_id).unwrap(), member_id)
            .await;
        assert!(result.is_ok());
        let consumer_group = topic
            .get_consumer_group(&Identifier::numeric(group_id).unwrap())
            .unwrap()
            .read()
            .await;
        let members = consumer_group.get_members();
        assert_eq!(members.len(), 1);
    }

    #[tokio::test]
    async fn should_be_left_by_existing_member() {
        let group_id = 1;
        let name = "test";
        let member_id = 1;
        let mut topic = get_topic().await;
        topic
            .create_consumer_group(Some(group_id), name)
            .await
            .unwrap();
        topic
            .join_consumer_group(&Identifier::numeric(group_id).unwrap(), member_id)
            .await
            .unwrap();
        let result = topic
            .leave_consumer_group(&Identifier::numeric(group_id).unwrap(), member_id)
            .await;
        assert!(result.is_ok());
        let consumer_group = topic
            .get_consumer_group(&Identifier::numeric(group_id).unwrap())
            .unwrap()
            .read()
            .await;
        let members = consumer_group.get_members();
        assert!(members.is_empty())
    }

    async fn get_topic() -> Topic {
        let tempdir = tempfile::TempDir::new().unwrap();
        let config = Arc::new(SystemConfig {
            path: tempdir.path().to_str().unwrap().to_string(),
            ..Default::default()
        });
        let storage = Arc::new(SystemStorage::new(
            config.clone(),
            Arc::new(PersisterKind::FileWithSync(FileWithSyncPersister {})),
        ));
        let stream_id = 1;
        let id = 2;
        let name = "test";
        let compression_algorithm = CompressionAlgorithm::None;
        let partitions_count = 3;
        let size_of_parent_stream = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_stream = Arc::new(AtomicU64::new(0));
        let segments_count_of_parent_stream = Arc::new(AtomicU32::new(0));

        Topic::create(
            stream_id,
            id,
            name,
            partitions_count,
            config,
            storage,
            size_of_parent_stream,
            messages_count_of_parent_stream,
            segments_count_of_parent_stream,
            IggyExpiry::NeverExpire,
            compression_algorithm,
            MaxTopicSize::ServerDefault,
            1,
        )
        .await
        .unwrap()
    }
}
