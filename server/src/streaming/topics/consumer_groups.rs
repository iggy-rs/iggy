use crate::streaming::topics::consumer_group::ConsumerGroup;
use crate::streaming::topics::topic::Topic;
use iggy::error::Error;
use iggy::identifier::{IdKind, Identifier};
use iggy::utils::text;
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
    ) -> Result<&RwLock<ConsumerGroup>, Error> {
        match identifier.kind {
            IdKind::Numeric => self.get_consumer_group_by_id(identifier.get_u32_value().unwrap()),
            IdKind::String => {
                self.get_consumer_group_by_name(&identifier.get_string_value().unwrap())
            }
        }
    }

    pub fn get_consumer_group_by_name(&self, name: &str) -> Result<&RwLock<ConsumerGroup>, Error> {
        let consumer_group_id = self.consumer_groups_ids.get(name);
        if consumer_group_id.is_none() {
            return Err(Error::ConsumerGroupNameNotFound(
                name.to_string(),
                self.topic_id,
            ));
        }

        self.get_consumer_group_by_id(*consumer_group_id.unwrap())
    }

    pub fn get_consumer_group_by_id(&self, id: u32) -> Result<&RwLock<ConsumerGroup>, Error> {
        let consumer_group = self.consumer_groups.get(&id);
        if consumer_group.is_none() {
            return Err(Error::ConsumerGroupIdNotFound(id, self.topic_id));
        }

        Ok(consumer_group.unwrap())
    }

    pub async fn create_consumer_group(&mut self, id: u32, name: &str) -> Result<(), Error> {
        if self.consumer_groups.contains_key(&id) {
            return Err(Error::ConsumerGroupIdAlreadyExists(id, self.topic_id));
        }

        let name = text::to_lowercase_non_whitespace(name);
        if self.consumer_groups_ids.contains_key(&name) {
            return Err(Error::ConsumerGroupNameAlreadyExists(name, self.topic_id));
        }

        let consumer_group =
            ConsumerGroup::new(self.topic_id, id, &name, self.partitions.len() as u32);
        self.consumer_groups.insert(id, RwLock::new(consumer_group));
        self.consumer_groups_ids.insert(name, id);
        let consumer_group = self.get_consumer_group_by_id(id)?;
        let consumer_group = consumer_group.read().await;
        self.storage
            .topic
            .save_consumer_group(self, &consumer_group)
            .await?;
        info!(
            "Created consumer group with ID: {} for topic with ID: {} and stream with ID: {}.",
            id, self.topic_id, self.stream_id
        );
        Ok(())
    }

    pub async fn delete_consumer_group(
        &mut self,
        id: &Identifier,
    ) -> Result<RwLock<ConsumerGroup>, Error> {
        let consumer_group_id;
        {
            let consumer_group = self.get_consumer_group(id)?;
            let consumer_group = consumer_group.read().await;
            consumer_group_id = consumer_group.consumer_group_id;
        }

        let consumer_group = self.consumer_groups.remove(&consumer_group_id);
        if let Some(consumer_group) = consumer_group {
            {
                let consumer_group = consumer_group.read().await;
                self.consumer_groups_ids.remove(&consumer_group.name);
                self.storage
                    .topic
                    .delete_consumer_group(self, &consumer_group)
                    .await?;
                info!(
                    "Deleted consumer group with ID: {} from topic with ID: {} and stream with ID: {}.",
                    id, self.topic_id, self.stream_id
                );
            }
            return Ok(consumer_group);
        }

        Err(Error::ConsumerGroupIdNotFound(
            consumer_group_id,
            self.topic_id,
        ))
    }

    pub async fn join_consumer_group(
        &self,
        consumer_group_id: &Identifier,
        member_id: u32,
    ) -> Result<(), Error> {
        let consumer_group = self.get_consumer_group(consumer_group_id)?;
        let mut consumer_group = consumer_group.write().await;
        consumer_group.add_member(member_id).await;
        info!(
            "Member with ID: {} has joined consumer group with ID: {} for topic with ID: {} and stream with ID: {}.",
            member_id, consumer_group_id, self.topic_id, self.stream_id
        );
        Ok(())
    }

    pub async fn leave_consumer_group(
        &self,
        consumer_group_id: &Identifier,
        member_id: u32,
    ) -> Result<(), Error> {
        let consumer_group = self.get_consumer_group(consumer_group_id)?;
        let mut consumer_group = consumer_group.write().await;
        consumer_group.delete_member(member_id).await;
        info!(
            "Member with ID: {} has left consumer group with ID: {} for topic with ID: {} and stream with ID: {}.",
            member_id, consumer_group_id, self.topic_id, self.stream_id
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::system::SystemConfig;
    use crate::streaming::storage::tests::get_test_system_storage;
    use std::sync::Arc;

    #[tokio::test]
    async fn should_be_created_given_valid_parameters() {
        let consumer_group_id = 1;
        let name = "test";
        let mut topic = get_topic();
        let result = topic.create_consumer_group(consumer_group_id, name).await;
        assert!(result.is_ok());
        assert_eq!(topic.consumer_groups.len(), 1);
        let consumer_group = topic
            .get_consumer_group(&Identifier::numeric(consumer_group_id).unwrap())
            .unwrap();
        let consumer_group = consumer_group.read().await;
        assert_eq!(consumer_group.consumer_group_id, consumer_group_id);
        assert_eq!(consumer_group.name, name);
        assert_eq!(consumer_group.topic_id, topic.topic_id);
        assert_eq!(
            consumer_group.partitions_count,
            topic.partitions.len() as u32
        );
    }

    #[tokio::test]
    async fn should_not_be_created_given_already_existing_group_with_same_id() {
        let consumer_group_id = 1;
        let name = "test";
        let mut topic = get_topic();
        let result = topic.create_consumer_group(consumer_group_id, name).await;
        assert!(result.is_ok());
        assert_eq!(topic.consumer_groups.len(), 1);
        let result = topic.create_consumer_group(consumer_group_id, name).await;
        assert!(result.is_err());
        assert_eq!(topic.consumer_groups.len(), 1);
        let err = result.unwrap_err();
        assert!(matches!(err, Error::ConsumerGroupIdAlreadyExists(_, _)));
    }

    #[tokio::test]
    async fn should_not_be_created_given_already_existing_group_with_same_name() {
        let consumer_group_id = 1;
        let name = "test";
        let mut topic = get_topic();
        let result = topic.create_consumer_group(consumer_group_id, name).await;
        assert!(result.is_ok());
        assert_eq!(topic.consumer_groups.len(), 1);
        let consumer_group_id = consumer_group_id + 1;
        let result = topic.create_consumer_group(consumer_group_id, name).await;
        assert!(result.is_err());
        assert_eq!(topic.consumer_groups.len(), 1);
        let err = result.unwrap_err();
        assert!(matches!(err, Error::ConsumerGroupNameAlreadyExists(_, _)));
    }

    #[tokio::test]
    async fn should_be_deleted_given_already_existing_group_with_same_id() {
        let consumer_group_id = 1;
        let name = "test";
        let mut topic = get_topic();
        let result = topic.create_consumer_group(consumer_group_id, name).await;
        assert!(result.is_ok());
        assert_eq!(topic.consumer_groups.len(), 1);
        let result = topic
            .delete_consumer_group(&Identifier::numeric(consumer_group_id).unwrap())
            .await;
        assert!(result.is_ok());
        assert!(topic.consumer_groups.is_empty());
    }

    #[tokio::test]
    async fn should_not_be_deleted_given_non_existing_group_with_same_id() {
        let consumer_group_id = 1;
        let name = "test";
        let mut topic = get_topic();
        let result = topic.create_consumer_group(consumer_group_id, name).await;
        assert!(result.is_ok());
        assert_eq!(topic.consumer_groups.len(), 1);
        let consumer_group_id = consumer_group_id + 1;
        let result = topic
            .delete_consumer_group(&Identifier::numeric(consumer_group_id).unwrap())
            .await;
        assert!(result.is_err());
        assert_eq!(topic.consumer_groups.len(), 1);
    }

    #[tokio::test]
    async fn should_be_joined_by_new_member() {
        let consumer_group_id = 1;
        let name = "test";
        let member_id = 1;
        let mut topic = get_topic();
        topic
            .create_consumer_group(consumer_group_id, name)
            .await
            .unwrap();
        let result = topic
            .join_consumer_group(&Identifier::numeric(consumer_group_id).unwrap(), member_id)
            .await;
        assert!(result.is_ok());
        let consumer_group = topic
            .get_consumer_group(&Identifier::numeric(consumer_group_id).unwrap())
            .unwrap()
            .read()
            .await;
        let members = consumer_group.get_members();
        assert_eq!(members.len(), 1);
    }

    #[tokio::test]
    async fn should_be_left_by_existing_member() {
        let consumer_group_id = 1;
        let name = "test";
        let member_id = 1;
        let mut topic = get_topic();
        topic
            .create_consumer_group(consumer_group_id, name)
            .await
            .unwrap();
        topic
            .join_consumer_group(&Identifier::numeric(consumer_group_id).unwrap(), member_id)
            .await
            .unwrap();
        let result = topic
            .leave_consumer_group(&Identifier::numeric(consumer_group_id).unwrap(), member_id)
            .await;
        assert!(result.is_ok());
        let consumer_group = topic
            .get_consumer_group(&Identifier::numeric(consumer_group_id).unwrap())
            .unwrap()
            .read()
            .await;
        let members = consumer_group.get_members();
        assert!(members.is_empty())
    }

    fn get_topic() -> Topic {
        let storage = Arc::new(get_test_system_storage());
        let stream_id = 1;
        let id = 2;
        let name = "test";
        let partitions_count = 3;
        let config = Arc::new(SystemConfig::default());

        Topic::create(
            stream_id,
            id,
            name,
            partitions_count,
            config,
            storage,
            None,
            None,
            1,
        )
        .unwrap()
    }
}
