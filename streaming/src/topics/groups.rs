use crate::topics::consumer_group::ConsumerGroup;
use crate::topics::topic::Topic;
use sdk::error::Error;
use tokio::sync::RwLock;
use tracing::info;

impl Topic {
    pub fn get_consumer_groups(&self) -> Vec<&RwLock<ConsumerGroup>> {
        self.consumer_groups.values().collect()
    }

    pub fn get_consumer_group(&self, id: u32) -> Result<&RwLock<ConsumerGroup>, Error> {
        let consumer_group = self.consumer_groups.get(&id);
        if consumer_group.is_none() {
            return Err(Error::ConsumerGroupNotFound(id, self.id));
        }

        Ok(consumer_group.unwrap())
    }

    pub fn create_consumer_group(&mut self, id: u32) -> Result<(), Error> {
        if self
            .consumer_groups
            .insert(
                id,
                RwLock::new(ConsumerGroup::new(
                    self.id,
                    id,
                    self.partitions.len() as u32,
                )),
            )
            .is_none()
        {
            info!(
                "Created consumer group with ID: {} for topic with ID: {} and stream with ID: {}.",
                id, self.id, self.stream_id
            );
            return Ok(());
        }

        Err(Error::ConsumerGroupAlreadyExists(id, self.id))
    }

    pub fn delete_consumer_group(&mut self, id: u32) -> Result<RwLock<ConsumerGroup>, Error> {
        let consumer_group = self.consumer_groups.remove(&id);
        if let Some(consumer_group) = consumer_group {
            info!(
                "Deleted consumer group with ID: {} from topic with ID: {} and stream with ID: {}.",
                id, self.id, self.stream_id
            );
            return Ok(consumer_group);
        }

        Err(Error::ConsumerGroupNotFound(id, self.id))
    }

    pub async fn join_consumer_group(&self, group_id: u32, member_id: u32) -> Result<(), Error> {
        let consumer_group = self.get_consumer_group(group_id)?;
        let mut consumer_group = consumer_group.write().await;
        consumer_group.add_member(member_id).await;
        info!(
            "Member with ID: {} has joined consumer group with ID: {} for topic with ID: {} and stream with ID: {}.",
            member_id, group_id, self.id, self.stream_id
        );
        Ok(())
    }

    pub async fn leave_consumer_group(&self, group_id: u32, member_id: u32) -> Result<(), Error> {
        let consumer_group = self.get_consumer_group(group_id)?;
        let mut consumer_group = consumer_group.write().await;
        consumer_group.delete_member(member_id).await;
        info!(
            "Member with ID: {} has left consumer group with ID: {} for topic with ID: {} and stream with ID: {}.",
            member_id, group_id, self.id, self.stream_id
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TopicConfig;
    use crate::storage::tests::get_test_system_storage;
    use std::sync::Arc;

    #[test]
    fn should_be_created_given_valid_parameters() {
        let group_id = 1;
        let mut topic = get_topic();
        let result = topic.create_consumer_group(group_id);
        assert!(result.is_ok());
    }

    #[test]
    fn should_not_be_created_given_already_existing_group_with_same_id() {
        let group_id = 1;
        let mut topic = get_topic();
        let result = topic.create_consumer_group(group_id);
        assert!(result.is_ok());
        assert_eq!(topic.consumer_groups.len(), 1);
        let result = topic.create_consumer_group(group_id);
        assert!(result.is_err());
        assert_eq!(topic.consumer_groups.len(), 1);
        let err = result.unwrap_err();
        assert!(matches!(err, Error::ConsumerGroupAlreadyExists(_, _)));
    }

    #[test]
    fn should_be_deleted_given_already_existing_group_with_same_id() {
        let group_id = 1;
        let mut topic = get_topic();
        let result = topic.create_consumer_group(group_id);
        assert!(result.is_ok());
        assert_eq!(topic.consumer_groups.len(), 1);
        let result = topic.delete_consumer_group(group_id);
        assert!(result.is_ok());
        assert!(topic.consumer_groups.is_empty());
    }

    #[test]
    fn should_not_be_deleted_given_non_existing_group_with_same_id() {
        let group_id = 1;
        let mut topic = get_topic();
        let result = topic.create_consumer_group(group_id);
        assert!(result.is_ok());
        assert_eq!(topic.consumer_groups.len(), 1);
        let result = topic.delete_consumer_group(group_id + 1);
        assert!(result.is_err());
        assert_eq!(topic.consumer_groups.len(), 1);
    }

    #[tokio::test]
    async fn should_be_joined_by_new_member() {
        let group_id = 1;
        let member_id = 1;
        let mut topic = get_topic();
        topic.create_consumer_group(group_id).unwrap();
        let result = topic.join_consumer_group(group_id, member_id).await;
        assert!(result.is_ok());
        let group = topic.get_consumer_group(group_id).unwrap().read().await;
        let members = group.get_members();
        assert_eq!(members.len(), 1);
    }

    #[tokio::test]
    async fn should_be_left_by_existing_member() {
        let group_id = 1;
        let member_id = 1;
        let mut topic = get_topic();
        topic.create_consumer_group(group_id).unwrap();
        topic
            .join_consumer_group(group_id, member_id)
            .await
            .unwrap();
        let result = topic.leave_consumer_group(group_id, member_id).await;
        assert!(result.is_ok());
        let group = topic.get_consumer_group(group_id).unwrap().read().await;
        let members = group.get_members();
        assert!(members.is_empty())
    }

    fn get_topic() -> Topic {
        let storage = Arc::new(get_test_system_storage());
        let stream_id = 1;
        let id = 2;
        let topics_path = "/topics";
        let name = "test";
        let partitions_count = 3;
        let config = Arc::new(TopicConfig::default());

        Topic::create(
            stream_id,
            id,
            name,
            partitions_count,
            topics_path,
            config,
            storage,
        )
    }
}
