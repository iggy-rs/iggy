use std::borrow::Borrow;

use crate::streaming::topics::consumer_group::ConsumerGroup;
use iggy::error::IggyError;
use iggy::identifier::Identifier;

use super::shard::IggyShard;

impl IggyShard {
    pub fn get_consumer_group(
        &self,
        client_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<&ConsumerGroup, IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        let stream = self.get_stream(stream_id)?.borrow();
        let topic = stream.get_topic(topic_id)?;
        self.permissioner
            .borrow()
            .get_consumer_group(user_id, stream.stream_id, topic.topic_id)?;

        topic.get_consumer_group(group_id)
    }

    pub fn get_consumer_groups(
        &self,
        client_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Vec<&ConsumerGroup>, IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        let stream = self.get_stream(stream_id)?;
        let topic = stream.get_topic(topic_id)?;
        self.permissioner.borrow().get_consumer_groups(
            user_id,
            stream.stream_id,
            topic.topic_id,
        )?;

        Ok(topic.get_consumer_groups())
    }

    pub async fn create_consumer_group(
        &self,
        client_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: Option<u32>,
        name: String,
    ) -> Result<(), IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        let stream = self.get_stream(stream_id)?;
        let topic = stream.get_topic(topic_id)?;
        self.permissioner.borrow().create_consumer_group(
            user_id,
            stream.stream_id,
            topic.topic_id,
        )?;

        let topic = self.get_stream_mut(stream_id)?.get_topic_mut(topic_id)?;
        topic.create_consumer_group(group_id, name).await?;
        Ok(())
    }

    pub async fn delete_consumer_group(
        &self,
        client_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
        consumer_group_id: &Identifier,
    ) -> Result<(), IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        let stream = self.get_stream(stream_id)?;
        let topic = stream.get_topic(topic_id)?;
        self.permissioner.borrow().delete_consumer_group(
            user_id,
            stream.stream_id,
            topic.topic_id,
        )?;
        let stream_id_value = stream.stream_id;
        let topic_id_value = topic.topic_id;

        let consumer_group;
        {
            let stream = self.get_stream_mut(stream_id)?;
            let topic = stream.get_topic_mut(topic_id)?;
            consumer_group = topic.delete_consumer_group(consumer_group_id).await?;
        }

        let client_manager = self.client_manager.borrow_mut();
        let consumer_group = consumer_group.read().await;
        for member in consumer_group.get_members() {
            let member = member.read().await;
            client_manager
                .leave_consumer_group(
                    member.id,
                    stream_id_value,
                    topic_id_value,
                    consumer_group.group_id,
                )
                .await?;
        }

        Ok(())
    }

    pub async fn join_consumer_group(
        &self,
        client_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
        consumer_group_id: &Identifier,
    ) -> Result<(), IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        let stream = self.get_stream(stream_id)?;
        let topic = stream.get_topic(topic_id)?;
        self.permissioner.borrow().join_consumer_group(
            user_id,
            stream.stream_id,
            topic.topic_id,
        )?;
        let stream_id_value = stream.stream_id;
        let topic_id_value = topic.topic_id;

        let group_id;
        {
            let stream = self.get_stream(stream_id)?;
            let topic = stream.get_topic(topic_id)?;

            {
                let consumer_group = topic.get_consumer_group(consumer_group_id)?;
                let consumer_group = consumer_group.read().await;
                group_id = consumer_group.group_id;
            }

            topic
                .join_consumer_group(consumer_group_id, client_id)
                .await?;
        }

        let client_manager = self.client_manager.borrow_mut();
        client_manager
            .join_consumer_group(client_id, stream_id_value, topic_id_value, group_id)
            .await?;
        Ok(())
    }

    pub async fn leave_consumer_group(
        &self,
        client_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
        consumer_group_id: &Identifier,
    ) -> Result<(), IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        let stream = self.get_stream(stream_id)?;
        let topic = stream.get_topic(topic_id)?;
        self.permissioner.borrow().leave_consumer_group(
            user_id,
            stream.stream_id,
            topic.topic_id,
        )?;

        self.leave_consumer_group_by_client(stream_id, topic_id, consumer_group_id, client_id)
            .await
    }

    pub async fn leave_consumer_group_by_client(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        consumer_group_id: &Identifier,
        client_id: u32,
    ) -> Result<(), IggyError> {
        let stream_id_value;
        let topic_id_value;
        let group_id;
        {
            let stream = self.get_stream(stream_id)?;
            let topic = stream.get_topic(topic_id)?;

            {
                let consumer_group = topic.get_consumer_group(consumer_group_id)?;
                let consumer_group = consumer_group.read().await;
                group_id = consumer_group.group_id;
            }

            stream_id_value = stream.stream_id;
            topic_id_value = topic.topic_id;
            topic
                .leave_consumer_group(consumer_group_id, client_id)
                .await?;
        }

        let client_manager = self.client_manager.borrow_mut();
        client_manager
            .leave_consumer_group(client_id, stream_id_value, topic_id_value, group_id)
            .await
    }
}
