use crate::systems::system::System;
use iggy::error::Error;
use iggy::identifier::Identifier;

impl System {
    pub async fn create_consumer_group(
        &mut self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        consumer_group_id: u32,
    ) -> Result<(), Error> {
        self.get_stream_mut(stream_id)?
            .get_topic_mut(topic_id)?
            .create_consumer_group(consumer_group_id)
            .await?;
        Ok(())
    }

    pub async fn delete_consumer_group(
        &mut self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        consumer_group_id: u32,
    ) -> Result<(), Error> {
        let stream_id_value;
        let topic_id_value;
        let consumer_group;
        {
            let stream = self.get_stream_mut(stream_id)?;
            stream_id_value = stream.id;
            let topic = stream.get_topic_mut(topic_id)?;
            topic_id_value = topic.topic_id;
            consumer_group = topic.delete_consumer_group(consumer_group_id).await?;
        }

        let client_manager = self.client_manager.read().await;
        let consumer_group = consumer_group.read().await;
        for member in consumer_group.get_members() {
            let member = member.read().await;
            client_manager
                .leave_consumer_group(
                    member.id,
                    stream_id_value,
                    topic_id_value,
                    consumer_group_id,
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
        consumer_group_id: u32,
    ) -> Result<(), Error> {
        let stream_id_value;
        let topic_id_value;
        {
            let stream = self.get_stream(stream_id)?;
            stream_id_value = stream.id;
            let topic = stream.get_topic(topic_id)?;
            topic_id_value = topic.topic_id;
            topic
                .join_consumer_group(consumer_group_id, client_id)
                .await?;
        }

        let client_manager = self.client_manager.read().await;
        client_manager
            .join_consumer_group(
                client_id,
                stream_id_value,
                topic_id_value,
                consumer_group_id,
            )
            .await?;
        Ok(())
    }

    pub async fn leave_consumer_group(
        &self,
        client_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
        consumer_group_id: u32,
    ) -> Result<(), Error> {
        let stream_id_value;
        let topic_id_value;
        {
            let stream = self.get_stream(stream_id)?;
            stream_id_value = stream.id;
            let topic = stream.get_topic(topic_id)?;
            topic_id_value = topic.topic_id;
            topic
                .leave_consumer_group(consumer_group_id, client_id)
                .await?;
        }

        let client_manager = self.client_manager.read().await;
        client_manager
            .leave_consumer_group(
                client_id,
                stream_id_value,
                topic_id_value,
                consumer_group_id,
            )
            .await?;
        Ok(())
    }
}
