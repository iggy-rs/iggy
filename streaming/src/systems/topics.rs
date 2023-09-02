use crate::systems::system::System;
use iggy::error::Error;
use iggy::identifier::Identifier;

impl System {
    pub async fn delete_topic(
        &mut self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), Error> {
        let stream_id_value;
        {
            let stream = self.get_stream(stream_id)?;
            stream_id_value = stream.id;
        }

        let deleted_topic_id = self
            .get_stream_mut(stream_id)?
            .delete_topic(topic_id)
            .await?;
        let client_manager = self.client_manager.read().await;
        client_manager
            .delete_consumer_groups_for_topic(stream_id_value, deleted_topic_id)
            .await;
        Ok(())
    }
}
