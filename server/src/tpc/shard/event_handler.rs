use iggy::error::IggyError;

use crate::streaming::session::Session;

use super::{shard::IggyShard, shard_frame::ShardEvent};

// TODO - give an non persist flag
impl IggyShard {
    pub async fn handle_event(&self, client_id: u32, event: ShardEvent) -> Result<(), IggyError> {
        match event {
            ShardEvent::CreatedStream(stream_id, name) => {
                self.create_stream(client_id, stream_id, name, false).await
            }
            ShardEvent::CreatedPartitions(stream_id, topic_id, partitions_count) => {
                self.create_partitions(client_id, &stream_id, &topic_id, partitions_count, false)
                    .await
            }
            ShardEvent::CreatedTopic(
                stream_id,
                topic_id,
                name,
                partitions_count,
                message_expiry,
                compression_algorithm,
                max_topic_size,
                replication_factor,
            ) => {
                self.create_topic(
                    client_id,
                    &stream_id,
                    topic_id,
                    name,
                    partitions_count,
                    message_expiry,
                    compression_algorithm,
                    max_topic_size,
                    replication_factor,
                    false,
                )
                .await
            }
            ShardEvent::LoginUser(username, password) => {
                self.login_user(username, password, client_id).await?;
                Ok(())
            }
            ShardEvent::NewSession(client_id, address) => {
                let session = Session::from_client_id(client_id, address);
                self.add_active_session(session);
                Ok(())
            }
        }
    }
}
