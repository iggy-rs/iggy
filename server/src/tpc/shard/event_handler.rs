use iggy::error::IggyError;
use tracing::error;

use crate::streaming::{clients::client_manager::Transport, session::Session};

use super::{shard::IggyShard, shard_frame::ShardEvent};

impl IggyShard {
    pub async fn handle_event(&self, client_id: u32, event: ShardEvent) -> Result<(), IggyError> {
        match event {
            ShardEvent::CreatedStream(stream_id, name) => {
                let user_id = self.ensure_authenticated(client_id)?;
                self.create_stream(user_id, stream_id, name, false).await
            }
            ShardEvent::CreatedPartitions(stream_id, topic_id, partitions_count) => {
                let user_id = self.ensure_authenticated(client_id)?;
                self.create_partitions(user_id, &stream_id, &topic_id, partitions_count, false)
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
                let user_id = self.ensure_authenticated(client_id)?;
                self.create_topic(
                    user_id,
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
            ShardEvent::NewSession(client_id, address, transport) => {
                self.add_client(&address, transport).await;
                let session = Session::from_client_id(client_id, address);
                self.add_active_session(session);
                Ok(())
            }
            ShardEvent::DeletedStream(stream_id) => {
                let user_id = self.ensure_authenticated(client_id)?;
                let _ = self.delete_stream(user_id, &stream_id, false).await?;
                Ok(())
            }
            ShardEvent::UpdatedStream(stream_id, name) => {
                let user_id = self.ensure_authenticated(client_id)?;
                self.update_stream(user_id, &stream_id, name, false).await?;
                Ok(())
            }
            ShardEvent::PurgedStream(stream_id) => {
                let user_id = self.ensure_authenticated(client_id)?;
                self.purge_stream(user_id, &stream_id, false).await?;
                Ok(())
            }
            ShardEvent::DeletedPartitions(stream_id, topic_id, partitions_count) => {
                let user_id = self.ensure_authenticated(client_id)?;
                self.delete_partitions(user_id, &stream_id, &topic_id, partitions_count, false)
                    .await?;
                Ok(())
            }
            ShardEvent::CreatedConsumerGroup(stream_id, topic_id, group_id, name) => {
                let user_id = self.ensure_authenticated(client_id)?;
                self.create_consumer_group(user_id, &stream_id, &topic_id, group_id, name)
                    .await?;
                Ok(())
            }
            ShardEvent::DeletedConsumerGroup(stream_id, topic_id, group_id) => {
                let user_id = self.ensure_authenticated(client_id)?;
                self.delete_consumer_group(user_id, &stream_id, &topic_id, &group_id)
                    .await?;
                Ok(())
            }
            ShardEvent::UpdatedTopic(
                stream_id,
                topic_id,
                name,
                message_expiry,
                compression_algorithm,
                max_topic_size,
                replication_factor,
            ) => {
                let user_id = self.ensure_authenticated(client_id)?;
                self.update_topic(
                    user_id,
                    &stream_id,
                    &topic_id,
                    name,
                    message_expiry,
                    compression_algorithm,
                    max_topic_size,
                    replication_factor,
                    false,
                )
                .await?;
                Ok(())
            }
            ShardEvent::PurgedTopic(stream_id, topic_id) => {
                let user_id = self.ensure_authenticated(client_id)?;
                self.purge_topic(user_id, &stream_id, &topic_id, false)
                    .await?;
                Ok(())
            }
            ShardEvent::DeletedTopic(stream_id, topic_id) => {
                let user_id = self.ensure_authenticated(client_id)?;
                self.delete_topic(user_id, &stream_id, &topic_id, false)
                    .await?;
                Ok(())
            }
            ShardEvent::CreatedUser(username, password, status, permissions) => {
                let user_id = self.ensure_authenticated(client_id)?;
                self.create_user(user_id, username, password, status, permissions)
                    .await?;
                Ok(())
            }
            ShardEvent::DeletedUser(user_id) => {
                let user_numeric_id = self.ensure_authenticated(client_id)?;
                self.delete_user(user_numeric_id, &user_id).await?;
                Ok(())
            }
            ShardEvent::LogoutUser => {
                let _ = self.ensure_authenticated(client_id)?;
                self.logout_user(client_id).await?;
                Ok(())
            }
            ShardEvent::UpdatedUser(user_id, username, status) => {
                let user_numeric_id = self.ensure_authenticated(client_id)?;
                self.update_user(user_numeric_id, &user_id, username, status)
                    .await?;
                Ok(())
            }
            ShardEvent::ChangedPassword(user_id, current_password, new_password) => {
                let user_numeric_id = self.ensure_authenticated(client_id)?;
                self.change_password(user_numeric_id, &user_id, current_password, new_password)
                    .await?;
                Ok(())
            }
            ShardEvent::CreatedPersonalAccessToken(name, expiry) => {
                let user_id = self.ensure_authenticated(client_id)?;
                self.create_personal_access_token(user_id, name, expiry)
                    .await?;
                Ok(())
            }
            ShardEvent::DeletedPersonalAccessToken(token) => {
                let _ = self.ensure_authenticated(client_id)?;
                self.delete_personal_access_token(client_id, token).await?;
                Ok(())
            }
            ShardEvent::LoginWithPersonalAccessToken(token) => {
                let _ = self
                    .login_with_personal_access_token(client_id, token)
                    .await?;
                Ok(())
            }
            ShardEvent::StoredConsumerOffset(stream_id, topic_id, consumer, offset) => {
                self.store_consumer_offset(
                    client_id, consumer, &stream_id, &topic_id, offset, false,
                )
                .await?;
                Ok(())
            }
        }
    }
}
