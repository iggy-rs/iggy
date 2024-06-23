use crate::binary::mapper;
use crate::streaming::polling_consumer::PollingConsumer;
use crate::tpc::shard::shard::IggyShard;
use crate::tpc::shard::shard_frame::ShardResponse;
use bytes::Bytes;
use iggy::command::Command;
use iggy::consumer_groups::create_consumer_group::CreateConsumerGroup;
use iggy::consumer_groups::delete_consumer_group::DeleteConsumerGroup;
use iggy::consumer_groups::get_consumer_group::GetConsumerGroup;
use iggy::consumer_groups::get_consumer_groups::GetConsumerGroups;
use iggy::consumer_groups::join_consumer_group::JoinConsumerGroup;
use iggy::consumer_groups::leave_consumer_group::LeaveConsumerGroup;
use iggy::consumer_offsets::get_consumer_offset::GetConsumerOffset;
use iggy::consumer_offsets::store_consumer_offset::StoreConsumerOffset;
use iggy::error::IggyError;
use iggy::messages::poll_messages::PollMessages;
use iggy::messages::send_messages::SendMessages;
use iggy::partitions::create_partitions::CreatePartitions;
use iggy::partitions::delete_partitions::DeletePartitions;
use iggy::personal_access_tokens::create_personal_access_token::CreatePersonalAccessToken;
use iggy::personal_access_tokens::delete_personal_access_token::DeletePersonalAccessToken;
use iggy::personal_access_tokens::login_with_personal_access_token::LoginWithPersonalAccessToken;
use iggy::streams::create_stream::CreateStream;
use iggy::streams::delete_stream::DeleteStream;
use iggy::streams::get_stream::GetStream;
use iggy::streams::purge_stream::PurgeStream;
use iggy::streams::update_stream::UpdateStream;
use iggy::system::get_client::GetClient;
use iggy::topics::create_topic::CreateTopic;
use iggy::topics::delete_topic::DeleteTopic;
use iggy::topics::get_topic::GetTopic;
use iggy::topics::get_topics::GetTopics;
use iggy::topics::purge_topic::PurgeTopic;
use iggy::topics::update_topic::UpdateTopic;
use iggy::users::change_password::ChangePassword;
use iggy::users::create_user::CreateUser;
use iggy::users::delete_user::DeleteUser;
use iggy::users::get_user::GetUser;
use iggy::users::login_user::LoginUser;
use iggy::users::update_permissions::UpdatePermissions;
use iggy::users::update_user::UpdateUser;

use super::messages::PollingArgs;

impl IggyShard {
    pub async fn handle_command(
        &self,
        client_id: u32,
        command: Command,
    ) -> Result<ShardResponse, IggyError> {
        //debug!("Handling command '{command}', session: {session}...");
        match command {
            Command::Ping(_) => Ok(ShardResponse::BinaryResponse(Bytes::new())),
            Command::GetStats(_) => {
                let stats = self.get_stats(client_id).await?;
                let bytes = mapper::map_stats(&stats);
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            Command::GetMe(_) => {
                let client = self.get_client(client_id).await?;
                let bytes = mapper::map_client(&client).await;
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            Command::GetClient(command) => {
                let GetClient { client_id } = command;
                let client = self.get_client(client_id).await?;
                let bytes = mapper::map_client(&client).await;
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            Command::GetClients(_) => {
                let clients = self.get_clients(client_id).await?;
                let bytes = mapper::map_clients(&clients).await;
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            Command::GetUser(command) => {
                let GetUser { user_id } = command;
                let user = self.find_user(client_id, &user_id).await?;
                let bytes = mapper::map_user(&user);
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            Command::GetUsers(_) => {
                let users = self.get_users(client_id).await?;
                let bytes = mapper::map_users(&users);
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            Command::CreateUser(command) => {
                let CreateUser {
                    username,
                    password,
                    status,
                    permissions,
                } = command;
                self.create_user(client_id, username, password, status, permissions)
                    .await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            Command::DeleteUser(command) => {
                let DeleteUser { user_id } = command;
                self.delete_user(client_id, &user_id).await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            Command::UpdateUser(command) => {
                let UpdateUser {
                    user_id,
                    username,
                    status,
                } = command;
                self.update_user(client_id, &user_id, username, status)
                    .await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            Command::UpdatePermissions(command) => {
                let UpdatePermissions {
                    user_id,
                    permissions,
                } = command;
                self.update_permissions(client_id, &user_id, permissions)
                    .await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            Command::ChangePassword(command) => {
                let ChangePassword {
                    user_id,
                    current_password,
                    new_password,
                } = command;
                self.change_password(client_id, &user_id, current_password, new_password)
                    .await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            Command::LoginUser(command) => {
                let LoginUser { username, password } = command;
                let user = self.login_user(username, password, client_id).await?;
                let bytes = mapper::map_identity_info(user.id);
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            Command::LogoutUser(_) => {
                self.logout_user(client_id).await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            Command::GetPersonalAccessTokens(_) => {
                let personal_access_tokens = self.get_personal_access_tokens(client_id).await?;
                let bytes = mapper::map_personal_access_tokens(&personal_access_tokens);
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            Command::CreatePersonalAccessToken(command) => {
                let CreatePersonalAccessToken { name, expiry } = command;
                let token = self
                    .create_personal_access_token(client_id, name, expiry)
                    .await?;
                let bytes = mapper::map_raw_pat(&token);
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            Command::DeletePersonalAccessToken(command) => {
                let DeletePersonalAccessToken { name } = command;
                self.delete_personal_access_token(client_id, name).await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            Command::LoginWithPersonalAccessToken(command) => {
                let LoginWithPersonalAccessToken { token } = command;
                let user = self
                    .login_with_personal_access_token(client_id, token)
                    .await?;
                let bytes = mapper::map_identity_info(user.id);
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            Command::SendMessages(command) => {
                let SendMessages {
                    stream_id,
                    topic_id,
                    partitioning,
                    messages,
                } = command;
                self.append_messages(client_id, stream_id, topic_id, partitioning, messages)
                    .await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            Command::PollMessages(command) => {
                let PollMessages {
                    stream_id,
                    topic_id,
                    partition_id,
                    strategy,
                    consumer,
                    count,
                    auto_commit,
                } = command;
                let consumer = PollingConsumer::from_consumer(consumer, client_id, partition_id);
                let messages = self
                    .poll_messages(
                        client_id,
                        consumer,
                        &stream_id,
                        &topic_id,
                        PollingArgs::new(strategy, count, auto_commit),
                    )
                    .await?;
                let messages = mapper::map_polled_messages(&messages);
                Ok(ShardResponse::BinaryResponse(messages))
            }
            Command::GetConsumerOffset(command) => {
                let GetConsumerOffset {
                    stream_id,
                    topic_id,
                    partition_id,
                    consumer,
                } = command;
                let consumer = PollingConsumer::from_consumer(consumer, client_id, partition_id);
                let offset = self
                    .get_consumer_offset(client_id, consumer, &stream_id, &topic_id)
                    .await?;
                let bytes = mapper::map_consumer_offset(&offset);
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            Command::StoreConsumerOffset(command) => {
                let StoreConsumerOffset {
                    stream_id,
                    topic_id,
                    partition_id,
                    consumer,
                    offset,
                } = command;
                let consumer = PollingConsumer::from_consumer(consumer, client_id, partition_id);
                self.store_consumer_offset(client_id, consumer, &stream_id, &topic_id, offset)
                    .await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            Command::GetStream(command) => {
                let GetStream { stream_id } = command;
                let stream = self.find_stream(client_id, &stream_id)?;
                let bytes = mapper::map_stream(stream).await;
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            Command::GetStreams(_) => {
                let _ = self.ensure_authenticated(client_id)?;
                let streams = self.get_streams();
                let bytes = mapper::map_streams(&streams).await;
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            Command::CreateStream(command) => {
                let CreateStream { stream_id, name } = command;
                self.create_stream(client_id, stream_id, name).await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            Command::DeleteStream(command) => {
                let DeleteStream { stream_id } = command;
                self.delete_stream(client_id, &stream_id).await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            Command::UpdateStream(command) => {
                let UpdateStream { stream_id, name } = command;
                self.update_stream(client_id, &stream_id, name).await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            Command::PurgeStream(command) => {
                let PurgeStream { stream_id } = command;
                self.purge_stream(client_id, &stream_id).await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            Command::GetTopic(command) => {
                let GetTopic {
                    stream_id,
                    topic_id,
                } = command;
                let topic = self.find_topic(client_id, &stream_id, &topic_id)?;
                let bytes = mapper::map_topic(&topic).await;
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            Command::GetTopics(command) => {
                let GetTopics { stream_id } = command;
                let topics = self.find_topics(client_id, &stream_id)?;
                let bytes = mapper::map_topics(&topics).await;
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            Command::CreateTopic(command) => {
                let CreateTopic {
                    stream_id,
                    topic_id,
                    name,
                    partitions_count,
                    message_expiry,
                    compression_algorithm,
                    max_topic_size,
                    replication_factor,
                } = command;
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
                )
                .await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            Command::DeleteTopic(command) => {
                let DeleteTopic {
                    stream_id,
                    topic_id,
                } = command;
                self.delete_topic(client_id, &stream_id, &topic_id).await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            Command::UpdateTopic(command) => {
                let UpdateTopic {
                    stream_id,
                    topic_id,
                    name,
                    message_expiry,
                    compression_algorithm,
                    max_topic_size,
                    replication_factor,
                } = command;
                self.update_topic(
                    client_id,
                    &stream_id,
                    &topic_id,
                    name,
                    message_expiry,
                    compression_algorithm,
                    max_topic_size,
                    replication_factor,
                )
                .await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            Command::PurgeTopic(command) => {
                let PurgeTopic {
                    stream_id,
                    topic_id,
                } = command;
                self.purge_topic(client_id, &stream_id, &topic_id).await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            Command::CreatePartitions(command) => {
                let CreatePartitions {
                    stream_id,
                    topic_id,
                    partitions_count: count,
                } = command;
                self.create_partitions(client_id, &stream_id, &topic_id, count)
                    .await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            Command::DeletePartitions(command) => {
                let DeletePartitions {
                    stream_id,
                    topic_id,
                    partitions_count: count,
                } = command;
                self.delete_partitions(client_id, &stream_id, &topic_id, count)
                    .await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            Command::GetConsumerGroup(command) => {
                let GetConsumerGroup {
                    stream_id,
                    topic_id,
                    group_id,
                } = command;
                let consumer_group =
                    self.get_consumer_group(client_id, &stream_id, &topic_id, &group_id)?;
                let bytes = mapper::map_consumer_group(&consumer_group).await;
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            Command::GetConsumerGroups(command) => {
                let GetConsumerGroups {
                    stream_id,
                    topic_id,
                } = command;
                let consumer_groups = self.get_consumer_groups(client_id, &stream_id, &topic_id)?;
                let bytes = mapper::map_consumer_groups(consumer_groups).await;
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            Command::CreateConsumerGroup(command) => {
                let CreateConsumerGroup {
                    stream_id,
                    topic_id,
                    group_id,
                    name,
                } = command;
                self.create_consumer_group(client_id, &stream_id, &topic_id, group_id, name)
                    .await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            Command::DeleteConsumerGroup(command) => {
                let DeleteConsumerGroup {
                    stream_id,
                    topic_id,
                    group_id,
                } = command;
                self.delete_consumer_group(client_id, &stream_id, &topic_id, &group_id)
                    .await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            Command::JoinConsumerGroup(command) => {
                let JoinConsumerGroup {
                    stream_id,
                    topic_id,
                    group_id,
                } = command;
                self.join_consumer_group(client_id, &stream_id, &topic_id, &group_id)
                    .await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            Command::LeaveConsumerGroup(command) => {
                let LeaveConsumerGroup {
                    stream_id,
                    topic_id,
                    group_id,
                } = command;
                self.leave_consumer_group(client_id, &stream_id, &topic_id, &group_id)
                    .await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
        }
    }
}
