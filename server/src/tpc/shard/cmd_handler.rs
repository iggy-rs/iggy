use crate::binary::mapper;
use crate::command::ServerCommand;
use crate::state::command::EntryCommand;
use crate::state::models::CreatePersonalAccessTokenWithHash;
use crate::state::State;
use crate::streaming::personal_access_tokens::personal_access_token::PersonalAccessToken;
use crate::streaming::polling_consumer::PollingConsumer;
use crate::tpc::shard::shard::IggyShard;
use crate::tpc::shard::shard_frame::ShardResponse;
use bytes::Bytes;
use iggy::consumer_groups::get_consumer_group::GetConsumerGroup;
use iggy::consumer_groups::get_consumer_groups::GetConsumerGroups;
use iggy::consumer_groups::join_consumer_group::JoinConsumerGroup;
use iggy::consumer_groups::leave_consumer_group::LeaveConsumerGroup;
use iggy::consumer_offsets::get_consumer_offset::GetConsumerOffset;
use iggy::consumer_offsets::store_consumer_offset::StoreConsumerOffset;
use iggy::error::IggyError;
use iggy::messages::poll_messages::PollMessages;
use iggy::messages::send_messages::SendMessages;
use iggy::personal_access_tokens::create_personal_access_token::CreatePersonalAccessToken;
use iggy::personal_access_tokens::login_with_personal_access_token::LoginWithPersonalAccessToken;
use iggy::streams::get_stream::GetStream;
use iggy::system::get_client::GetClient;
use iggy::topics::get_topic::GetTopic;
use iggy::topics::get_topics::GetTopics;
use iggy::users::get_user::GetUser;
use iggy::users::login_user::LoginUser;

use super::messages::PollingArgs;
use super::shard_frame::ShardEvent;

impl IggyShard {
    pub async fn handle_command(
        &self,
        client_id: u32,
        command: ServerCommand,
    ) -> Result<ShardResponse, IggyError> {
        //debug!("Handling command '{command}', session: {session}...");
        match command {
            ServerCommand::Ping(_) => Ok(ShardResponse::BinaryResponse(Bytes::new())),
            ServerCommand::GetStats(_) => {
                let stats = self.get_stats(client_id).await?;
                let bytes = mapper::map_stats(stats);
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            ServerCommand::GetMe(_) => {
                let client = self.get_client(client_id).await?;
                let bytes = mapper::map_client(client).await;
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            ServerCommand::GetClient(command) => {
                let GetClient { client_id } = command;
                let client = self.get_client(client_id).await?;
                let bytes = mapper::map_client(client).await;
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            ServerCommand::GetClients(_) => {
                let clients = self.get_clients(client_id).await?;
                let bytes = mapper::map_clients(clients).await;
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            ServerCommand::GetUser(command) => {
                let GetUser { user_id } = command;
                let user = self.find_user(client_id, &user_id).await?;
                let bytes = mapper::map_user(user);
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            ServerCommand::GetUsers(_) => {
                let users = self.get_users(client_id).await?;
                let bytes = mapper::map_users(users);
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            ServerCommand::CreateUser(command) => {
                let user_id = self.ensure_authenticated(client_id)?;
                self.create_user(
                    user_id,
                    command.username.clone(),
                    command.password.clone(),
                    command.status.clone(),
                    command.permissions.clone(),
                )
                .await?;
                self.state
                    .apply(user_id, EntryCommand::CreateUser(command.clone()))
                    .await?;
                //TODO(numinex) - This has to be broadcasted aswell..
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            ServerCommand::DeleteUser(command) => {
                let user_id_numeric = self.ensure_authenticated(client_id)?;
                self.delete_user(user_id_numeric, &command.user_id).await?;
                self.state
                    .apply(user_id_numeric, EntryCommand::DeleteUser(command))
                    .await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            ServerCommand::UpdateUser(command) => {
                let user_id_numeric = self.ensure_authenticated(client_id)?;
                self.update_user(
                    user_id_numeric,
                    &command.user_id.clone(),
                    command.username.clone(),
                    command.status.clone(),
                )
                .await?;
                self.state
                    .apply(user_id_numeric, EntryCommand::UpdateUser(command.clone()))
                    .await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            ServerCommand::UpdatePermissions(command) => {
                let user_id_numeric = self.ensure_authenticated(client_id)?;
                self.update_permissions(
                    user_id_numeric,
                    &command.user_id.clone(),
                    command.permissions.clone(),
                )
                .await?;
                self.state
                    .apply(
                        user_id_numeric,
                        EntryCommand::UpdatePermissions(command.clone()),
                    )
                    .await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            ServerCommand::ChangePassword(command) => {
                let user_id_numeric = self.ensure_authenticated(client_id)?;
                self.change_password(
                    client_id,
                    &command.user_id,
                    command.current_password.clone(),
                    command.new_password.clone(),
                )
                .await?;
                self.state
                    .apply(
                        user_id_numeric,
                        EntryCommand::ChangePassword(command.clone()),
                    )
                    .await?;
                //TODO(numinex) - This has to be broadcasted aswell..
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            ServerCommand::LoginUser(command) => {
                let LoginUser {
                    username, password, ..
                } = command;
                let user = self
                    .login_user(username.clone(), password.clone(), client_id)
                    .await?;
                let bytes = mapper::map_identity_info(user.id);
                let event = ShardEvent::LoginUser(username, password);
                self.broadcast_event_to_all_shards(client_id, event);
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            ServerCommand::LogoutUser(_) => {
                self.logout_user(client_id).await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            ServerCommand::GetPersonalAccessTokens(_) => {
                let personal_access_tokens = self.get_personal_access_tokens(client_id).await?;
                let bytes = mapper::map_personal_access_tokens(personal_access_tokens);
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            ServerCommand::CreatePersonalAccessToken(command) => {
                let user_id = self.ensure_authenticated(client_id)?;
                let token = self
                    .create_personal_access_token(
                        user_id,
                        command.name.clone(),
                        command.expiry.clone(),
                    )
                    .await?;
                let token_hash = PersonalAccessToken::hash_token(&token);
                let bytes = mapper::map_raw_pat(token);
                self.state
                    .apply(
                        user_id,
                        EntryCommand::CreatePersonalAccessToken(
                            CreatePersonalAccessTokenWithHash {
                                command: CreatePersonalAccessToken {
                                    name: command.name,
                                    expiry: command.expiry,
                                },
                                hash: token_hash,
                            },
                        ),
                    )
                    .await?;
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            ServerCommand::DeletePersonalAccessToken(command) => {
                let user_id = self.ensure_authenticated(client_id)?;
                self.delete_personal_access_token(user_id, command.name.clone())
                    .await?;
                self.state
                    .apply(
                        user_id,
                        EntryCommand::DeletePersonalAccessToken(command.clone()),
                    )
                    .await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            ServerCommand::LoginWithPersonalAccessToken(command) => {
                let LoginWithPersonalAccessToken { token } = command;
                let user = self
                    .login_with_personal_access_token(client_id, token)
                    .await?;
                let bytes = mapper::map_identity_info(user.id);
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            ServerCommand::SendMessages(command) => {
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
            ServerCommand::PollMessages(command) => {
                let PollMessages {
                    stream_id,
                    topic_id,
                    partition_id,
                    strategy,
                    consumer,
                    count,
                    auto_commit,
                } = command;
                let consumer = PollingConsumer::from_consumer(&consumer, client_id, &partition_id);
                let partition_id = partition_id.unwrap();
                let messages = self
                    .poll_messages(
                        client_id,
                        partition_id,
                        consumer,
                        &stream_id,
                        &topic_id,
                        PollingArgs::new(strategy, count, auto_commit),
                    )
                    .await?;
                let bytes = mapper::map_polled_messages(messages);
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            ServerCommand::GetConsumerOffset(command) => {
                let GetConsumerOffset {
                    stream_id,
                    topic_id,
                    partition_id,
                    consumer,
                } = command;
                let consumer = PollingConsumer::from_consumer(&consumer, client_id, &partition_id);
                let consumer_offset = self
                    .get_consumer_offset(client_id, consumer, &stream_id, &topic_id)
                    .await?;
                let bytes = mapper::map_consumer_offset(consumer_offset);
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            ServerCommand::StoreConsumerOffset(command) => {
                let StoreConsumerOffset {
                    stream_id,
                    topic_id,
                    partition_id,
                    consumer,
                    offset,
                } = command;
                let consumer = PollingConsumer::from_consumer(&consumer, client_id, &partition_id);
                self.store_consumer_offset(client_id, consumer, &stream_id, &topic_id, offset)
                    .await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            ServerCommand::GetStream(command) => {
                let GetStream { stream_id } = command;
                let stream = self.find_stream(client_id, &stream_id)?;
                let bytes = mapper::map_stream(stream).await;
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            ServerCommand::GetStreams(_) => {
                let _ = self.ensure_authenticated(client_id)?;
                let streams = self.get_streams();
                let bytes = mapper::map_streams(streams).await;
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            ServerCommand::CreateStream(command) => {
                let user_id = self.ensure_authenticated(client_id)?;
                self.create_stream(
                    user_id,
                    command.stream_id.clone(),
                    command.name.clone(),
                    true,
                )
                .await?;
                self.state
                    .apply(user_id, EntryCommand::CreateStream(command.clone()))
                    .await?;
                let event = ShardEvent::CreatedStream(command.stream_id, command.name);
                self.broadcast_event_to_all_shards(client_id, event);
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            ServerCommand::DeleteStream(command) => {
                let user_id = self.ensure_authenticated(client_id)?;
                self.delete_stream(user_id, &command.stream_id).await?;
                self.state
                    .apply(user_id, EntryCommand::DeleteStream(command))
                    .await?;
                // TODO(numinex) - This has to be broadcasted aswell..
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            ServerCommand::UpdateStream(command) => {
                let user_id = self.ensure_authenticated(client_id)?;
                self.update_stream(user_id, &command.stream_id, command.name.clone())
                    .await?;
                self.state
                    .apply(user_id, EntryCommand::UpdateStream(command.clone()))
                    .await?;
                // TODO(numinex) - This has to be broadcasted aswell..
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            ServerCommand::PurgeStream(command) => {
                let user_id = self.ensure_authenticated(client_id)?;
                self.purge_stream(user_id, &command.stream_id).await?;
                self.state
                    .apply(user_id, EntryCommand::PurgeStream(command.clone()))
                    .await?;
                // TODO(numinex) - This has to be broadcasted aswell..
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            ServerCommand::GetTopic(command) => {
                let GetTopic {
                    stream_id,
                    topic_id,
                } = command;
                let topic = self.find_topic(client_id, &stream_id, &topic_id)?;
                let bytes = mapper::map_topic(topic).await;
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            ServerCommand::GetTopics(command) => {
                let GetTopics { stream_id } = command;
                let topics = self.find_topics(client_id, &stream_id)?;
                let bytes = mapper::map_topics(topics).await;
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            ServerCommand::CreateTopic(command) => {
                let user_id = self.ensure_authenticated(client_id)?;
                self.create_topic(
                    user_id,
                    &command.stream_id,
                    command.topic_id,
                    command.name.clone(),
                    command.partitions_count,
                    command.message_expiry,
                    command.compression_algorithm,
                    command.max_topic_size,
                    command.replication_factor,
                    true,
                )
                .await?;
                self.state
                    .apply(client_id, EntryCommand::CreateTopic(command.clone()))
                    .await?;
                let event = ShardEvent::CreatedTopic(
                    command.stream_id,
                    command.topic_id,
                    command.name,
                    command.partitions_count,
                    command.message_expiry,
                    command.compression_algorithm,
                    command.max_topic_size,
                    command.replication_factor,
                );
                self.broadcast_event_to_all_shards(client_id, event);
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            ServerCommand::DeleteTopic(command) => {
                let user_id = self.ensure_authenticated(client_id)?;
                self.delete_topic(user_id, &command.stream_id, &command.topic_id)
                    .await?;
                self.state
                    .apply(user_id, EntryCommand::DeleteTopic(command.clone()))
                    .await?;
                //TODO(numinex) - This has to be broadcasted aswell..
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            ServerCommand::UpdateTopic(command) => {
                let user_id = self.ensure_authenticated(client_id)?;
                self.update_topic(
                    user_id,
                    &command.stream_id,
                    &command.topic_id,
                    command.name.clone(),
                    command.message_expiry,
                    command.compression_algorithm,
                    command.max_topic_size,
                    command.replication_factor,
                )
                .await?;
                self.state
                    .apply(user_id, EntryCommand::UpdateTopic(command.clone()))
                    .await?;
                // TODO(numinex) - This has to be broadcasted aswell..
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            ServerCommand::PurgeTopic(command) => {
                let user_id = self.ensure_authenticated(client_id)?;
                self.purge_topic(user_id, &command.stream_id, &command.topic_id)
                    .await?;
                self.state
                    .apply(user_id, EntryCommand::PurgeTopic(command.clone()))
                    .await?;
                // TODO(numinex) - This has to be broadcasted aswell..
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            ServerCommand::CreatePartitions(command) => {
                let user_id = self.ensure_authenticated(client_id)?;
                self.create_partitions(
                    user_id,
                    &command.stream_id.clone(),
                    &command.topic_id.clone(),
                    command.partitions_count,
                    true,
                )
                .await?;
                self.state
                    .apply(user_id, EntryCommand::CreatePartitions(command.clone()))
                    .await?;
                let event = ShardEvent::CreatedPartitions(
                    command.stream_id,
                    command.topic_id,
                    command.partitions_count,
                );
                self.broadcast_event_to_all_shards(client_id, event);
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            ServerCommand::DeletePartitions(command) => {
                let user_id = self.ensure_authenticated(client_id)?;
                self.delete_partitions(
                    user_id,
                    &command.stream_id,
                    &command.topic_id,
                    command.partitions_count,
                )
                .await?;
                self.state
                    .apply(user_id, EntryCommand::DeletePartitions(command.clone()))
                    .await?;
                // TODO(numinex): We have to broadcast this event aswell...
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            ServerCommand::GetConsumerGroup(command) => {
                let GetConsumerGroup {
                    stream_id,
                    topic_id,
                    group_id,
                } = command;
                let consumer_group =
                    self.get_consumer_group(client_id, &stream_id, &topic_id, &group_id)?;
                let bytes = mapper::map_consumer_group(consumer_group).await;
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            ServerCommand::GetConsumerGroups(command) => {
                let GetConsumerGroups {
                    stream_id,
                    topic_id,
                } = command;
                let consumer_groups = self.get_consumer_groups(client_id, &stream_id, &topic_id)?;
                let bytes = mapper::map_consumer_groups(consumer_groups).await;
                Ok(ShardResponse::BinaryResponse(bytes))
            }
            ServerCommand::CreateConsumerGroup(command) => {
                let user_id = self.ensure_authenticated(client_id)?;
                self.create_consumer_group(
                    user_id,
                    &command.stream_id,
                    &command.topic_id,
                    command.group_id,
                    command.name.clone(),
                )
                .await?;
                self.state
                    .apply(user_id, EntryCommand::CreateConsumerGroup(command))
                    .await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            ServerCommand::DeleteConsumerGroup(command) => {
                let user_id = self.ensure_authenticated(client_id)?;
                self.delete_consumer_group(
                    user_id,
                    &command.stream_id,
                    &command.topic_id,
                    &command.group_id,
                )
                .await?;
                self.state
                    .apply(user_id, EntryCommand::DeleteConsumerGroup(command))
                    .await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            ServerCommand::JoinConsumerGroup(command) => {
                let JoinConsumerGroup {
                    stream_id,
                    topic_id,
                    group_id,
                } = command;
                self.join_consumer_group(client_id, &stream_id, &topic_id, &group_id)
                    .await?;
                Ok(ShardResponse::BinaryResponse(Bytes::new()))
            }
            ServerCommand::LeaveConsumerGroup(command) => {
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
