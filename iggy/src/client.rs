use crate::consumer_groups::create_consumer_group::CreateConsumerGroup;
use crate::consumer_groups::delete_consumer_group::DeleteConsumerGroup;
use crate::consumer_groups::get_consumer_group::GetConsumerGroup;
use crate::consumer_groups::get_consumer_groups::GetConsumerGroups;
use crate::consumer_groups::join_consumer_group::JoinConsumerGroup;
use crate::consumer_groups::leave_consumer_group::LeaveConsumerGroup;
use crate::consumer_offsets::get_consumer_offset::GetConsumerOffset;
use crate::consumer_offsets::store_consumer_offset::StoreConsumerOffset;
use crate::error::Error;
use crate::messages::poll_messages::PollMessages;
use crate::messages::send_messages::SendMessages;
use crate::models::client_info::{ClientInfo, ClientInfoDetails};
use crate::models::consumer_group::{ConsumerGroup, ConsumerGroupDetails};
use crate::models::consumer_offset_info::ConsumerOffsetInfo;
use crate::models::identity_info::IdentityInfo;
use crate::models::messages::PolledMessages;
use crate::models::personal_access_token::{PersonalAccessTokenInfo, RawPersonalAccessToken};
use crate::models::stats::Stats;
use crate::models::stream::{Stream, StreamDetails};
use crate::models::topic::{Topic, TopicDetails};
use crate::models::user_info::{UserInfo, UserInfoDetails};
use crate::partitions::create_partitions::CreatePartitions;
use crate::partitions::delete_partitions::DeletePartitions;
use crate::personal_access_tokens::create_personal_access_token::CreatePersonalAccessToken;
use crate::personal_access_tokens::delete_personal_access_token::DeletePersonalAccessToken;
use crate::personal_access_tokens::get_personal_access_tokens::GetPersonalAccessTokens;
use crate::personal_access_tokens::login_with_personal_access_token::LoginWithPersonalAccessToken;
use crate::streams::create_stream::CreateStream;
use crate::streams::delete_stream::DeleteStream;
use crate::streams::get_stream::GetStream;
use crate::streams::get_streams::GetStreams;
use crate::streams::update_stream::UpdateStream;
use crate::system::get_client::GetClient;
use crate::system::get_clients::GetClients;
use crate::system::get_me::GetMe;
use crate::system::get_stats::GetStats;
use crate::system::ping::Ping;
use crate::topics::create_topic::CreateTopic;
use crate::topics::delete_topic::DeleteTopic;
use crate::topics::get_topic::GetTopic;
use crate::topics::get_topics::GetTopics;
use crate::topics::update_topic::UpdateTopic;
use crate::users::change_password::ChangePassword;
use crate::users::create_user::CreateUser;
use crate::users::delete_user::DeleteUser;
use crate::users::get_user::GetUser;
use crate::users::get_users::GetUsers;
use crate::users::login_user::LoginUser;
use crate::users::logout_user::LogoutUser;
use crate::users::update_permissions::UpdatePermissions;
use crate::users::update_user::UpdateUser;
use async_trait::async_trait;
use std::fmt::Debug;

/// The client is the main interface to the Iggy server.
/// It consists of multiple modules, each of which is responsible for a specific set of commands.
/// Except the ping, login and get me commands, all the other commands require authentication.
#[async_trait]
pub trait Client:
    SystemClient
    + UserClient
    + PersonalAccessTokenClient
    + StreamClient
    + TopicClient
    + PartitionClient
    + MessageClient
    + ConsumerOffsetClient
    + ConsumerGroupClient
    + Sync
    + Send
    + Debug
{
    /// Connect to the server. Depending on the selected transport and provided configuration it might also perform authentication, retry logic etc.
    /// If the client is already connected, it will do nothing.
    async fn connect(&self) -> Result<(), Error>;

    /// Disconnect from the server. If the client is not connected, it will do nothing.
    async fn disconnect(&self) -> Result<(), Error>;
}

/// This trait defines the methods to interact with the system module.
#[async_trait]
pub trait SystemClient {
    /// Get the stats of the system such as PID, memory usage, streams count etc.
    ///
    /// Authentication is required, and the permission to read the server info.
    async fn get_stats(&self, command: &GetStats) -> Result<Stats, Error>;
    /// Get the info about the currently connected client (not to be confused with the user).
    ///
    /// Authentication is required.
    async fn get_me(&self, command: &GetMe) -> Result<ClientInfoDetails, Error>;
    /// Get the info about a specific client by unique ID (not to be confused with the user).
    ///
    /// Authentication is required, and the permission to read the server info.
    async fn get_client(&self, command: &GetClient) -> Result<ClientInfoDetails, Error>;
    /// Get the info about all the currently connected clients (not to be confused with the users).
    ///
    /// Authentication is required, and the permission to read the server info.
    async fn get_clients(&self, command: &GetClients) -> Result<Vec<ClientInfo>, Error>;
    /// Ping the server to check if it's alive.
    async fn ping(&self, command: &Ping) -> Result<(), Error>;
}

/// This trait defines the methods to interact with the user module.
#[async_trait]
pub trait UserClient {
    /// Get the info about a specific user by unique ID or username.
    ///
    /// Authentication is required, and the permission to read the users, unless the provided user ID is the same as the authenticated user.
    async fn get_user(&self, command: &GetUser) -> Result<UserInfoDetails, Error>;
    /// Get the info about all the users.
    ///
    /// Authentication is required, and the permission to read the users.
    async fn get_users(&self, command: &GetUsers) -> Result<Vec<UserInfo>, Error>;
    /// Create a new user.
    ///
    /// Authentication is required, and the permission to manage the users.
    async fn create_user(&self, command: &CreateUser) -> Result<(), Error>;
    /// Delete a user by unique ID or username.
    ///
    /// Authentication is required, and the permission to manage the users.
    async fn delete_user(&self, command: &DeleteUser) -> Result<(), Error>;
    /// Update a user by unique ID or username.
    ///
    /// Authentication is required, and the permission to manage the users.
    async fn update_user(&self, command: &UpdateUser) -> Result<(), Error>;
    /// Update the permissions of a user by unique ID or username.
    ///
    /// Authentication is required, and the permission to manage the users.
    async fn update_permissions(&self, command: &UpdatePermissions) -> Result<(), Error>;
    /// Change the password of a user by unique ID or username.
    ///
    /// Authentication is required, and the permission to manage the users, unless the provided user ID is the same as the authenticated user.
    async fn change_password(&self, command: &ChangePassword) -> Result<(), Error>;
    /// Login a user by username and password.
    async fn login_user(&self, command: &LoginUser) -> Result<IdentityInfo, Error>;
    /// Logout the currently authenticated user.
    async fn logout_user(&self, command: &LogoutUser) -> Result<(), Error>;
}

/// This trait defines the methods to interact with the personal access token module.
#[async_trait]
pub trait PersonalAccessTokenClient {
    /// Get the info about all the personal access tokens of the currently authenticated user.
    async fn get_personal_access_tokens(
        &self,
        command: &GetPersonalAccessTokens,
    ) -> Result<Vec<PersonalAccessTokenInfo>, Error>;
    /// Create a new personal access token for the currently authenticated user.
    async fn create_personal_access_token(
        &self,
        command: &CreatePersonalAccessToken,
    ) -> Result<RawPersonalAccessToken, Error>;
    /// Delete a personal access token of the currently authenticated user by unique token name.
    async fn delete_personal_access_token(
        &self,
        command: &DeletePersonalAccessToken,
    ) -> Result<(), Error>;
    /// Login the user with the provided personal access token.
    async fn login_with_personal_access_token(
        &self,
        command: &LoginWithPersonalAccessToken,
    ) -> Result<IdentityInfo, Error>;
}

/// This trait defines the methods to interact with the stream module.
#[async_trait]
pub trait StreamClient {
    /// Get the info about a specific stream by unique ID or name.
    ///
    /// Authentication is required, and the permission to read the streams.
    async fn get_stream(&self, command: &GetStream) -> Result<StreamDetails, Error>;
    /// Get the info about all the streams.
    ///
    /// Authentication is required, and the permission to read the streams.
    async fn get_streams(&self, command: &GetStreams) -> Result<Vec<Stream>, Error>;
    /// Create a new stream.
    ///
    /// Authentication is required, and the permission to manage the streams.
    async fn create_stream(&self, command: &CreateStream) -> Result<(), Error>;
    /// Update a stream by unique ID or name.
    ///
    /// Authentication is required, and the permission to manage the streams.
    async fn update_stream(&self, command: &UpdateStream) -> Result<(), Error>;
    /// Delete a stream by unique ID or name.
    ///
    /// Authentication is required, and the permission to manage the streams.
    async fn delete_stream(&self, command: &DeleteStream) -> Result<(), Error>;
}

/// This trait defines the methods to interact with the topic module.
#[async_trait]
pub trait TopicClient {
    /// Get the info about a specific topic by unique ID or name.
    ///
    /// Authentication is required, and the permission to read the topics.
    async fn get_topic(&self, command: &GetTopic) -> Result<TopicDetails, Error>;
    /// Get the info about all the topics.
    ///
    /// Authentication is required, and the permission to read the topics.
    async fn get_topics(&self, command: &GetTopics) -> Result<Vec<Topic>, Error>;
    /// Create a new topic.
    ///
    /// Authentication is required, and the permission to manage the topics.
    async fn create_topic(&self, command: &CreateTopic) -> Result<(), Error>;
    /// Update a topic by unique ID or name.
    ///
    /// Authentication is required, and the permission to manage the topics.
    async fn update_topic(&self, command: &UpdateTopic) -> Result<(), Error>;
    /// Delete a topic by unique ID or name.
    ///
    /// Authentication is required, and the permission to manage the topics.
    async fn delete_topic(&self, command: &DeleteTopic) -> Result<(), Error>;
}

/// This trait defines the methods to interact with the partition module.
#[async_trait]
pub trait PartitionClient {
    /// Create new N partitions for a topic by unique ID or name.
    ///
    /// For example, given a topic with 3 partitions, if you create 2 partitions, the topic will have 5 partitions (from 1 to 5).
    ///
    /// Authentication is required, and the permission to manage the partitions.
    async fn create_partitions(&self, command: &CreatePartitions) -> Result<(), Error>;
    /// Delete last N partitions for a topic by unique ID or name.
    ///
    /// For example, given a topic with 5 partitions, if you delete 2 partitions, the topic will have 3 partitions left (from 1 to 3).
    ///
    /// Authentication is required, and the permission to manage the partitions.
    async fn delete_partitions(&self, command: &DeletePartitions) -> Result<(), Error>;
}

/// This trait defines the methods to interact with the messaging module.
#[async_trait]
pub trait MessageClient {
    /// Poll given amount of messages using the specified consumer and strategy from the specified stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to poll the messages.
    async fn poll_messages(&self, command: &PollMessages) -> Result<PolledMessages, Error>;
    /// Send messages using specified partitioning strategy to the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to send the messages.
    async fn send_messages(&self, command: &mut SendMessages) -> Result<(), Error>;
}

/// This trait defines the methods to interact with the consumer offset module.
#[async_trait]
pub trait ConsumerOffsetClient {
    /// Store the consumer offset for a specific consumer or consumer group for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to poll the messages.
    async fn store_consumer_offset(&self, command: &StoreConsumerOffset) -> Result<(), Error>;
    /// Get the consumer offset for a specific consumer or consumer group for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to poll the messages.
    async fn get_consumer_offset(
        &self,
        command: &GetConsumerOffset,
    ) -> Result<ConsumerOffsetInfo, Error>;
}

/// This trait defines the methods to interact with the consumer group module.
#[async_trait]
pub trait ConsumerGroupClient {
    /// Get the info about a specific consumer group by unique ID or name for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to read the streams or topics.
    async fn get_consumer_group(
        &self,
        command: &GetConsumerGroup,
    ) -> Result<ConsumerGroupDetails, Error>;
    /// Get the info about all the consumer groups for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to read the streams or topics.
    async fn get_consumer_groups(
        &self,
        command: &GetConsumerGroups,
    ) -> Result<Vec<ConsumerGroup>, Error>;
    /// Create a new consumer group for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to manage the streams or topics.
    async fn create_consumer_group(&self, command: &CreateConsumerGroup) -> Result<(), Error>;
    /// Delete a consumer group by unique ID or name for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to manage the streams or topics.
    async fn delete_consumer_group(&self, command: &DeleteConsumerGroup) -> Result<(), Error>;
    /// Join a consumer group by unique ID or name for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to read the streams or topics.
    async fn join_consumer_group(&self, command: &JoinConsumerGroup) -> Result<(), Error>;
    /// Leave a consumer group by unique ID or name for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to read the streams or topics.
    async fn leave_consumer_group(&self, command: &LeaveConsumerGroup) -> Result<(), Error>;
}
