use crate::consumer::Consumer;
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::messages::poll_messages::PollingStrategy;
use crate::messages::send_messages::{Message, Partitioning};
use crate::models::client_info::{ClientInfo, ClientInfoDetails};
use crate::models::consumer_group::{ConsumerGroup, ConsumerGroupDetails};
use crate::models::consumer_offset_info::ConsumerOffsetInfo;
use crate::models::identity_info::IdentityInfo;
use crate::models::messages::PolledMessages;
use crate::models::permissions::Permissions;
use crate::models::personal_access_token::{PersonalAccessTokenInfo, RawPersonalAccessToken};
use crate::models::stats::Stats;
use crate::models::stream::{Stream, StreamDetails};
use crate::models::topic::{Topic, TopicDetails};
use crate::models::user_info::{UserInfo, UserInfoDetails};
use crate::models::user_status::UserStatus;
use crate::utils::byte_size::IggyByteSize;
use crate::utils::expiry::IggyExpiry;
use crate::utils::personal_access_token_expiry::PersonalAccessTokenExpiry;
use async_trait::async_trait;
use std::fmt::Debug;

/// The next version of the client which is the main interface to the Iggy server.
/// It consists of multiple modules, each of which is responsible for a specific set of commands.
/// Except the ping, login and get me, all the other methods require authentication.
#[async_trait]
pub trait ClientV2:
    SystemClientV2
    + UserClientV2
    + PersonalAccessTokenClientV2
    + StreamClientV2
    + TopicClientV2
    + PartitionClientV2
    + MessageClientV2
    + ConsumerOffsetClientV2
    + ConsumerGroupClientV2
    + Sync
    + Send
    + Debug
{
    /// Connect to the server. Depending on the selected transport and provided configuration it might also perform authentication, retry logic etc.
    /// If the client is already connected, it will do nothing.
    async fn connect(&self) -> Result<(), IggyError>;

    /// Disconnect from the server. If the client is not connected, it will do nothing.
    async fn disconnect(&self) -> Result<(), IggyError>;
}

/// This trait defines the methods to interact with the system module.
#[async_trait]
pub trait SystemClientV2 {
    /// Get the stats of the system such as PID, memory usage, streams count etc.
    ///
    /// Authentication is required, and the permission to read the server info.
    async fn get_stats(&self) -> Result<Stats, IggyError>;
    /// Get the info about the currently connected client (not to be confused with the user).
    ///
    /// Authentication is required.
    async fn get_me(&self) -> Result<ClientInfoDetails, IggyError>;
    /// Get the info about a specific client by unique ID (not to be confused with the user).
    ///
    /// Authentication is required, and the permission to read the server info.
    async fn get_client(&self, client_id: u32) -> Result<ClientInfoDetails, IggyError>;
    /// Get the info about all the currently connected clients (not to be confused with the users).
    ///
    /// Authentication is required, and the permission to read the server info.
    async fn get_clients(&self) -> Result<Vec<ClientInfo>, IggyError>;
    /// Ping the server to check if it's alive.
    async fn ping(&self) -> Result<(), IggyError>;
}

/// This trait defines the methods to interact with the user module.
#[async_trait]
pub trait UserClientV2 {
    /// Get the info about a specific user by unique ID or username.
    ///
    /// Authentication is required, and the permission to read the users, unless the provided user ID is the same as the authenticated user.
    async fn get_user(&self, user_id: &Identifier) -> Result<UserInfoDetails, IggyError>;
    /// Get the info about all the users.
    ///
    /// Authentication is required, and the permission to read the users.
    async fn get_users(&self) -> Result<Vec<UserInfo>, IggyError>;
    /// Create a new user.
    ///
    /// Authentication is required, and the permission to manage the users.
    async fn create_user(
        &self,
        username: &str,
        password: &str,
        status: Option<UserStatus>,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError>;
    /// Delete a user by unique ID or username.
    ///
    /// Authentication is required, and the permission to manage the users.
    async fn delete_user(&self, user_id: &Identifier) -> Result<(), IggyError>;
    /// Update a user by unique ID or username.
    ///
    /// Authentication is required, and the permission to manage the users.
    async fn update_user(
        &self,
        user_id: &Identifier,
        username: Option<&str>,
        status: Option<UserStatus>,
    ) -> Result<(), IggyError>;
    /// Update the permissions of a user by unique ID or username.
    ///
    /// Authentication is required, and the permission to manage the users.
    async fn update_permissions(
        &self,
        user_id: &Identifier,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError>;
    /// Change the password of a user by unique ID or username.
    ///
    /// Authentication is required, and the permission to manage the users, unless the provided user ID is the same as the authenticated user.
    async fn change_password(
        &self,
        user_id: &Identifier,
        current_password: &str,
        new_password: &str,
    ) -> Result<(), IggyError>;
    /// Login a user by username and password.
    async fn login_user(&self, username: &str, password: &str) -> Result<IdentityInfo, IggyError>;
    /// Logout the currently authenticated user.
    async fn logout_user(&self) -> Result<(), IggyError>;
}

/// This trait defines the methods to interact with the personal access token module.
#[async_trait]
pub trait PersonalAccessTokenClientV2 {
    /// Get the info about all the personal access tokens of the currently authenticated user.
    async fn get_personal_access_tokens(&self) -> Result<Vec<PersonalAccessTokenInfo>, IggyError>;
    /// Create a new personal access token for the currently authenticated user.
    async fn create_personal_access_token(
        &self,
        name: &str,
        expiry: PersonalAccessTokenExpiry,
    ) -> Result<RawPersonalAccessToken, IggyError>;
    /// Delete a personal access token of the currently authenticated user by unique token name.
    async fn delete_personal_access_token(&self, name: &str) -> Result<(), IggyError>;
    /// Login the user with the provided personal access token.
    async fn login_with_personal_access_token(
        &self,
        token: &str,
    ) -> Result<IdentityInfo, IggyError>;
}

/// This trait defines the methods to interact with the stream module.
#[async_trait]
pub trait StreamClientV2 {
    /// Get the info about a specific stream by unique ID or name.
    ///
    /// Authentication is required, and the permission to read the streams.
    async fn get_stream(&self, stream_id: &Identifier) -> Result<StreamDetails, IggyError>;
    /// Get the info about all the streams.
    ///
    /// Authentication is required, and the permission to read the streams.
    async fn get_streams(&self) -> Result<Vec<Stream>, IggyError>;
    /// Create a new stream.
    ///
    /// Authentication is required, and the permission to manage the streams.
    async fn create_stream(&self, name: &str, stream_id: Option<u32>) -> Result<(), IggyError>;
    /// Update a stream by unique ID or name.
    ///
    /// Authentication is required, and the permission to manage the streams.
    async fn update_stream(&self, stream_id: &Identifier, name: &str) -> Result<(), IggyError>;
    /// Delete a stream by unique ID or name.
    ///
    /// Authentication is required, and the permission to manage the streams.
    async fn delete_stream(&self, stream_id: &Identifier) -> Result<(), IggyError>;
    /// Purge a stream by unique ID or name.
    ///
    /// Authentication is required, and the permission to manage the streams.
    async fn purge_stream(&self, stream_id: &Identifier) -> Result<(), IggyError>;
}

/// This trait defines the methods to interact with the topic module.
#[async_trait]
pub trait TopicClientV2 {
    /// Get the info about a specific topic by unique ID or name.
    ///
    /// Authentication is required, and the permission to read the topics.
    async fn get_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<TopicDetails, IggyError>;
    /// Get the info about all the topics.
    ///
    /// Authentication is required, and the permission to read the topics.
    async fn get_topics(&self, stream_id: &Identifier) -> Result<Vec<Topic>, IggyError>;
    /// Create a new topic.
    ///
    /// Authentication is required, and the permission to manage the topics.
    #[allow(clippy::too_many_arguments)]
    async fn create_topic(
        &self,
        stream_id: &Identifier,
        name: &str,
        partitions_count: u32,
        replication_factor: Option<u8>,
        topic_id: Option<u32>,
        message_expiry: IggyExpiry,
        max_topic_size: Option<IggyByteSize>,
    ) -> Result<(), IggyError>;
    /// Update a topic by unique ID or name.
    ///
    /// Authentication is required, and the permission to manage the topics.
    async fn update_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: &str,
        replication_factor: Option<u8>,
        message_expiry: IggyExpiry,
        max_topic_size: Option<IggyByteSize>,
    ) -> Result<(), IggyError>;
    /// Delete a topic by unique ID or name.
    ///
    /// Authentication is required, and the permission to manage the topics.
    async fn delete_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError>;
    /// Purge a topic by unique ID or name.
    ///
    /// Authentication is required, and the permission to manage the topics.
    async fn purge_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError>;
}

/// This trait defines the methods to interact with the partition module.
#[async_trait]
pub trait PartitionClientV2 {
    /// Create new N partitions for a topic by unique ID or name.
    ///
    /// For example, given a topic with 3 partitions, if you create 2 partitions, the topic will have 5 partitions (from 1 to 5).
    ///
    /// Authentication is required, and the permission to manage the partitions.
    async fn create_partitions(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<(), IggyError>;
    /// Delete last N partitions for a topic by unique ID or name.
    ///
    /// For example, given a topic with 5 partitions, if you delete 2 partitions, the topic will have 3 partitions left (from 1 to 3).
    ///
    /// Authentication is required, and the permission to manage the partitions.
    async fn delete_partitions(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<(), IggyError>;
}

/// This trait defines the methods to interact with the messaging module.
#[async_trait]
pub trait MessageClientV2 {
    /// Poll given amount of messages using the specified consumer and strategy from the specified stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to poll the messages.
    #[allow(clippy::too_many_arguments)]
    async fn poll_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        consumer: &Consumer,
        strategy: &PollingStrategy,
        count: u32,
        auto_commit: bool,
    ) -> Result<PolledMessages, IggyError>;
    /// Send messages using specified partitioning strategy to the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to send the messages.
    async fn send_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        messages: &mut [Message],
    ) -> Result<(), IggyError>;
}

/// This trait defines the methods to interact with the consumer offset module.
#[async_trait]
pub trait ConsumerOffsetClientV2 {
    /// Store the consumer offset for a specific consumer or consumer group for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to poll the messages.
    async fn store_consumer_offset(
        &self,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        offset: u64,
    ) -> Result<(), IggyError>;
    /// Get the consumer offset for a specific consumer or consumer group for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to poll the messages.
    async fn get_consumer_offset(
        &self,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
    ) -> Result<ConsumerOffsetInfo, IggyError>;
}

/// This trait defines the methods to interact with the consumer group module.
#[async_trait]
pub trait ConsumerGroupClientV2 {
    /// Get the info about a specific consumer group by unique ID or name for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to read the streams or topics.
    async fn get_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<ConsumerGroupDetails, IggyError>;
    /// Get the info about all the consumer groups for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to read the streams or topics.
    async fn get_consumer_groups(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Vec<ConsumerGroup>, IggyError>;
    /// Create a new consumer group for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to manage the streams or topics.
    async fn create_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: u32,
        name: &str,
    ) -> Result<(), IggyError>;
    /// Delete a consumer group by unique ID or name for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to manage the streams or topics.
    async fn delete_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError>;
    /// Join a consumer group by unique ID or name for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to read the streams or topics.
    async fn join_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError>;
    /// Leave a consumer group by unique ID or name for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to read the streams or topics.
    async fn leave_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError>;
}
