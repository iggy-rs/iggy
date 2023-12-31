use async_trait::async_trait;
use iggy::client::{
    Client, ConsumerGroupClient, ConsumerOffsetClient, MessageClient, PartitionClient,
    PersonalAccessTokenClient, StreamClient, SystemClient, TopicClient, UserClient,
};
use iggy::consumer_groups::create_consumer_group::CreateConsumerGroup;
use iggy::consumer_groups::delete_consumer_group::DeleteConsumerGroup;
use iggy::consumer_groups::get_consumer_group::GetConsumerGroup;
use iggy::consumer_groups::get_consumer_groups::GetConsumerGroups;
use iggy::consumer_groups::join_consumer_group::JoinConsumerGroup;
use iggy::consumer_groups::leave_consumer_group::LeaveConsumerGroup;
use iggy::consumer_offsets::get_consumer_offset::GetConsumerOffset;
use iggy::consumer_offsets::store_consumer_offset::StoreConsumerOffset;
use iggy::error::Error;
use iggy::messages::poll_messages::PollMessages;
use iggy::messages::send_messages::SendMessages;
use iggy::models::client_info::{ClientInfo, ClientInfoDetails};
use iggy::models::consumer_group::{ConsumerGroup, ConsumerGroupDetails};
use iggy::models::consumer_offset_info::ConsumerOffsetInfo;
use iggy::models::identity_info::IdentityInfo;
use iggy::models::messages::PolledMessages;
use iggy::models::personal_access_token::{PersonalAccessTokenInfo, RawPersonalAccessToken};
use iggy::models::stats::Stats;
use iggy::models::stream::{Stream, StreamDetails};
use iggy::models::topic::{Topic, TopicDetails};
use iggy::models::user_info::{UserInfo, UserInfoDetails};
use iggy::partitions::create_partitions::CreatePartitions;
use iggy::partitions::delete_partitions::DeletePartitions;
use iggy::personal_access_tokens::create_personal_access_token::CreatePersonalAccessToken;
use iggy::personal_access_tokens::delete_personal_access_token::DeletePersonalAccessToken;
use iggy::personal_access_tokens::get_personal_access_tokens::GetPersonalAccessTokens;
use iggy::personal_access_tokens::login_with_personal_access_token::LoginWithPersonalAccessToken;
use iggy::streams::create_stream::CreateStream;
use iggy::streams::delete_stream::DeleteStream;
use iggy::streams::get_stream::GetStream;
use iggy::streams::get_streams::GetStreams;
use iggy::streams::update_stream::UpdateStream;
use iggy::system::get_client::GetClient;
use iggy::system::get_clients::GetClients;
use iggy::system::get_me::GetMe;
use iggy::system::get_stats::GetStats;
use iggy::system::ping::Ping;
use iggy::topics::create_topic::CreateTopic;
use iggy::topics::delete_topic::DeleteTopic;
use iggy::topics::get_topic::GetTopic;
use iggy::topics::get_topics::GetTopics;
use iggy::topics::update_topic::UpdateTopic;
use iggy::users::change_password::ChangePassword;
use iggy::users::create_user::CreateUser;
use iggy::users::delete_user::DeleteUser;
use iggy::users::get_user::GetUser;
use iggy::users::get_users::GetUsers;
use iggy::users::login_user::LoginUser;
use iggy::users::logout_user::LogoutUser;
use iggy::users::update_permissions::UpdatePermissions;
use iggy::users::update_user::UpdateUser;
use mockall::mock;

mock! {
    #[derive(Debug)]
    pub IggyClient {}

    #[async_trait]
    impl Client for IggyClient {
        async fn connect(&self) -> Result<(), Error>;
        async fn disconnect(&self) -> Result<(), Error>;
    }

    #[async_trait]
    impl SystemClient for IggyClient {
        async fn get_stats(&self, command: &GetStats) -> Result<Stats, Error>;
        async fn get_me(&self, command: &GetMe) -> Result<ClientInfoDetails, Error>;
        async fn get_client(&self, command: &GetClient) -> Result<ClientInfoDetails, Error>;
        async fn get_clients(&self, command: &GetClients) -> Result<Vec<ClientInfo>, Error>;
        async fn ping(&self, command: &Ping) -> Result<(), Error>;
    }

    #[async_trait]
    impl UserClient for IggyClient {
        async fn get_user(&self, command: &GetUser) -> Result<UserInfoDetails, Error>;
        async fn get_users(&self, command: &GetUsers) -> Result<Vec<UserInfo>, Error>;
        async fn create_user(&self, command: &CreateUser) -> Result<(), Error>;
        async fn delete_user(&self, command: &DeleteUser) -> Result<(), Error>;
        async fn update_user(&self, command: &UpdateUser) -> Result<(), Error>;
        async fn update_permissions(&self, command: &UpdatePermissions) -> Result<(), Error>;
        async fn change_password(&self, command: &ChangePassword) -> Result<(), Error>;
        async fn login_user(&self, command: &LoginUser) -> Result<IdentityInfo, Error>;
        async fn logout_user(&self, command: &LogoutUser) -> Result<(), Error>;
    }

    #[async_trait]
    impl PersonalAccessTokenClient for IggyClient {
        async fn get_personal_access_tokens(
            &self,
            command: &GetPersonalAccessTokens,
        ) -> Result<Vec<PersonalAccessTokenInfo>, Error>;
        async fn create_personal_access_token(
            &self,
            command: &CreatePersonalAccessToken,
        ) -> Result<RawPersonalAccessToken, Error>;
        async fn delete_personal_access_token(
            &self,
            command: &DeletePersonalAccessToken,
        ) -> Result<(), Error>;
        async fn login_with_personal_access_token(
            &self,
            command: &LoginWithPersonalAccessToken,
        ) -> Result<IdentityInfo, Error>;
    }

    #[async_trait]
    impl StreamClient for IggyClient {
        async fn create_stream(&self, command: &CreateStream) -> Result<(), Error>;
        async fn get_stream(&self, command: &GetStream) -> Result<StreamDetails, Error>;
        async fn get_streams(&self, command: &GetStreams) -> Result<Vec<Stream>, Error>;
        async fn update_stream(&self, command: &UpdateStream) -> Result<(), Error>;
        async fn delete_stream(&self, command: &DeleteStream) -> Result<(), Error>;
    }

    #[async_trait]
    impl TopicClient for IggyClient {
        async fn get_topic(&self, command: &GetTopic) -> Result<TopicDetails, Error>;
        async fn get_topics(&self, command: &GetTopics) -> Result<Vec<Topic>, Error>;
        async fn create_topic(&self, command: &CreateTopic) -> Result<(), Error>;
        async fn update_topic(&self, command: &UpdateTopic) -> Result<(), Error>;
        async fn delete_topic(&self, command: &DeleteTopic) -> Result<(), Error>;
    }

    #[async_trait]
    impl PartitionClient for IggyClient {
        async fn create_partitions(&self, command: &CreatePartitions) -> Result<(), Error>;
        async fn delete_partitions(&self, command: &DeletePartitions) -> Result<(), Error>;
    }

    #[async_trait]
    impl MessageClient for IggyClient {
        async fn poll_messages(&self, command: &PollMessages) -> Result<PolledMessages, Error>;
        async fn send_messages(&self, command: &mut SendMessages) -> Result<(), Error>;
    }

    #[async_trait]
    impl ConsumerOffsetClient for IggyClient {
        async fn store_consumer_offset(&self, command: &StoreConsumerOffset) -> Result<(), Error>;
        async fn get_consumer_offset(
            &self,
            command: &GetConsumerOffset,
        ) -> Result<ConsumerOffsetInfo, Error>;
    }

    #[async_trait]
    impl ConsumerGroupClient for IggyClient {
        async fn get_consumer_group(
            &self,
            command: &GetConsumerGroup,
        ) -> Result<ConsumerGroupDetails, Error>;
        async fn get_consumer_groups(
            &self,
            command: &GetConsumerGroups,
        ) -> Result<Vec<ConsumerGroup>, Error>;
        async fn create_consumer_group(&self, command: &CreateConsumerGroup) -> Result<(), Error>;
        async fn delete_consumer_group(&self, command: &DeleteConsumerGroup) -> Result<(), Error>;
        async fn join_consumer_group(&self, command: &JoinConsumerGroup) -> Result<(), Error>;
        async fn leave_consumer_group(&self, command: &LeaveConsumerGroup) -> Result<(), Error>;
    }

}
