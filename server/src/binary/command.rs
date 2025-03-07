use crate::binary::sender::SenderKind;
use crate::binary::COMPONENT;
use crate::define_server_command_enum;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use bytes::{BufMut, Bytes, BytesMut};
use enum_dispatch::enum_dispatch;
use error_set::ErrContext;
use iggy::command::*;
use iggy::consumer_groups::create_consumer_group::CreateConsumerGroup;
use iggy::consumer_groups::delete_consumer_group::DeleteConsumerGroup;
use iggy::consumer_groups::get_consumer_group::GetConsumerGroup;
use iggy::consumer_groups::get_consumer_groups::GetConsumerGroups;
use iggy::consumer_groups::join_consumer_group::JoinConsumerGroup;
use iggy::consumer_groups::leave_consumer_group::LeaveConsumerGroup;
use iggy::consumer_offsets::delete_consumer_offset::DeleteConsumerOffset;
use iggy::consumer_offsets::get_consumer_offset::GetConsumerOffset;
use iggy::consumer_offsets::store_consumer_offset::StoreConsumerOffset;
use iggy::error::IggyError;
use iggy::messages::flush_unsaved_buffer::FlushUnsavedBuffer;
use iggy::messages::poll_messages::PollMessages;
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
use iggy::streams::purge_stream::PurgeStream;
use iggy::streams::update_stream::UpdateStream;
use iggy::system::get_client::GetClient;
use iggy::system::get_clients::GetClients;
use iggy::system::get_me::GetMe;
use iggy::system::get_snapshot::GetSnapshot;
use iggy::system::get_stats::GetStats;
use iggy::system::ping::Ping;
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
use iggy::users::get_users::GetUsers;
use iggy::users::login_user::LoginUser;
use iggy::users::logout_user::LogoutUser;
use iggy::users::update_permissions::UpdatePermissions;
use iggy::users::update_user::UpdateUser;
use iggy::{bytes_serializable::BytesSerializable, messages::send_messages::SendMessages};
use strum::EnumString;
use tracing::{debug, error};

define_server_command_enum! {
    Ping(Ping), PING_CODE, PING, false;
    GetStats(GetStats), GET_STATS_CODE, GET_STATS, false;
    GetMe(GetMe), GET_ME_CODE, GET_ME, false;
    GetClient(GetClient), GET_CLIENT_CODE, GET_CLIENT, true;
    GetClients(GetClients), GET_CLIENTS_CODE, GET_CLIENTS, false;
    GetSnapshot(GetSnapshot), GET_SNAPSHOT_FILE_CODE, GET_SNAPSHOT_FILE, false;
    PollMessages(PollMessages), POLL_MESSAGES_CODE, POLL_MESSAGES, true;
    FlushUnsavedBuffer(FlushUnsavedBuffer), FLUSH_UNSAVED_BUFFER_CODE, FLUSH_UNSAVED_BUFFER, true;
    GetUser(GetUser), GET_USER_CODE, GET_USER, true;
    GetUsers(GetUsers), GET_USERS_CODE, GET_USERS, false;
    CreateUser(CreateUser), CREATE_USER_CODE, CREATE_USER, true;
    DeleteUser(DeleteUser), DELETE_USER_CODE, DELETE_USER, true;
    UpdateUser(UpdateUser), UPDATE_USER_CODE, UPDATE_USER, true;
    UpdatePermissions(UpdatePermissions), UPDATE_PERMISSIONS_CODE, UPDATE_PERMISSIONS, true;
    ChangePassword(ChangePassword), CHANGE_PASSWORD_CODE, CHANGE_PASSWORD, true;
    LoginUser(LoginUser), LOGIN_USER_CODE, LOGIN_USER, true;
    LogoutUser(LogoutUser), LOGOUT_USER_CODE, LOGOUT_USER, false;
    GetPersonalAccessTokens(GetPersonalAccessTokens), GET_PERSONAL_ACCESS_TOKENS_CODE, GET_PERSONAL_ACCESS_TOKENS, false;
    CreatePersonalAccessToken(CreatePersonalAccessToken), CREATE_PERSONAL_ACCESS_TOKEN_CODE, CREATE_PERSONAL_ACCESS_TOKEN, true;
    DeletePersonalAccessToken(DeletePersonalAccessToken), DELETE_PERSONAL_ACCESS_TOKEN_CODE, DELETE_PERSONAL_ACCESS_TOKEN, false;
    LoginWithPersonalAccessToken(LoginWithPersonalAccessToken), LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE, LOGIN_WITH_PERSONAL_ACCESS_TOKEN, true;
    SendMessages(SendMessages), SEND_MESSAGES_CODE, SEND_MESSAGES, false;
    GetConsumerOffset(GetConsumerOffset), GET_CONSUMER_OFFSET_CODE, GET_CONSUMER_OFFSET, true;
    StoreConsumerOffset(StoreConsumerOffset), STORE_CONSUMER_OFFSET_CODE, STORE_CONSUMER_OFFSET, true;
    DeleteConsumerOffset(DeleteConsumerOffset), DELETE_CONSUMER_OFFSET_CODE, DELETE_CONSUMER_OFFSET, true;
    GetStream(GetStream), GET_STREAM_CODE, GET_STREAM, true;
    GetStreams(GetStreams), GET_STREAMS_CODE, GET_STREAMS, false;
    CreateStream(CreateStream), CREATE_STREAM_CODE, CREATE_STREAM, true;
    DeleteStream(DeleteStream), DELETE_STREAM_CODE, DELETE_STREAM, true;
    UpdateStream(UpdateStream), UPDATE_STREAM_CODE, UPDATE_STREAM, true;
    PurgeStream(PurgeStream), PURGE_STREAM_CODE, PURGE_STREAM, true;
    GetTopic(GetTopic), GET_TOPIC_CODE, GET_TOPIC, true;
    GetTopics(GetTopics), GET_TOPICS_CODE, GET_TOPICS, false;
    CreateTopic(CreateTopic), CREATE_TOPIC_CODE, CREATE_TOPIC, true;
    DeleteTopic(DeleteTopic), DELETE_TOPIC_CODE, DELETE_TOPIC, true;
    UpdateTopic(UpdateTopic), UPDATE_TOPIC_CODE, UPDATE_TOPIC, true;
    PurgeTopic(PurgeTopic), PURGE_TOPIC_CODE, PURGE_TOPIC, true;
    CreatePartitions(CreatePartitions), CREATE_PARTITIONS_CODE, CREATE_PARTITIONS, true;
    DeletePartitions(DeletePartitions), DELETE_PARTITIONS_CODE, DELETE_PARTITIONS, true;
    GetConsumerGroup(GetConsumerGroup), GET_CONSUMER_GROUP_CODE, GET_CONSUMER_GROUP, true;
    GetConsumerGroups(GetConsumerGroups), GET_CONSUMER_GROUPS_CODE, GET_CONSUMER_GROUPS, false;
    CreateConsumerGroup(CreateConsumerGroup), CREATE_CONSUMER_GROUP_CODE, CREATE_CONSUMER_GROUP, true;
    DeleteConsumerGroup(DeleteConsumerGroup), DELETE_CONSUMER_GROUP_CODE, DELETE_CONSUMER_GROUP, true;
    JoinConsumerGroup(JoinConsumerGroup), JOIN_CONSUMER_GROUP_CODE, JOIN_CONSUMER_GROUP, true;
    LeaveConsumerGroup(LeaveConsumerGroup), LEAVE_CONSUMER_GROUP_CODE, LEAVE_CONSUMER_GROUP, true;
}

#[enum_dispatch]
pub trait ServerCommandHandler {
    /// Return the command code
    fn code(&self) -> u32;

    /// Handle the command execution
    #[allow(async_fn_in_trait)]
    async fn handle(
        self,
        sender: &mut SenderKind,
        length: u32,
        session: &Session,
        system: &SharedSystem,
    ) -> Result<(), IggyError>;
}

pub trait BinaryServerCommand {
    /// Parse command from sender
    #[allow(async_fn_in_trait)]
    async fn from_sender(
        sender: &mut SenderKind,
        code: u32,
        length: u32,
    ) -> Result<Self, IggyError>
    where
        Self: Sized;
}

fn as_bytes<T: Command>(command: &T) -> Bytes {
    let payload = command.to_bytes();
    let mut bytes = BytesMut::with_capacity(4 + payload.len());
    bytes.put_u32_le(command.code());
    bytes.put_slice(&payload);
    bytes.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes_and_deserialized_from_bytes() {
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::Ping(Ping::default()),
            PING_CODE,
            &Ping::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::GetStats(GetStats::default()),
            GET_STATS_CODE,
            &GetStats::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::GetMe(GetMe::default()),
            GET_ME_CODE,
            &GetMe::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::GetClient(GetClient::default()),
            GET_CLIENT_CODE,
            &GetClient::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::GetClients(GetClients::default()),
            GET_CLIENTS_CODE,
            &GetClients::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::GetUser(GetUser::default()),
            GET_USER_CODE,
            &GetUser::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::GetUsers(GetUsers::default()),
            GET_USERS_CODE,
            &GetUsers::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::CreateUser(CreateUser::default()),
            CREATE_USER_CODE,
            &CreateUser::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::DeleteUser(DeleteUser::default()),
            DELETE_USER_CODE,
            &DeleteUser::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::UpdateUser(UpdateUser::default()),
            UPDATE_USER_CODE,
            &UpdateUser::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::UpdatePermissions(UpdatePermissions::default()),
            UPDATE_PERMISSIONS_CODE,
            &UpdatePermissions::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::ChangePassword(ChangePassword::default()),
            CHANGE_PASSWORD_CODE,
            &ChangePassword::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::LoginUser(LoginUser::default()),
            LOGIN_USER_CODE,
            &LoginUser::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::LogoutUser(LogoutUser::default()),
            LOGOUT_USER_CODE,
            &LogoutUser::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::GetPersonalAccessTokens(GetPersonalAccessTokens::default()),
            GET_PERSONAL_ACCESS_TOKENS_CODE,
            &GetPersonalAccessTokens::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::CreatePersonalAccessToken(CreatePersonalAccessToken::default()),
            CREATE_PERSONAL_ACCESS_TOKEN_CODE,
            &CreatePersonalAccessToken::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::DeletePersonalAccessToken(DeletePersonalAccessToken::default()),
            DELETE_PERSONAL_ACCESS_TOKEN_CODE,
            &DeletePersonalAccessToken::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::LoginWithPersonalAccessToken(LoginWithPersonalAccessToken::default()),
            LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE,
            &LoginWithPersonalAccessToken::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::PollMessages(PollMessages::default()),
            POLL_MESSAGES_CODE,
            &PollMessages::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::StoreConsumerOffset(StoreConsumerOffset::default()),
            STORE_CONSUMER_OFFSET_CODE,
            &StoreConsumerOffset::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::GetConsumerOffset(GetConsumerOffset::default()),
            GET_CONSUMER_OFFSET_CODE,
            &GetConsumerOffset::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::GetStream(GetStream::default()),
            GET_STREAM_CODE,
            &GetStream::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::GetStreams(GetStreams::default()),
            GET_STREAMS_CODE,
            &GetStreams::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::CreateStream(CreateStream::default()),
            CREATE_STREAM_CODE,
            &CreateStream::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::DeleteStream(DeleteStream::default()),
            DELETE_STREAM_CODE,
            &DeleteStream::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::UpdateStream(UpdateStream::default()),
            UPDATE_STREAM_CODE,
            &UpdateStream::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::PurgeStream(PurgeStream::default()),
            PURGE_STREAM_CODE,
            &PurgeStream::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::GetTopic(GetTopic::default()),
            GET_TOPIC_CODE,
            &GetTopic::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::GetTopics(GetTopics::default()),
            GET_TOPICS_CODE,
            &GetTopics::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::CreateTopic(CreateTopic::default()),
            CREATE_TOPIC_CODE,
            &CreateTopic::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::DeleteTopic(DeleteTopic::default()),
            DELETE_TOPIC_CODE,
            &DeleteTopic::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::UpdateTopic(UpdateTopic::default()),
            UPDATE_TOPIC_CODE,
            &UpdateTopic::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::PurgeTopic(PurgeTopic::default()),
            PURGE_TOPIC_CODE,
            &PurgeTopic::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::CreatePartitions(CreatePartitions::default()),
            CREATE_PARTITIONS_CODE,
            &CreatePartitions::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::DeletePartitions(DeletePartitions::default()),
            DELETE_PARTITIONS_CODE,
            &DeletePartitions::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::GetConsumerGroup(GetConsumerGroup::default()),
            GET_CONSUMER_GROUP_CODE,
            &GetConsumerGroup::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::GetConsumerGroups(GetConsumerGroups::default()),
            GET_CONSUMER_GROUPS_CODE,
            &GetConsumerGroups::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::CreateConsumerGroup(CreateConsumerGroup::default()),
            CREATE_CONSUMER_GROUP_CODE,
            &CreateConsumerGroup::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::DeleteConsumerGroup(DeleteConsumerGroup::default()),
            DELETE_CONSUMER_GROUP_CODE,
            &DeleteConsumerGroup::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::JoinConsumerGroup(JoinConsumerGroup::default()),
            JOIN_CONSUMER_GROUP_CODE,
            &JoinConsumerGroup::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::LeaveConsumerGroup(LeaveConsumerGroup::default()),
            LEAVE_CONSUMER_GROUP_CODE,
            &LeaveConsumerGroup::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::FlushUnsavedBuffer(FlushUnsavedBuffer::default()),
            FLUSH_UNSAVED_BUFFER_CODE,
            &FlushUnsavedBuffer::default(),
        );
    }

    fn assert_serialized_as_bytes_and_deserialized_from_bytes(
        command: &ServerCommand,
        code: u32,
        payload: &dyn Command,
    ) {
        assert_serialized_as_bytes(command, code, payload);
        assert_deserialized_from_bytes(command, code, payload);
    }

    fn assert_serialized_as_bytes(
        server_command: &ServerCommand,
        code: u32,
        command: &dyn Command,
    ) {
        let payload = command.to_bytes();
        let mut bytes = BytesMut::with_capacity(4 + payload.len());
        bytes.put_u32_le(code);
        bytes.put_slice(&payload);
        assert_eq!(server_command.to_bytes(), bytes);
    }

    fn assert_deserialized_from_bytes(
        command: &ServerCommand,
        command_id: u32,
        payload: &dyn Command,
    ) {
        let payload = payload.to_bytes();
        let mut bytes = BytesMut::with_capacity(payload.len());
        bytes.put_slice(&payload);
        let bytes = Bytes::from(bytes);
        assert_eq!(
            &ServerCommand::from_code_and_payload(command_id, bytes).unwrap(),
            command
        );
    }
}
