use crate::binary::handlers::consumer_groups::{
    create_consumer_group_handler, delete_consumer_group_handler, get_consumer_group_handler,
    get_consumer_groups_handler, join_consumer_group_handler, leave_consumer_group_handler,
};
use crate::binary::handlers::consumer_offsets::*;
use crate::binary::handlers::messages::*;
use crate::binary::handlers::partitions::*;
use crate::binary::handlers::personal_access_tokens::{
    create_personal_access_token_handler, delete_personal_access_token_handler,
    get_personal_access_tokens_handler, login_with_personal_access_token_handler,
};
use crate::binary::handlers::streams::*;
use crate::binary::handlers::system::*;
use crate::binary::handlers::topics::*;
use crate::binary::handlers::users::{
    change_password_handler, create_user_handler, delete_user_handler, get_user_handler,
    get_users_handler, login_user_handler, logout_user_handler, update_permissions_handler,
    update_user_handler,
};
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use iggy::command::Command;
use iggy::error::Error;
use tracing::debug;

pub async fn handle(
    command: &Command,
    sender: &mut dyn Sender,
    session: &mut Session,
    system: SharedSystem,
) -> Result<(), Error> {
    let result = try_handle(command, sender, session, &system).await;
    if result.is_ok() {
        debug!("Command was handled successfully, session: {session}.",);
        return Ok(());
    }

    let error = result.err().unwrap();
    debug!("Command was not handled successfully, session: {session}, error: {error}.",);
    sender.send_error_response(error).await?;
    Ok(())
}

async fn try_handle(
    command: &Command,
    sender: &mut dyn Sender,
    session: &mut Session,
    system: &SharedSystem,
) -> Result<(), Error> {
    debug!("Handling command '{command}', session: {session}...");
    match command {
        Command::Ping(command) => ping_handler::handle(command, sender, session).await,
        Command::GetStats(command) => {
            get_stats_handler::handle(command, sender, session, system).await
        }
        Command::GetMe(command) => get_me_handler::handle(command, sender, session, system).await,
        Command::GetClient(command) => {
            get_client_handler::handle(command, sender, session, system).await
        }
        Command::GetClients(command) => {
            get_clients_handler::handle(command, sender, session, system).await
        }
        Command::GetUser(command) => {
            get_user_handler::handle(command, sender, session, system).await
        }
        Command::GetUsers(command) => {
            get_users_handler::handle(command, sender, session, system).await
        }
        Command::CreateUser(command) => {
            create_user_handler::handle(command, sender, session, system).await
        }
        Command::DeleteUser(command) => {
            delete_user_handler::handle(command, sender, session, system).await
        }
        Command::UpdateUser(command) => {
            update_user_handler::handle(command, sender, session, system).await
        }
        Command::UpdatePermissions(command) => {
            update_permissions_handler::handle(command, sender, session, system).await
        }
        Command::ChangePassword(command) => {
            change_password_handler::handle(command, sender, session, system).await
        }
        Command::LoginUser(command) => {
            login_user_handler::handle(command, sender, session, system).await
        }
        Command::LogoutUser(command) => {
            logout_user_handler::handle(command, sender, session, system).await
        }
        Command::GetPersonalAccessTokens(command) => {
            get_personal_access_tokens_handler::handle(command, sender, session, system).await
        }
        Command::CreatePersonalAccessToken(command) => {
            create_personal_access_token_handler::handle(command, sender, session, system).await
        }
        Command::DeletePersonalAccessToken(command) => {
            delete_personal_access_token_handler::handle(command, sender, session, system).await
        }
        Command::LoginWithPersonalAccessToken(command) => {
            login_with_personal_access_token_handler::handle(command, sender, session, system).await
        }
        Command::SendMessages(command) => {
            send_messages_handler::handle(command, sender, session, system).await
        }
        Command::PollMessages(command) => {
            poll_messages_handler::handle(command, sender, session, system).await
        }
        Command::GetConsumerOffset(command) => {
            get_consumer_offset_handler::handle(command, sender, session, system).await
        }
        Command::StoreConsumerOffset(command) => {
            store_consumer_offset_handler::handle(command, sender, session, system).await
        }
        Command::GetStream(command) => {
            get_stream_handler::handle(command, sender, session, system).await
        }
        Command::GetStreams(command) => {
            get_streams_handler::handle(command, sender, session, system).await
        }
        Command::CreateStream(command) => {
            create_stream_handler::handle(command, sender, session, system).await
        }
        Command::DeleteStream(command) => {
            delete_stream_handler::handle(command, sender, session, system).await
        }
        Command::UpdateStream(command) => {
            update_stream_handler::handle(command, sender, session, system).await
        }
        Command::GetTopic(command) => {
            get_topic_handler::handle(command, sender, session, system).await
        }
        Command::GetTopics(command) => {
            get_topics_handler::handle(command, sender, session, system).await
        }
        Command::CreateTopic(command) => {
            create_topic_handler::handle(command, sender, session, system).await
        }
        Command::DeleteTopic(command) => {
            delete_topic_handler::handle(command, sender, session, system).await
        }
        Command::UpdateTopic(command) => {
            update_topic_handler::handle(command, sender, session, system).await
        }
        Command::CreatePartitions(command) => {
            create_partitions_handler::handle(command, sender, session, system).await
        }
        Command::DeletePartitions(command) => {
            delete_partitions_handler::handle(command, sender, session, system).await
        }
        Command::GetConsumerGroup(command) => {
            get_consumer_group_handler::handle(command, sender, session, system).await
        }
        Command::GetConsumerGroups(command) => {
            get_consumer_groups_handler::handle(command, sender, session, system).await
        }
        Command::CreateConsumerGroup(command) => {
            create_consumer_group_handler::handle(command, sender, session, system).await
        }
        Command::DeleteConsumerGroup(command) => {
            delete_consumer_group_handler::handle(command, sender, session, system).await
        }
        Command::JoinConsumerGroup(command) => {
            join_consumer_group_handler::handle(command, sender, session, system).await
        }
        Command::LeaveConsumerGroup(command) => {
            leave_consumer_group_handler::handle(command, sender, session, system).await
        }
    }
}
