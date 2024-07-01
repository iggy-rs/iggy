use crate::binary::sender::Sender;
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::consumer_groups::delete_consumer_group::DeleteConsumerGroup;
use iggy::error::IggyError;
use tracing::debug;

pub async fn handle(
    command: DeleteConsumerGroup,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let mut system = system.write();
    system
        .delete_consumer_group(
            session,
            &command.stream_id,
            &command.topic_id,
            &command.group_id,
        )
        .await?;
    system
        .state
        .apply(
            session.get_user_id(),
            EntryCommand::DeleteConsumerGroup(command),
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
