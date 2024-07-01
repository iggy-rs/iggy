use crate::binary::sender::Sender;
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::IggyError;
use iggy::partitions::delete_partitions::DeletePartitions;
use tracing::debug;

pub async fn handle(
    command: DeletePartitions,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let mut system = system.write();
    system
        .delete_partitions(
            session,
            &command.stream_id,
            &command.topic_id,
            command.partitions_count,
        )
        .await?;
    system
        .state
        .apply(
            session.get_user_id(),
            EntryCommand::DeletePartitions(command),
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
