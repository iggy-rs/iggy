use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::bytes_serializable::BytesSerializable;
use iggy::command::UPDATE_STREAM_CODE;
use iggy::error::IggyError;
use iggy::streams::update_stream::UpdateStream;
use tracing::debug;

pub async fn handle(
    command: &UpdateStream,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let mut system = system.write();
    system
        .update_stream(session, &command.stream_id, &command.name)
        .await?;
    system
        .metadata
        .apply(
            UPDATE_STREAM_CODE,
            session.get_user_id(),
            &command.as_bytes(),
            None,
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
