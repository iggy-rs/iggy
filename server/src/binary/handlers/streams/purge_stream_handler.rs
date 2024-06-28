use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::bytes_serializable::BytesSerializable;
use iggy::command::PURGE_STREAM_CODE;
use iggy::error::IggyError;
use iggy::streams::purge_stream::PurgeStream;
use tracing::debug;

pub async fn handle(
    command: &PurgeStream,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read();
    system.purge_stream(session, &command.stream_id).await?;
    system
        .state
        .apply(
            PURGE_STREAM_CODE,
            session.get_user_id(),
            &command.to_bytes(),
            None,
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
