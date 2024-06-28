use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::bytes_serializable::BytesSerializable;
use iggy::command::CREATE_STREAM_CODE;
use iggy::error::IggyError;
use iggy::streams::create_stream::CreateStream;
use tracing::debug;

pub async fn handle(
    command: &CreateStream,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let mut system = system.write();
    system
        .create_stream(session, command.stream_id, &command.name)
        .await?;
    system
        .state
        .apply(
            CREATE_STREAM_CODE,
            session.get_user_id(),
            &command.to_bytes(),
            None,
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
