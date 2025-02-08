use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use bytes::Bytes;
use iggy::error::IggyError;
use iggy::system::get_snapshot::GetSnapshot;
use tracing::debug;

pub async fn handle(
    command: GetSnapshot,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let snapshot = system
        .get_snapshot(session, command.compression, command.snapshot_types)
        .await?;
    let bytes = Bytes::copy_from_slice(&snapshot.0);
    sender.send_ok_response(&bytes).await?;
    Ok(())
}
