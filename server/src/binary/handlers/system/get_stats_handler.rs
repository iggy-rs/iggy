use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use iggy::error::IggyError;
use iggy::system::get_stats::GetStats;
use tracing::debug;

pub async fn handle(
    command: GetStats,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let stats = system.get_stats(session).await?;
    let bytes = mapper::map_stats(&stats);
    sender.send_ok_response(&bytes).await?;
    Ok(())
}
