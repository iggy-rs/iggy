use crate::binary::handlers::system::COMPONENT;
use crate::binary::mapper;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::system::get_stats::GetStats;
use tracing::debug;

pub async fn handle(
    command: GetStats,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let stats = system.get_stats().await.with_error_context(|error| {
        format!("{COMPONENT} (error: {error}) - failed to get stats, session: {session}")
    })?;
    let bytes = mapper::map_stats(&stats);
    sender.send_ok_response(&bytes).await?;
    Ok(())
}
