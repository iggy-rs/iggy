use std::io::IoSlice;

use crate::binary::handlers::messages::COMPONENT;
use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::messages::PollingArgs;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::messages::poll_messages::PollMessages;
use iggy::models::batch::IGGY_BATCH_OVERHEAD;
use tracing::debug;

pub async fn handle(
    command: PollMessages,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let result = system
        .poll_messages(
            session,
            &command.consumer,
            &command.stream_id,
            &command.topic_id,
            command.partition_id,
            PollingArgs::new(command.strategy, command.count, command.auto_commit),
        )
        .await
        .with_error_context(|_| format!(
            "{COMPONENT} - failed to poll messages for consumer: {}, stream_id: {}, topic_id: {}, partition_id: {:?}, session: {}",
            command.consumer, command.stream_id, command.topic_id, command.partition_id, session
        ))?;
    let mut slices = Vec::with_capacity(10);

    // Ok response
    slices.push(IoSlice::new(&[0, 0, 0, 0]));

    let batches_len = result
        .batch_slices
        .iter()
        .map(|b| b.range.len())
        .sum::<usize>();
    let payload_len = IGGY_BATCH_OVERHEAD as usize + batches_len;
    let len_bytes = (payload_len as u32).to_le_bytes();
    slices.push(IoSlice::new(&len_bytes));

    let mut data_refs = Vec::with_capacity(10);
    let header = result.header.as_bytes();
    slices.push(IoSlice::new(&header));

    // Borrowchecker ty kurwo.
    // TODO: decouple ranges from the batch bytes,
    // so we can zip those two iterators and take ownership of the `range` component
    for slice in result.batch_slices.iter() {
        data_refs.push(slice.bytes.clone());
    }
    for (idx, slice) in result.batch_slices.into_iter().enumerate() {
        let range = slice.range;
        let data = &data_refs[idx];
        slices.push(IoSlice::new(&data[range]));
    }

    sender.send_vectored_ok_response(&slices).await?;
    Ok(())
}
