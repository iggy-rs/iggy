use crate::handlers::STATUS_OK;
use anyhow::Result;
use shared::error::Error;
use shared::topics::get_topics::GetTopics;
use streaming::system::System;

pub async fn handle(
    command: GetTopics,
    send: &mut quinn::SendStream,
    system: &mut System,
) -> Result<(), Error> {
    let topics = system
        .get_stream(command.stream_id)?
        .get_topics()
        .iter()
        .flat_map(|topic| {
            [
                &topic.id.to_le_bytes(),
                &(topic.get_partitions().len() as u32).to_le_bytes(),
                &(topic.name.len() as u32).to_le_bytes(),
                topic.name.as_bytes(),
            ]
            .concat()
        })
        .collect::<Vec<u8>>();

    send.write_all([STATUS_OK, topics.as_slice()].concat().as_slice())
        .await?;
    Ok(())
}
