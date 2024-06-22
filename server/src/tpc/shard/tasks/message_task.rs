use crate::tpc::shard::shard::IggyShard;
use futures::StreamExt;
use iggy::error::IggyError;
use std::rc::Rc;
use tracing::error;

async fn run_shard_messages_receiver(shard: Rc<IggyShard>) -> Result<(), IggyError> {
    let mut message_receiver = shard.message_receiver.take().expect(
        "Message receiver is missing, 
    this can happend due to `take` being called multiple times on a single shard.",
    );
    loop {
        if let Some(frame) = message_receiver.next().await {
            shard.handle_shard_message(frame.message).await
        }
    }
}

pub async fn spawn_shard_message_task(shard: Rc<IggyShard>) -> Result<(), IggyError> {
    monoio::spawn(async move {
        let result = run_shard_messages_receiver(shard).await;
        if let Err(err) = &result {
            error!("Error running shard: {err}");
        }
        result
    })
    .await
}
