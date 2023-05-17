use crate::args::Args;
use crate::poll_messages_test::init_poll_messages;
use crate::send_messages_test::init_send_messages;
use futures::future::join_all;
use sdk::error::Error;
use tracing::info;

pub async fn run_tests(args: Args) -> Result<(), Error> {
    info!("Starting the tests...");
    let total_messages =
        (args.messages_per_batch * args.message_batches * args.clients_count) as u64;
    if args.test_send_messages {
        info!(
            "Starting the send messages test for total amount of messages: {}...",
            total_messages
        );
        let send_messages = init_send_messages(&args).await?;
        join_all(send_messages).await;
        info!(
            "Finished the send messages test for total amount of messages: {}.",
            total_messages
        );
    }
    if args.test_poll_messages {
        info!(
            "Starting the poll messages test for total amount of messages: {}...",
            total_messages
        );
        let poll_messages = init_poll_messages(&args).await?;
        join_all(poll_messages).await;
        info!(
            "Finished the poll messages test for total amount of messages: {}.",
            total_messages
        );
    }
    info!("Finished the tests.");
    Ok(())
}
