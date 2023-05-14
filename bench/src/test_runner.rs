use crate::args::Args;
use crate::poll_messages_test::init_poll_messages;
use crate::send_messages_test::init_send_messages;
use futures::future::join_all;
use sdk::error::Error;
use tracing::info;

pub async fn run_tests(args: Args) -> Result<(), Error> {
    info!("Starting the tests...");
    if args.test_send_messages {
        info!("Starting the send messages test...");
        let send_messages = init_send_messages(&args).await?;
        join_all(send_messages).await;
        info!("Finished the send messages test.");
    }
    if args.test_poll_messages {
        info!("Starting the poll messages test...");
        let poll_messages = init_poll_messages(&args).await?;
        join_all(poll_messages).await;
        info!("Finished the poll messages test.");
    }
    info!("Finished the tests.");
    Ok(())
}
