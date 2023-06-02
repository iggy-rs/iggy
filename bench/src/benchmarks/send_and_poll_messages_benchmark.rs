use crate::args::Args;
use crate::benchmarks::{poll_messages_benchmark, send_messages_benchmark};
use crate::client_factory::ClientFactory;
use futures::future::join_all;
use sdk::error::Error;
use std::sync::Arc;
use tokio::time::Instant;
use tracing::info;

pub async fn run(client_factory: Arc<dyn ClientFactory>, args: Arc<Args>) -> Result<(), Error> {
    let start_stream_id = args.get_start_stream_id();
    let mut send_futures = Vec::with_capacity(args.producers as usize);
    for producer_id in 1..=args.producers {
        let args = args.clone();
        let stream_id = match args.parallel_producer_streams {
            true => start_stream_id + producer_id,
            false => start_stream_id + 1,
        };
        let future = send_messages_benchmark::run(
            client_factory.clone(),
            producer_id,
            args.clone(),
            stream_id,
        );
        send_futures.push(future);
    }

    let mut poll_futures = Vec::with_capacity(args.consumers as usize);
    for consumer_id in 1..=args.consumers {
        let args = args.clone();
        let stream_id = match args.parallel_consumer_streams {
            true => start_stream_id + consumer_id,
            false => start_stream_id + 1,
        };
        let future = poll_messages_benchmark::run(
            client_factory.clone(),
            consumer_id,
            args.clone(),
            stream_id,
        );
        poll_futures.push(future);
    }

    info!("Starting to send and poll messages...");
    let start = Instant::now();
    let producers = tokio::spawn(async move {
        join_all(send_futures).await;
    });
    let consumers = tokio::spawn(async move {
        join_all(poll_futures).await;
    });
    join_all(vec![producers, consumers]).await;
    let duration = start.elapsed();
    info!(
        "Finished sending and polling messages in: {} ms",
        duration.as_millis()
    );

    Ok(())
}
