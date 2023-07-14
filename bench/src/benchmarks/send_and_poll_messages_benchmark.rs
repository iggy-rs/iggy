use crate::args::Args;
use crate::benchmark::{display_results, BenchmarkKind};
use crate::benchmarks::{poll_messages_benchmark, send_messages_benchmark};
use crate::client_factory::ClientFactory;
use futures::future::join_all;
use iggy::error::Error;
use std::sync::Arc;
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

    let total_messages = (args.messages_per_batch * args.message_batches * args.streams) as u64;
    info!(
        "Starting to send and poll messages, total amount: {}",
        total_messages
    );
    let producers = tokio::spawn(async move { join_all(send_futures).await });
    let consumers = tokio::spawn(async move { join_all(poll_futures).await });
    let results = join_all(vec![producers, consumers]).await;
    info!(
        "Finished sending and polling messages, total amount: {}",
        total_messages
    );

    let results = results.into_iter().flatten().flatten().collect::<Vec<_>>();
    let send_messages_results = results
        .iter()
        .map(|r| r.as_ref().unwrap().clone())
        .filter(|r| r.kind == BenchmarkKind::SendMessages)
        .collect::<Vec<_>>();

    let poll_messages_results = results
        .iter()
        .map(|r| r.as_ref().unwrap().clone())
        .filter(|r| r.kind == BenchmarkKind::PollMessages)
        .collect::<Vec<_>>();

    display_results(
        send_messages_results,
        BenchmarkKind::SendMessages,
        total_messages,
    );
    display_results(
        poll_messages_results,
        BenchmarkKind::PollMessages,
        total_messages,
    );

    Ok(())
}
