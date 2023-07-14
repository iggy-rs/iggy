use crate::args::Args;
use crate::benchmark::{display_results, BenchmarkKind, Transport};
use crate::benchmark_result::BenchmarkResult;
use crate::benchmarks::send_and_poll_messages_benchmark;
use crate::client_factory::ClientFactory;
use crate::http::HttpClientFactory;
use crate::quic::QuicClientFactory;
use crate::tcp::TcpClientFactory;
use crate::{benchmark, initializer};
use futures::future::join_all;
use iggy::error::Error;
use std::sync::Arc;
use tracing::info;

pub async fn run(args: Args) -> Result<(), Error> {
    info!("Starting the benchmarks...");
    let args = Arc::new(args);
    if args.http {
        let client_factory = Arc::new(HttpClientFactory {});
        start(args.clone(), Transport::Http, client_factory).await?;
    }
    if args.quic {
        let client_factory = Arc::new(QuicClientFactory {});
        start(args.clone(), Transport::Quic, client_factory).await?;
    }
    if args.tcp {
        let client_factory = Arc::new(TcpClientFactory {});
        start(args.clone(), Transport::Tcp, client_factory).await?;
    }
    info!("Finished the benchmarks.");
    Ok(())
}

async fn start(
    args: Arc<Args>,
    transport: Transport,
    client_factory: Arc<dyn ClientFactory>,
) -> Result<(), Error> {
    if args.test_send_messages {
        initializer::init_streams(client_factory.clone(), args.clone()).await?;
        execute(
            args.clone(),
            BenchmarkKind::SendMessages,
            transport,
            client_factory.clone(),
        )
        .await;
    }
    if args.test_poll_messages {
        execute(
            args.clone(),
            BenchmarkKind::PollMessages,
            transport,
            client_factory.clone(),
        )
        .await;
    }
    if args.test_send_and_poll_messages {
        initializer::init_streams(client_factory.clone(), args.clone()).await?;
        send_and_poll_messages_benchmark::run(client_factory.clone(), args.clone()).await?;
    }

    Ok(())
}

async fn execute(
    args: Arc<Args>,
    kind: BenchmarkKind,
    transport: Transport,
    client_factory: Arc<dyn ClientFactory>,
) {
    let total_messages = (args.messages_per_batch * args.message_batches * args.streams) as u64;
    info!(
        "Starting the {} benchmark for: {}, total amount of messages: {}...",
        transport, kind, total_messages
    );

    let results = benchmark::start(args.clone(), client_factory, kind).await;
    let results = join_all(results).await;
    let results = results
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<Vec<BenchmarkResult>>();

    display_results(results, kind, total_messages);
}
