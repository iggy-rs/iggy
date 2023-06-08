use crate::args::Args;
use crate::benchmark_result::BenchmarkResult;
use crate::benchmarks::*;
use crate::client_factory::ClientFactory;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::Duration;
use tokio::task;
use tokio::task::JoinHandle;
use tracing::{error, info};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BenchmarkKind {
    SendMessages,
    PollMessages,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Transport {
    Http,
    Quic,
    Tcp,
}

pub fn display_results(results: Vec<BenchmarkResult>, kind: BenchmarkKind, total_messages: u64) {
    let total_size_bytes = results.iter().map(|r| r.total_size_bytes).sum::<u64>();
    let total_duration = results.iter().map(|r| r.duration).sum::<Duration>();
    let average_latency =
        results.iter().map(|r| r.average_latency).sum::<f64>() / results.len() as f64;
    let average_throughput =
        total_size_bytes as f64 / total_duration.as_secs_f64() / 1024.0 / 1024.0;

    info!(
            "Finished the {} benchmark for total amount of messages: {} in {} ms, total size: {} bytes, average latency: {:.2} ms, average throughput: {:.2} MB/s.",
            kind, total_messages, total_duration.as_millis(), total_size_bytes, average_latency, average_throughput
        );
}

impl Display for BenchmarkKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BenchmarkKind::SendMessages => write!(f, "send messages"),
            BenchmarkKind::PollMessages => write!(f, "poll messages"),
        }
    }
}

impl Display for Transport {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Transport::Http => write!(f, "HTTP"),
            Transport::Quic => write!(f, "QUIC"),
            Transport::Tcp => write!(f, "TCP"),
        }
    }
}

pub async fn start(
    args: Arc<Args>,
    client_factory: Arc<dyn ClientFactory>,
    kind: BenchmarkKind,
) -> Vec<JoinHandle<BenchmarkResult>> {
    let clients_count = match kind {
        BenchmarkKind::SendMessages => args.producers,
        BenchmarkKind::PollMessages => args.consumers,
    };
    info!("Creating {} client(s)...", clients_count);
    let mut futures = Vec::with_capacity(clients_count as usize);
    for client_id in 1..=clients_count {
        let args = args.clone();
        let client_factory = client_factory.clone();
        let future = task::spawn(async move {
            info!("Executing the benchmark on client #{}...", client_id);
            let args = args.clone();
            let start_stream_id = args.get_start_stream_id();
            let client_factory = client_factory.clone();
            let result = match kind {
                BenchmarkKind::SendMessages => {
                    let stream_id = match args.parallel_producer_streams {
                        true => start_stream_id + client_id,
                        false => start_stream_id + 1,
                    };
                    send_messages_benchmark::run(client_factory, client_id, args, stream_id).await
                }
                BenchmarkKind::PollMessages => {
                    let stream_id = match args.parallel_consumer_streams {
                        true => start_stream_id + client_id,
                        false => start_stream_id + 1,
                    };
                    poll_messages_benchmark::run(client_factory, client_id, args, stream_id).await
                }
            };
            match &result {
                Ok(_) => info!("Executed the benchmark on client #{}.", client_id),
                Err(error) => error!("Error on client #{}: {:?}", client_id, error),
            }

            result.unwrap()
        });
        futures.push(future);
    }
    info!("Created {} client(s).", clients_count);

    futures
}
