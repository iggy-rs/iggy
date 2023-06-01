use crate::args::Args;
use crate::benchmark_result::BenchmarkResult;
use crate::benchmarks::*;
use crate::client_factory::ClientFactory;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use tokio::task;
use tokio::task::JoinHandle;
use tracing::{error, info};

#[derive(Debug, Clone, Copy)]
pub enum BenchmarkKind {
    SendMessages,
    PollMessages,
}

#[derive(Debug, Clone, Copy)]
pub enum Transport {
    Http,
    Quic,
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
        }
    }
}

pub async fn start(
    args: Arc<Args>,
    start_stream_id: u32,
    client_factory: &dyn ClientFactory,
    kind: BenchmarkKind,
) -> Vec<JoinHandle<BenchmarkResult>> {
    info!("Creating {} client(s)...", args.clients_count);
    let mut futures = Vec::with_capacity(args.clients_count as usize);
    for i in 0..args.clients_count {
        let client_id = i + 1;
        let args = args.clone();
        let client = client_factory.create_client(args.clone()).await;
        let future = task::spawn(async move {
            info!("Executing the benchmark on client #{}...", client_id);
            let args = args.clone();
            let result = match kind {
                BenchmarkKind::SendMessages => {
                    send_messages_benchmark::run(client.as_ref(), client_id, args, start_stream_id)
                        .await
                }
                BenchmarkKind::PollMessages => {
                    poll_messages_benchmark::run(client.as_ref(), client_id, args, start_stream_id)
                        .await
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
    info!("Created {} client(s).", args.clients_count);

    futures
}
