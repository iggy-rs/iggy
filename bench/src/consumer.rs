use crate::args::simple::BenchmarkKind;
use crate::benchmark_result::BenchmarkResult;
use iggy::client::MessageClient;
use iggy::clients::client::{IggyClient, IggyClientConfig};
use iggy::consumer::Consumer as IggyConsumer;
use iggy::error::Error;
use iggy::identifier::Identifier;
use iggy::messages::poll_messages::{PollMessages, PollingStrategy};
use integration::test_server::{login_root, ClientFactory};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tracing::{info, trace};

pub struct Consumer {
    client_factory: Arc<dyn ClientFactory>,
    consumer_id: u32,
    stream_id: u32,
    messages_per_batch: u32,
    message_batches: u32,
}

impl Consumer {
    pub fn new(
        client_factory: Arc<dyn ClientFactory>,
        consumer_id: u32,
        stream_id: u32,
        messages_per_batch: u32,
        message_batches: u32,
    ) -> Self {
        Self {
            client_factory,
            consumer_id,
            stream_id,
            messages_per_batch,
            message_batches,
        }
    }

    pub async fn run(&self) -> Result<BenchmarkResult, Error> {
        let topic_id: u32 = 1;
        let partition_id: u32 = 1;
        let total_messages = (self.messages_per_batch * self.message_batches) as u64;
        let client = self.client_factory.create_client().await;
        let client = IggyClient::create(client, IggyClientConfig::default(), None, None, None);
        login_root(&client).await;
        info!(
            "Consumer #{} → preparing the test messages...",
            self.consumer_id
        );
        info!(
            "Consumer #{} → polling {} messages in {} batches of {} messages...",
            self.consumer_id, total_messages, self.message_batches, self.messages_per_batch
        );

        let mut poll_messages = PollMessages {
            consumer: IggyConsumer::new(Identifier::numeric(self.consumer_id).unwrap()),
            stream_id: Identifier::numeric(self.stream_id).unwrap(),
            topic_id: Identifier::numeric(topic_id).unwrap(),
            partition_id: Some(partition_id),
            strategy: PollingStrategy::offset(0),
            count: self.messages_per_batch,
            auto_commit: false,
        };

        let mut latencies: Vec<Duration> = Vec::with_capacity(self.message_batches as usize);
        let mut total_size_bytes = 0;
        let mut current_iteration = 0;
        let mut received_messages = 0;
        let start_timestamp = Instant::now();
        while received_messages < total_messages {
            let offset = (current_iteration * self.messages_per_batch) as u64;
            poll_messages.strategy.value = offset;

            let latency_start = Instant::now();
            let polled_messages = client.poll_messages(&poll_messages).await;
            let latency_end = latency_start.elapsed();
            if polled_messages.is_err() {
                trace!("Offset: {} is not available yet, retrying...", offset);
                continue;
            }

            let polled_messages = polled_messages.unwrap();
            if polled_messages.messages.is_empty() {
                trace!("Messages are empty for offset: {}, retrying...", offset);
                continue;
            }

            if polled_messages.messages.len() != self.messages_per_batch as usize {
                trace!(
                    "Consumer #{} → expected {} messages, but got {} messages, retrying...",
                    self.consumer_id,
                    self.messages_per_batch,
                    polled_messages.messages.len()
                );
                continue;
            }

            latencies.push(latency_end);
            received_messages += polled_messages.messages.len() as u64;
            for message in polled_messages.messages {
                total_size_bytes += message.get_size_bytes() as u64;
            }
            current_iteration += 1;
        }

        let end_timestamp = Instant::now();
        let duration = end_timestamp - start_timestamp;
        let average_latency: Duration = latencies.iter().sum::<Duration>() / latencies.len() as u32;
        let average_throughput = total_size_bytes as f64 / duration.as_secs_f64() / 1024.0 / 1024.0;

        info!(
        "Consumer #{} → polled {} messages ({} batches of {} messages in {} ms, total size: {} bytes, average latency: {:.2} ms, average throughput: {:.2} MB/s",
        self.consumer_id,
        total_messages,
        self.message_batches,
        self.messages_per_batch,
        duration.as_millis(),
        total_size_bytes,
        average_latency.as_millis(),
        average_throughput
    );

        Ok(BenchmarkResult {
            kind: BenchmarkKind::Poll,
            start_timestamp,
            end_timestamp,
            average_latency,
            total_size_bytes,
            total_messages,
        })
    }
}
