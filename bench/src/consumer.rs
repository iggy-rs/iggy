use crate::args::simple::BenchmarkKind;
use crate::benchmark_result::BenchmarkResult;
use iggy::client::MessageClient;
use iggy::clients::client::{IggyClient, IggyClientBackgroundConfig};
use iggy::consumer::Consumer as IggyConsumer;
use iggy::error::IggyError;
use iggy::identifier::Identifier;
use iggy::messages::poll_messages::{PollMessages, PollingStrategy};
use iggy::utils::duration::IggyDuration;
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
    warmup_time: IggyDuration,
}

impl Consumer {
    pub fn new(
        client_factory: Arc<dyn ClientFactory>,
        consumer_id: u32,
        stream_id: u32,
        messages_per_batch: u32,
        message_batches: u32,
        warmup_time: IggyDuration,
    ) -> Self {
        Self {
            client_factory,
            consumer_id,
            stream_id,
            messages_per_batch,
            message_batches,
            warmup_time,
        }
    }

    pub async fn run(&self) -> Result<BenchmarkResult, IggyError> {
        let topic_id: u32 = 1;
        let partition_id: u32 = 1;
        let total_messages = (self.messages_per_batch * self.message_batches) as u64;
        let client = self.client_factory.create_client().await;
        let client = IggyClient::create(
            client,
            IggyClientBackgroundConfig::default(),
            None,
            None,
            None,
        );
        login_root(&client).await;

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
        let mut current_iteration: u64 = 0;
        let mut received_messages = 0;
        let mut topic_not_found_counter = 0;

        info!(
            "Consumer #{} → warming up for {}...",
            self.consumer_id, self.warmup_time
        );
        let warmup_end = Instant::now() + self.warmup_time.get_duration();
        while Instant::now() < warmup_end {
            let offset = current_iteration * self.messages_per_batch as u64;
            poll_messages.strategy.value = offset;
            client.poll_messages(&poll_messages).await?;
            current_iteration += 1;
        }

        info!(
            "Consumer #{} → polling {} messages in {} batches of {} messages...",
            self.consumer_id, total_messages, self.message_batches, self.messages_per_batch
        );

        current_iteration = 0;
        let start_timestamp = Instant::now();
        while received_messages < total_messages {
            let offset = current_iteration * self.messages_per_batch as u64;
            poll_messages.strategy.value = offset;

            let latency_start = Instant::now();

            let polled_messages: Result<iggy::models::messages::PolledMessages, IggyError> =
                client.poll_messages(&poll_messages).await;

            let latency_end = latency_start.elapsed();

            if let Err(e) = polled_messages {
                if let IggyError::InvalidResponse(code, _, _) = e {
                    if code == 2010 {
                        topic_not_found_counter += 1;
                        if topic_not_found_counter > 200 {
                            continue;
                        }
                        return Err(e);
                    }
                } else {
                    return Err(e);
                }
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
        let average_throughput = total_size_bytes as f64 / duration.as_secs_f64() / 1e6;

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
