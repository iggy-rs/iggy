use crate::args::simple::BenchmarkKind;
use crate::benchmark_result::{BenchmarkResult, LatencyPercentiles};
use iggy::client::{ConsumerGroupClient, MessageClient};
use iggy::clients::client::IggyClient;
use iggy::consumer::Consumer as IggyConsumer;
use iggy::error::IggyError;
use iggy::messages::poll_messages::PollingStrategy;
use iggy::utils::duration::IggyDuration;
use integration::test_server::{login_root, ClientFactory};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tracing::{error, info, warn};

pub struct Consumer {
    client_factory: Arc<dyn ClientFactory>,
    consumer_id: u32,
    consumer_group_id: Option<u32>,
    stream_id: u32,
    messages_per_batch: u32,
    message_batches: u32,
    warmup_time: IggyDuration,
}

impl Consumer {
    pub fn new(
        client_factory: Arc<dyn ClientFactory>,
        consumer_id: u32,
        consumer_group_id: Option<u32>,
        stream_id: u32,
        messages_per_batch: u32,
        message_batches: u32,
        warmup_time: IggyDuration,
    ) -> Self {
        Self {
            client_factory,
            consumer_id,
            consumer_group_id,
            stream_id,
            messages_per_batch,
            message_batches,
            warmup_time,
        }
    }

    pub async fn run(&self) -> Result<BenchmarkResult, IggyError> {
        let topic_id: u32 = 1;
        let default_partition_id: u32 = 1;
        let total_messages = (self.messages_per_batch * self.message_batches) as u64;
        let client = self.client_factory.create_client().await;
        let client = IggyClient::create(client, None, None);
        login_root(&client).await;
        let stream_id = self.stream_id.try_into().unwrap();
        let topic_id = topic_id.try_into().unwrap();
        let partition_id = if self.consumer_group_id.is_some() {
            None
        } else {
            Some(default_partition_id)
        };
        let consumer = match self.consumer_group_id {
            Some(consumer_group_id) => {
                client
                    .join_consumer_group(
                        &stream_id,
                        &topic_id,
                        &consumer_group_id.try_into().unwrap(),
                    )
                    .await
                    .expect("Failed to join consumer group");
                IggyConsumer::group(consumer_group_id.try_into().unwrap())
            }
            None => IggyConsumer::new(self.consumer_id.try_into().unwrap()),
        };

        let mut latencies: Vec<Duration> = Vec::with_capacity(self.message_batches as usize);
        let mut total_size_bytes = 0;
        let mut current_iteration: u64 = 0;
        let mut received_messages = 0;
        let mut topic_not_found_counter = 0;
        let mut strategy = PollingStrategy::offset(0);

        if self.warmup_time.get_duration() != Duration::from_millis(0) {
            if let Some(cg_id) = self.consumer_group_id {
                info!(
                    "Consumer #{}, part of consumer group #{}, → warming up for {}...",
                    self.consumer_id, cg_id, self.warmup_time
                );
            } else {
                info!(
                    "Consumer #{} → warming up for {}...",
                    self.consumer_id, self.warmup_time
                );
            }
            let warmup_end = Instant::now() + self.warmup_time.get_duration();
            while Instant::now() < warmup_end {
                let offset = current_iteration * self.messages_per_batch as u64;
                strategy.set_value(offset);
                let polled_messages = client
                    .poll_messages(
                        &stream_id,
                        &topic_id,
                        partition_id,
                        &consumer,
                        &strategy,
                        self.messages_per_batch,
                        false,
                    )
                    .await?;

                if polled_messages.messages.is_empty() {
                    warn!(
                        "Consumer: {} - Messages are empty for offset: {}, retrying...",
                        self.consumer_id, offset
                    );
                    continue;
                }
                current_iteration += 1;
            }
        }

        if let Some(cg_id) = self.consumer_group_id {
            info!(
            "Consumer #{}, part of consumer group #{} → polling {} messages in {} batches of {} messages...",
            self.consumer_id, cg_id, total_messages, self.message_batches, self.messages_per_batch
        );
        } else {
            info!(
                "Consumer #{} → polling {} messages in {} batches of {} messages...",
                self.consumer_id, total_messages, self.message_batches, self.messages_per_batch
            );
        }

        current_iteration = 0;
        let start_timestamp = Instant::now();
        while received_messages < total_messages {
            let offset = current_iteration * self.messages_per_batch as u64;
            let latency_start = Instant::now();

            let polled_messages = client
                .poll_messages(
                    &stream_id,
                    &topic_id,
                    partition_id,
                    &consumer,
                    &PollingStrategy::offset(offset),
                    self.messages_per_batch,
                    false,
                )
                .await;

            let latency_end = latency_start.elapsed();

            if let Err(e) = polled_messages {
                if let IggyError::InvalidResponse(code, _, _) = e {
                    if code == 2010 {
                        topic_not_found_counter += 1;
                        if topic_not_found_counter > 1000 {
                            return Err(e);
                        }
                    }
                } else {
                    return Err(e);
                }
                error!("Unexpected error: {:?}", e);
                continue;
            }

            let polled_messages = polled_messages.unwrap();
            if polled_messages.messages.is_empty() {
                warn!(
                    "Consumer: {} - Messages are empty for offset: {}, retrying...",
                    self.consumer_id, offset
                );
                continue;
            }

            if polled_messages.messages.len() != self.messages_per_batch as usize {
                warn!(
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

        latencies.sort();
        let last_idx = latencies.len() - 1;
        let p50 = latencies[last_idx / 2];
        let p90 = latencies[last_idx * 9 / 10];
        let p95 = latencies[last_idx * 95 / 100];
        let p99 = latencies[last_idx * 99 / 100];
        let p999 = latencies[last_idx * 999 / 1000];
        let latency_percentiles = LatencyPercentiles {
            p50,
            p90,
            p95,
            p99,
            p999,
        };

        let duration = end_timestamp - start_timestamp;
        let average_latency: Duration = latencies.iter().sum::<Duration>() / latencies.len() as u32;
        let average_throughput = total_size_bytes as f64 / duration.as_secs_f64() / 1e6;

        info!(
        "Consumer #{} → polled {} messages {} batches of {} messages in {:.2} s, total size: {} bytes, average throughput: {:.2} MB/s, p50 latency: {:.2} ms, p90 latency: {:.2} ms, p95 latency: {:.2} ms, p99 latency: {:.2} ms, p999 latency: {:.2} ms, average latency: {:.2} ms",
        self.consumer_id,
        total_messages,
        self.message_batches,
        self.messages_per_batch,
        duration.as_secs_f64(),
        total_size_bytes,
        average_throughput,
        p50.as_secs_f64() * 1000.0,
        p90.as_secs_f64() * 1000.0,
        p95.as_secs_f64() * 1000.0,
        p99.as_secs_f64() * 1000.0,
        p999.as_secs_f64() * 1000.0,
        average_latency.as_secs_f64() * 1000.0
    );

        Ok(BenchmarkResult {
            kind: BenchmarkKind::Poll,
            start_timestamp,
            end_timestamp,
            average_latency,
            latency_percentiles,
            total_size_bytes,
            total_messages,
        })
    }
}
