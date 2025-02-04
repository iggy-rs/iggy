use crate::actors::utils::{calculate_latency_from_first_message, put_timestamp_in_first_message};
use crate::analytics::metrics::individual::from_records;
use crate::analytics::record::BenchmarkRecord;
use crate::rate_limiter::RateLimiter;
use human_repr::HumanCount;
use iggy::client::MessageClient;
use iggy::clients::client::IggyClient;
use iggy::consumer::Consumer as IggyConsumer;
use iggy::error::IggyError;
use iggy::messages::poll_messages::PollingStrategy;
use iggy::messages::send_messages::{Message, Partitioning};
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::duration::IggyDuration;
use iggy::utils::sizeable::Sizeable;
use iggy_bench_report::actor_kind::ActorKind;
use iggy_bench_report::benchmark_kind::BenchmarkKind;
use iggy_bench_report::individual_metrics::BenchmarkIndividualMetrics;
use integration::test_server::{login_root, ClientFactory};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tracing::{info, warn};

pub struct ProducingConsumer {
    client_factory: Arc<dyn ClientFactory>,
    benchmark_kind: BenchmarkKind,
    actor_id: u32,
    stream_id: u32,
    partitions_count: u32,
    messages_per_batch: u32,
    message_batches: u32,
    message_size: u32,
    warmup_time: IggyDuration,
    sampling_time: IggyDuration,
    moving_average_window: u32,
    rate_limiter: Option<RateLimiter>,
    calculate_latency_from_timestamp_in_first_message: bool,
}

impl ProducingConsumer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client_factory: Arc<dyn ClientFactory>,
        benchmark_kind: BenchmarkKind,
        actor_id: u32,
        stream_id: u32,
        partitions_count: u32,
        messages_per_batch: u32,
        message_batches: u32,
        message_size: u32,
        warmup_time: IggyDuration,
        sampling_time: IggyDuration,
        moving_average_window: u32,
        rate_limiter: Option<RateLimiter>,
        calculate_latency_from_timestamp_in_first_message: bool,
    ) -> Self {
        Self {
            client_factory,
            benchmark_kind,
            actor_id,
            stream_id,
            partitions_count,
            messages_per_batch,
            message_batches,
            message_size,
            warmup_time,
            sampling_time,
            moving_average_window,
            rate_limiter,
            calculate_latency_from_timestamp_in_first_message,
        }
    }

    pub async fn run(&self) -> Result<BenchmarkIndividualMetrics, IggyError> {
        let topic_id: u32 = 1;
        let default_partition_id: u32 = 1;
        let message_batches = self.message_batches;
        let messages_per_batch = self.messages_per_batch;
        let message_size = self.message_size;
        let total_messages = (messages_per_batch * message_batches) as u64;

        let client = self.client_factory.create_client().await;
        let client = IggyClient::create(client, None, None);
        login_root(&client).await;

        info!(
            "ProducingConsumer #{} → preparing test messages...",
            self.actor_id
        );
        let payload = self.create_payload(message_size);

        let stream_id = self.stream_id.try_into()?;
        let topic_id = topic_id.try_into()?;
        let partitioning = match self.partitions_count {
            0 => panic!("Partition count must be greater than 0"),
            1 => Partitioning::partition_id(default_partition_id),
            2.. => Partitioning::balanced(),
        };

        let consumer = IggyConsumer::new(self.actor_id.try_into().unwrap());
        let mut current_offset: u64 = 0;

        /*
        // Warmup if needed
        if self.warmup_time.get_duration() != Duration::from_millis(0) {
            info!(
                "ProducingConsumer #{} → warming up for {}...",
                self.actor_id, self.warmup_time
            );
            let warmup_end = Instant::now() + self.warmup_time.get_duration();
            while Instant::now() < warmup_end {
                client
                    .send_messages(&stream_id, &topic_id, &partitioning, messages)
                    .await?;

                let strategy = PollingStrategy::offset(current_offset);
                let polled_messages = client
                    .poll_messages(
                        &stream_id,
                        &topic_id,
                        Some(default_partition_id),
                        &consumer,
                        &strategy,
                        messages_per_batch,
                        false,
                    )
                    .await?;

                if polled_messages.is_empty() {
                    warn!(
                        "ProducingConsumer #{} - Messages are empty for offset: {}, retrying...",
                        self.actor_id, current_offset
                    );
                    continue;
                }
                current_offset += messages_per_batch as u64;
            }
        }
        */

        info!(
            "ProducingConsumer #{} → sending and polling {} messages in {} batches of {} messages from/to stream {}...",
            self.actor_id,
            total_messages.human_count_bare(),
            message_batches.human_count_bare(),
            messages_per_batch.human_count_bare(),
            stream_id
        );

        let start_timestamp = Instant::now();
        let mut latencies: Vec<Duration> = Vec::with_capacity(message_batches as usize);
        let mut records: Vec<BenchmarkRecord> = Vec::with_capacity(message_batches as usize);

        for batch_id in 1..=message_batches {
            let mut batch_user_data_bytes = 0;
            let mut batch_total_bytes = 0;
            let mut messages = Vec::with_capacity(messages_per_batch as usize);
            for _ in 0..messages_per_batch {
                let message = Message::from_str(&payload).unwrap();
                batch_user_data_bytes += message.length as u64;
                batch_total_bytes += message.get_size_bytes().as_bytes_u64();
                messages.push(message);
            }
            if let Some(rate_limiter) = &self.rate_limiter {
                rate_limiter.throttle(batch_user_data_bytes).await;
            }

            put_timestamp_in_first_message(&mut messages[0]);
            let before_send = Instant::now();
            client
                .send_messages(&stream_id, &topic_id, &partitioning, messages)
                .await?;
            let strategy = PollingStrategy::offset(current_offset);
            let polled_messages = client
                .poll_messages(
                    &stream_id,
                    &topic_id,
                    Some(default_partition_id),
                    &consumer,
                    &strategy,
                    messages_per_batch,
                    false,
                )
                .await?;
            if polled_messages.is_empty() {
                warn!(
                    "ProducingConsumer #{} - Messages are empty for offset: {}, retrying...",
                    self.actor_id, current_offset
                );
                continue;
            }
            // Extract send timestamp from first message in batch
            let latency = if self.calculate_latency_from_timestamp_in_first_message {
                calculate_latency_from_first_message(&polled_messages[0])
            } else {
                before_send.elapsed()
            };
            latencies.push(latency);

            current_offset += messages_per_batch as u64;

            records.push(BenchmarkRecord {
                elapsed_time_us: start_timestamp.elapsed().as_micros() as u64,
                latency_us: latency.as_micros() as u64,
                messages: (batch_id * messages_per_batch) as u64,
                message_batches: batch_id as u64,
                user_data_bytes: batch_id as u64 * batch_user_data_bytes,
                total_bytes: batch_id as u64 * batch_total_bytes,
            });
        }

        let metrics = from_records(
            records,
            self.benchmark_kind,
            ActorKind::ProducingConsumer,
            self.actor_id,
            self.sampling_time,
            self.moving_average_window,
        );

        Self::log_statistics(
            self.actor_id,
            total_messages,
            message_batches,
            messages_per_batch,
            &metrics,
        );
        Ok(metrics)
    }

    fn create_payload(&self, size: u32) -> String {
        "a".repeat(size as usize)
    }

    fn log_statistics(
        actor_id: u32,
        total_messages: u64,
        message_batches: u32,
        messages_per_batch: u32,
        metrics: &BenchmarkIndividualMetrics,
    ) {
        info!(
            "ProducingConsumer #{} → sent {} messages in {} batches of {} messages in {:.2} s, total size: {}, average throughput: {:.2} MB/s, \
    p50 latency: {:.2} ms, p90 latency: {:.2} ms, p95 latency: {:.2} ms, p99 latency: {:.2} ms, p999 latency: {:.2} ms, p9999 latency: {:.2} ms, \
    average latency: {:.2} ms, median latency: {:.2} ms",
            actor_id,
            total_messages,
            message_batches,
            messages_per_batch,
            metrics.summary.total_time_secs,
            IggyByteSize::from(metrics.summary.total_user_data_bytes),
            metrics.summary.throughput_megabytes_per_second,
            metrics.summary.p50_latency_ms,
            metrics.summary.p90_latency_ms,
            metrics.summary.p95_latency_ms,
            metrics.summary.p99_latency_ms,
            metrics.summary.p999_latency_ms,
            metrics.summary.p9999_latency_ms,
            metrics.summary.avg_latency_ms,
            metrics.summary.median_latency_ms
        );
    }
}
