use crate::args::simple::BenchmarkKind;
use crate::benchmark_result::BenchmarkResult;
use crate::statistics::actor_statistics::BenchmarkActorStatistics;
use crate::statistics::record::BenchmarkRecord;
use iggy::client::{ConsumerGroupClient, MessageClient};
use iggy::clients::client::IggyClient;
use iggy::consumer::Consumer as IggyConsumer;
use iggy::error::IggyError;
use iggy::messages::poll_messages::PollingStrategy;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::duration::IggyDuration;
use iggy::utils::sizeable::Sizeable;
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
    output_directory: Option<String>,
}

impl Consumer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client_factory: Arc<dyn ClientFactory>,
        consumer_id: u32,
        consumer_group_id: Option<u32>,
        stream_id: u32,
        messages_per_batch: u32,
        message_batches: u32,
        warmup_time: IggyDuration,
        output_directory: Option<String>,
    ) -> Self {
        Self {
            client_factory,
            consumer_id,
            consumer_group_id,
            stream_id,
            messages_per_batch,
            message_batches,
            warmup_time,
            output_directory,
        }
    }

    pub async fn run(&self) -> Result<BenchmarkResult, IggyError> {
        let topic_id: u32 = 1;
        let default_partition_id: u32 = 1;
        let message_batches = self.message_batches as u64;
        let messages_per_batch = self.messages_per_batch;
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

        let mut latencies: Vec<Duration> = Vec::with_capacity(message_batches as usize);
        let mut total_user_data_bytes = IggyByteSize::default();
        let mut total_bytes = IggyByteSize::default();
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
                let offset = current_iteration * messages_per_batch as u64;
                strategy.set_value(offset);
                let polled_messages = client
                    .poll_messages(
                        &stream_id,
                        &topic_id,
                        partition_id,
                        &consumer,
                        &strategy,
                        messages_per_batch,
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
            self.consumer_id, cg_id, total_messages, message_batches, messages_per_batch
        );
        } else {
            info!(
                "Consumer #{} → polling {} messages in {} batches of {} messages...",
                self.consumer_id, total_messages, message_batches, messages_per_batch
            );
        }

        current_iteration = 0;
        let mut previous_start_offset = 0;
        let start_timestamp = Instant::now();
        let mut records = Vec::with_capacity(message_batches as usize);
        let mut batch_user_size_bytes = 0;
        let mut batch_size_total_bytes = 0;
        while received_messages < total_messages {
            let offset = current_iteration * messages_per_batch as u64;

            let before_poll = Instant::now();
            let polled_messages = client
                .poll_messages(
                    &stream_id,
                    &topic_id,
                    partition_id,
                    &consumer,
                    &PollingStrategy::offset(offset),
                    messages_per_batch,
                    false,
                )
                .await;
            let latency = before_poll.elapsed();

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
            assert!(previous_start_offset <= polled_messages.messages[0].offset);
            if polled_messages.messages.len() != messages_per_batch as usize {
                warn!(
                    "Consumer #{} → expected {} messages, but got {} messages, retrying...",
                    self.consumer_id,
                    messages_per_batch,
                    polled_messages.messages.len()
                );
                continue;
            }
            previous_start_offset = polled_messages.messages[0].offset;
            latencies.push(latency);
            received_messages += polled_messages.messages.len() as u64;

            // We don't need to calculate the size whole batch every time by iterating over it - just always use the size of the first message
            if batch_user_size_bytes == 0 || batch_size_total_bytes == 0 {
                batch_user_size_bytes =
                    polled_messages.messages[0].payload.len() as u64 * messages_per_batch as u64;
                batch_size_total_bytes =
                    polled_messages.messages[0].get_size_bytes().as_bytes_u64()
                        * messages_per_batch as u64;
            }

            total_user_data_bytes += IggyByteSize::from(batch_user_size_bytes);
            total_bytes += IggyByteSize::from(batch_size_total_bytes);
            current_iteration += 1;
            let message_batches = current_iteration;
            records.push(BenchmarkRecord::new(
                start_timestamp.elapsed().as_micros() as u64,
                latency.as_micros() as u64,
                received_messages,
                message_batches,
                total_user_data_bytes.as_bytes_u64(),
                total_bytes.as_bytes_u64(),
            ));
        }

        let statistics = BenchmarkActorStatistics::from_records(&records);

        if let Some(output_directory) = &self.output_directory {
            std::fs::create_dir_all(format!("{}/raw_data", output_directory)).unwrap();

            // Dump raw data to file
            let output_file = format!(
                "{}/raw_data/consumer_{}_data.csv",
                output_directory, self.consumer_id
            );
            info!(
                "Consumer #{} → writing the results to {}...",
                self.consumer_id, output_file
            );
            let mut writer = csv::Writer::from_path(output_file).unwrap();
            for sample in records {
                writer.serialize(sample).unwrap();
            }
            writer.flush().unwrap();

            // Dump summary to file
            let summary_file = format!(
                "{}/raw_data/consumer_{}_summary.toml",
                output_directory, self.consumer_id
            );
            statistics.dump_to_toml(&summary_file);
        }

        Self::log_consumer_statistics(
            self.consumer_id,
            total_messages,
            message_batches as u32,
            messages_per_batch,
            &statistics,
        );

        Ok(BenchmarkResult {
            kind: BenchmarkKind::Poll,
            statistics,
        })
    }

    pub fn log_consumer_statistics(
        consumer_id: u32,
        total_messages: u64,
        message_batches: u32,
        messages_per_batch: u32,
        stats: &BenchmarkActorStatistics,
    ) {
        info!(
            "Consumer #{} → polled {} messages, {} batches of {} messages in {:.2} s, total size: {}, average throughput: {:.2} MB/s, \
    p50 latency: {:.2} ms, p90 latency: {:.2} ms, p95 latency: {:.2} ms, p99 latency: {:.2} ms, p999 latency: {:.2} ms, \
    average latency: {:.2} ms, median latency: {:.2} ms",
            consumer_id,
            total_messages,
            message_batches,
            messages_per_batch,
            stats.total_time_secs,
            IggyByteSize::from(stats.total_user_data_bytes),
            stats.throughput_megabytes_per_second,
            stats.p50_latency_ms,
            stats.p90_latency_ms,
            stats.p95_latency_ms,
            stats.p99_latency_ms,
            stats.p999_latency_ms,
            stats.avg_latency_ms,
            stats.median_latency_ms
        );
    }
}
