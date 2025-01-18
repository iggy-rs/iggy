use crate::args::simple::BenchmarkKind;
use crate::benchmark_result::BenchmarkResult;
use crate::statistics::actor_statistics::BenchmarkActorStatistics;
use crate::statistics::record::BenchmarkRecord;
use iggy::client::MessageClient;
use iggy::clients::client::IggyClient;
use iggy::error::IggyError;
use iggy::messages::send_messages::{Message, Partitioning};
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::duration::IggyDuration;
use iggy::utils::sizeable::Sizeable;
use integration::test_server::{login_root, ClientFactory};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tracing::{error, info};

pub struct Producer {
    client_factory: Arc<dyn ClientFactory>,
    producer_id: u32,
    stream_id: u32,
    partitions_count: u32,
    message_batches: u32,
    messages_per_batch: u32,
    message_size: u32,
    warmup_time: IggyDuration,
    output_directory: Option<String>,
}

impl Producer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client_factory: Arc<dyn ClientFactory>,
        producer_id: u32,
        stream_id: u32,
        partitions_count: u32,
        messages_per_batch: u32,
        message_batches: u32,
        message_size: u32,
        warmup_time: IggyDuration,
        output_directory: Option<String>,
    ) -> Self {
        Producer {
            client_factory,
            producer_id,
            stream_id,
            partitions_count,
            messages_per_batch,
            message_batches,
            message_size,
            warmup_time,
            output_directory,
        }
    }

    fn generate_messages(messages_per_batch: u32, message_size: u32) -> (u64, u64, Vec<Message>) {
        let payload = Self::create_payload(message_size);
        let mut messages = Vec::with_capacity(messages_per_batch as usize);
        let mut batch_user_data_bytes = 0;
        let mut batch_total_bytes = 0;
        for _ in 0..messages_per_batch {
            let message = Message::from_str(&payload).unwrap();
            batch_user_data_bytes += message.length as u64;
            batch_total_bytes += message.get_size_bytes().as_bytes_u64();
            messages.push(message);
        }
        (batch_user_data_bytes, batch_total_bytes, messages)
    }

    pub async fn run(&self) -> Result<BenchmarkResult, IggyError> {
        let topic_id: u32 = 1;
        let default_partition_id: u32 = 1;
        let partitions_count = self.partitions_count;
        let message_batches = self.message_batches;
        let messages_per_batch = self.messages_per_batch;
        let message_size = self.message_size;

        let total_messages = (messages_per_batch * message_batches) as u64;
        let client = self.client_factory.create_client().await;
        let client = IggyClient::create(client, None, None);
        login_root(&client).await;
        info!(
            "Producer #{} → preparing the test messages...",
            self.producer_id
        );
        let stream_id = self.stream_id.try_into()?;
        let topic_id = topic_id.try_into()?;
        let partitioning = match partitions_count {
            0 => panic!("Partition count must be greater than 0"),
            1 => Partitioning::partition_id(default_partition_id),
            2.. => Partitioning::balanced(),
        };

        /*
        if self.warmup_time.get_duration() != Duration::from_millis(0) {
            info!(
                "Producer #{} → warming up for {}...",
                self.producer_id, self.warmup_time
            );
            let warmup_end = Instant::now() + self.warmup_time.get_duration();
            while Instant::now() < warmup_end {
                let (_, _, messages) = Self::generate_messages(messages_per_batch, message_size);
                client
                    .send_messages(&stream_id, &topic_id, &partitioning, messages)
                    .await?;
            }
        }
        */

        info!(
            "Producer #{} → sending {} messages in {} batches of {} messages...",
            self.producer_id, total_messages, message_batches, messages_per_batch
        );

        let start_timestamp = Instant::now();
        let mut latencies: Vec<Duration> = Vec::with_capacity(message_batches as usize);
        let mut records: Vec<BenchmarkRecord> = Vec::with_capacity(message_batches as usize);
        for i in 1..=message_batches {
            let (batch_user_data_bytes, batch_total_bytes, messages) =
                Self::generate_messages(messages_per_batch, message_size);
            let before_send = Instant::now();
            client
                .send_messages(&stream_id, &topic_id, &partitioning, messages)
                .await?;
            let latency = before_send.elapsed();
            error!(
                "send_messages took: {} ms, iter: {}",
                latency.as_millis(),
                i
            );

            let messages_processed = (i * messages_per_batch) as u64;
            let batches_processed = i as u64;
            let total_user_data_bytes = batches_processed * batch_user_data_bytes;
            let total_bytes = batches_processed * batch_total_bytes;

            latencies.push(latency);
            records.push(BenchmarkRecord::new(
                start_timestamp.elapsed().as_micros() as u64,
                latency.as_micros() as u64,
                messages_processed,
                batches_processed,
                total_user_data_bytes,
                total_bytes,
            ));
        }
        let statistics = BenchmarkActorStatistics::from_records(&records);

        if let Some(output_directory) = &self.output_directory {
            std::fs::create_dir_all(format!("{}/raw_data", output_directory)).unwrap();

            // Dump raw data to file
            let results_file = format!(
                "{}/raw_data/producer_{}_data.csv",
                output_directory, self.producer_id
            );
            info!(
                "Producer #{} → writing the results to {}...",
                self.producer_id, results_file
            );

            let mut writer = csv::Writer::from_path(results_file).unwrap();
            for sample in records {
                writer.serialize(sample).unwrap();
            }
            writer.flush().unwrap();

            // Dump summary to file
            let summary_file = format!(
                "{}/raw_data/producer_{}_summary.toml",
                output_directory, self.producer_id
            );
            statistics.dump_to_toml(&summary_file);
        }

        Self::log_producer_statistics(
            self.producer_id,
            total_messages,
            message_batches,
            messages_per_batch,
            &statistics,
        );

        Ok(BenchmarkResult {
            kind: BenchmarkKind::Send,
            statistics,
        })
    }

    fn create_payload(size: u32) -> String {
        let mut payload = String::with_capacity(size as usize);
        for i in 0..size {
            let char = (i % 26 + 97) as u8 as char;
            payload.push(char);
        }

        payload
    }

    fn log_producer_statistics(
        producer_id: u32,
        total_messages: u64,
        message_batches: u32,
        messages_per_batch: u32,
        stats: &BenchmarkActorStatistics,
    ) {
        info!(
            "Producer #{} → sent {} messages in {} batches of {} messages in {:.2} s, total size: {}, average throughput: {:.2} MB/s, \
    p50 latency: {:.2} ms, p90 latency: {:.2} ms, p95 latency: {:.2} ms, p99 latency: {:.2} ms, p999 latency: {:.2} ms, \
    average latency: {:.2} ms, median latency: {:.2} ms",
            producer_id,
            total_messages,
            message_batches,
            messages_per_batch,
            stats.total_time_secs,
            IggyByteSize::from(stats.total_bytes),
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
