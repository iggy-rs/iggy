use crate::args::simple::BenchmarkKind;
use crate::benchmark_result::BenchmarkResult;
use iggy::client::MessageClient;
use iggy::clients::client::{IggyClient, IggyClientBackgroundConfig};
use iggy::error::IggyError;
use iggy::messages::send_messages::{Message, Partitioning};
use iggy::utils::duration::IggyDuration;
use integration::test_server::{login_root, ClientFactory};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tracing::info;

pub struct Producer {
    client_factory: Arc<dyn ClientFactory>,
    producer_id: u32,
    stream_id: u32,
    messages_per_batch: u32,
    message_batches: u32,
    message_size: u32,
    warmup_time: IggyDuration,
}

impl Producer {
    pub fn new(
        client_factory: Arc<dyn ClientFactory>,
        producer_id: u32,
        stream_id: u32,
        messages_per_batch: u32,
        message_batches: u32,
        message_size: u32,
        warmup_time: IggyDuration,
    ) -> Self {
        Producer {
            client_factory,
            producer_id,
            stream_id,
            messages_per_batch,
            message_batches,
            message_size,
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
        info!(
            "Producer #{} → preparing the test messages...",
            self.producer_id
        );
        let payload = Self::create_payload(self.message_size);
        let mut messages = Vec::with_capacity(self.messages_per_batch as usize);
        for _ in 0..self.messages_per_batch {
            let message = Message::from_str(&payload).unwrap();
            messages.push(message);
        }

        let stream_id = self.stream_id.try_into()?;
        let topic_id = topic_id.try_into()?;
        let partitioning = Partitioning::partition_id(partition_id);

        info!(
            "Producer #{} → warming up for {}...",
            self.producer_id, self.warmup_time
        );
        let warmup_end = Instant::now() + self.warmup_time.get_duration();
        while Instant::now() < warmup_end {
            client
                .send_messages(&stream_id, &topic_id, &partitioning, &mut messages)
                .await?;
        }

        info!(
            "Producer #{} → sending {} messages in {} batches of {} messages...",
            self.producer_id, total_messages, self.message_batches, self.messages_per_batch
        );

        let start_timestamp = Instant::now();
        let mut latencies: Vec<Duration> = Vec::with_capacity(self.message_batches as usize);
        for _ in 0..self.message_batches {
            let latency_start = Instant::now();
            client
                .send_messages(&stream_id, &topic_id, &partitioning, &mut messages)
                .await?;
            let latency_end = latency_start.elapsed();
            latencies.push(latency_end);
        }
        let end_timestamp = Instant::now();
        let duration = end_timestamp - start_timestamp;
        let average_latency: Duration = latencies.iter().sum::<Duration>() / latencies.len() as u32;
        let total_size_bytes = total_messages * self.message_size as u64;
        let average_throughput = total_size_bytes as f64 / duration.as_secs_f64() / 1e6;

        info!(
        "Producer #{} → sent {} messages in {} batches of {} messages in {:.2} s, total size: {} bytes, average latency: {:.2} ms, average throughput: {:.2} MB/s",
        self.producer_id,
        total_messages,
        self.message_batches,
        self.messages_per_batch,
        duration.as_secs_f64(),
        total_size_bytes,
        average_latency.as_secs_f64() * 1000.0,
        average_throughput
    );

        Ok(BenchmarkResult {
            kind: BenchmarkKind::Send,
            start_timestamp,
            end_timestamp,
            average_latency,
            total_size_bytes,
            total_messages,
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
}
