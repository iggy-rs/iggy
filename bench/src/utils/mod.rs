use iggy::{
    client::{Client, SystemClient, UserClient},
    clients::builder::IggyClientBuilder,
    error::IggyError,
    models::{messages::PolledMessages, stats::Stats},
    snapshot::{SnapshotCompression, SystemSnapshotType},
    users::defaults::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME},
    utils::sizeable::Sizeable,
};
use iggy_bench_report::{
    benchmark_kind::BenchmarkKind, individual_metrics::BenchmarkIndividualMetrics,
    numeric_parameter::IggyBenchNumericParameter, params::BenchmarkParams,
    transport::BenchmarkTransport,
};
use integration::test_server::Transport;
use std::{fs, path::Path};
use tracing::{error, info};

use crate::args::{common::IggyBenchArgs, defaults::*};

pub mod batch_generator;
pub mod client_factory;
pub mod cpu_name;
pub mod finish_condition;
pub mod rate_limiter;
pub mod server_starter;

pub fn batch_total_size_bytes(polled_messages: &PolledMessages) -> u64 {
    polled_messages
        .messages
        .iter()
        .map(|m| m.get_size_bytes().as_bytes_u64())
        .sum()
}

pub fn batch_user_size_bytes(polled_messages: &PolledMessages) -> u64 {
    polled_messages
        .messages
        .iter()
        .map(|m| m.payload.len() as u64)
        .sum()
}

pub async fn get_server_stats(
    transport: &BenchmarkTransport,
    server_address: &str,
) -> Result<Stats, IggyError> {
    let client = IggyClientBuilder::new();

    let client = match transport {
        BenchmarkTransport::Tcp => client
            .with_tcp()
            .with_server_address(server_address.to_string())
            .build()?,
        BenchmarkTransport::Http => client
            .with_http()
            .with_api_url(format!("http://{}", server_address))
            .build()?,
        BenchmarkTransport::Quic => client
            .with_quic()
            .with_server_address(server_address.to_string())
            .build()?,
    };

    client.connect().await?;
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await?;

    client.get_stats().await
}

pub async fn collect_server_logs_and_save_to_file(
    transport: &BenchmarkTransport,
    server_address: &str,
    output_dir: &Path,
) -> Result<(), IggyError> {
    let client = IggyClientBuilder::new();

    let client = match transport {
        BenchmarkTransport::Tcp => client
            .with_tcp()
            .with_server_address(server_address.to_string())
            .build()?,
        BenchmarkTransport::Http => client
            .with_http()
            .with_api_url(format!("http://{}", server_address))
            .build()?,
        BenchmarkTransport::Quic => client
            .with_quic()
            .with_server_address(server_address.to_string())
            .build()?,
    };

    client.connect().await?;
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await?;

    let snapshot = client
        .snapshot(
            SnapshotCompression::Deflated,
            vec![SystemSnapshotType::ServerLogs],
        )
        .await?
        .0;

    fs::write(output_dir.join("server_logs.zip"), snapshot).map_err(|e| {
        error!("Failed to write server logs to file: {:?}", e);
        IggyError::CannotWriteToFile
    })
}

fn message_batches_from_metrics(individual_metrics: &[BenchmarkIndividualMetrics]) -> u64 {
    individual_metrics
        .iter()
        .map(|s| s.summary.total_message_batches)
        .sum()
}

pub fn params_from_args_and_metrics(
    args: &IggyBenchArgs,
    metrics: &[BenchmarkIndividualMetrics],
) -> BenchmarkParams {
    let benchmark_kind = args.benchmark_kind.as_simple_kind();

    // Ugly conversion but let it stay here to have `iggy-bench-report` not depend on `iggy` or `integration`
    let transport = match args.transport() {
        Transport::Tcp => BenchmarkTransport::Tcp,
        Transport::Quic => BenchmarkTransport::Quic,
        Transport::Http => BenchmarkTransport::Http,
    };
    let server_address = args.server_address().to_string();
    let remark = args.remark();
    let extra_info = args.extra_info();
    let gitref = args.gitref();
    let gitref_date = args.gitref_date();
    let messages_per_batch = args.messages_per_batch();
    let message_size = args.message_size();
    let message_batches = message_batches_from_metrics(metrics);
    let producers = args.producers();
    let consumers = args.consumers();
    let streams = args.streams();
    let partitions = args.number_of_partitions();
    let consumer_groups = args.number_of_consumer_groups();
    let rate_limit = args.rate_limit().map(|limit| limit.to_string());
    let pretty_name = args.generate_pretty_name();
    let bench_command = recreate_bench_command(args);

    let remark_for_identifier = remark
        .clone()
        .unwrap_or("no_remark".to_string())
        .replace(' ', "_");

    let data_volume_identifier = args.data_volume_identifier();

    let params_identifier = vec![
        benchmark_kind.to_string(),
        transport.to_string(),
        remark_for_identifier,
        messages_per_batch.to_string(),
        data_volume_identifier,
        message_size.to_string(),
        producers.to_string(),
        consumers.to_string(),
        streams.to_string(),
        partitions.to_string(),
        consumer_groups.to_string(),
    ];

    let params_identifier = params_identifier.join("_");

    BenchmarkParams {
        benchmark_kind,
        transport,
        server_address,
        remark,
        extra_info,
        gitref,
        gitref_date,
        messages_per_batch,
        message_batches,
        message_size,
        producers,
        consumers,
        streams,
        partitions,
        consumer_groups,
        rate_limit,
        pretty_name,
        bench_command,
        params_identifier,
    }
}

fn recreate_bench_command(args: &IggyBenchArgs) -> String {
    let mut parts = Vec::new();

    // If using localhost, add env vars
    let server_address = args.server_address();
    let is_localhost = server_address
        .split(':')
        .next()
        .map(|host| host == "localhost" || host == "127.0.0.1")
        .unwrap_or(false);

    if is_localhost {
        // Get all env vars starting with IGGY_
        let iggy_vars: Vec<_> = std::env::vars()
            .filter(|(k, _)| k.starts_with("IGGY_"))
            .collect();

        if !iggy_vars.is_empty() {
            info!("Found env vars starting with IGGY_: {:?}", iggy_vars);
            parts.extend(iggy_vars.into_iter().map(|(k, v)| format!("{}={}", k, v)));
        }
    }

    parts.push("iggy-bench".to_string());

    let messages_per_batch = args.messages_per_batch();
    if messages_per_batch != IggyBenchNumericParameter::Value(DEFAULT_MESSAGES_PER_BATCH.get()) {
        parts.push(format!("--messages-per-batch {}", messages_per_batch));
    }

    let message_batches = args.message_batches();
    if let Some(message_batches) = message_batches {
        if message_batches != DEFAULT_MESSAGE_BATCHES {
            parts.push(format!("--message-batches {}", message_batches));
        }
    }

    let total_messages_size = args.total_data();
    if let Some(total_messages_size) = total_messages_size {
        if total_messages_size != DEFAULT_TOTAL_MESSAGES_SIZE {
            parts.push(format!("--total-messages-size {}", total_messages_size));
        }
    }

    let message_size = args.message_size();
    if message_size != IggyBenchNumericParameter::Value(DEFAULT_MESSAGE_SIZE.get()) {
        parts.push(format!("--message-size {}", message_size));
    }

    if let Some(rate_limit) = args.rate_limit() {
        parts.push(format!("--rate-limit \'{}\'", rate_limit));
    }

    if args.warmup_time().to_string() != DEFAULT_WARMUP_TIME {
        parts.push(format!("--warmup-time \'{}\'", args.warmup_time()));
    }

    let kind_str = match args.benchmark_kind.as_simple_kind() {
        BenchmarkKind::PinnedProducer => "pinned-producer",
        BenchmarkKind::PinnedConsumer => "pinned-consumer",
        BenchmarkKind::PinnedProducerAndConsumer => "pinned-producer-and-consumer",
        BenchmarkKind::BalancedProducer => "balanced-producer",
        BenchmarkKind::BalancedConsumerGroup => "balanced-consumer-group",
        BenchmarkKind::BalancedProducerAndConsumerGroup => "balanced-producer-and-consumer-group",
        BenchmarkKind::EndToEndProducingConsumer => "end-to-end-producing-consumer",
        BenchmarkKind::EndToEndProducingConsumerGroup => "end-to-end-producing-consumer-group",
    };
    parts.push(kind_str.to_string());

    let producers = args.producers();
    let consumers = args.consumers();
    let number_of_consumer_groups = args.number_of_consumer_groups();

    match args.benchmark_kind.as_simple_kind() {
        BenchmarkKind::PinnedProducer
        | BenchmarkKind::BalancedProducer
        | BenchmarkKind::EndToEndProducingConsumer => {
            if producers != DEFAULT_NUMBER_OF_PRODUCERS.get() {
                parts.push(format!("--producers {}", producers));
            }
        }
        BenchmarkKind::PinnedConsumer | BenchmarkKind::BalancedConsumerGroup => {
            if consumers != DEFAULT_NUMBER_OF_CONSUMERS.get() {
                parts.push(format!("--consumers {}", consumers));
            }
        }
        BenchmarkKind::PinnedProducerAndConsumer
        | BenchmarkKind::BalancedProducerAndConsumerGroup => {
            if producers != DEFAULT_NUMBER_OF_PRODUCERS.get() {
                parts.push(format!("--producers {}", producers));
            }
            if consumers != DEFAULT_NUMBER_OF_CONSUMERS.get() {
                parts.push(format!("--consumers {}", consumers));
            }
        }
        BenchmarkKind::EndToEndProducingConsumerGroup => {
            if producers != DEFAULT_NUMBER_OF_PRODUCERS.get() {
                parts.push(format!("--producers {}", producers));
            }
            if consumers != DEFAULT_NUMBER_OF_CONSUMERS.get() {
                parts.push(format!("--consumers {}", consumers));
            }
            if number_of_consumer_groups != DEFAULT_NUMBER_OF_CONSUMER_GROUPS.get() {
                parts.push(format!("--consumer-groups {}", number_of_consumer_groups));
            }
        }
    }

    let streams = args.streams();
    let default_streams = match args.benchmark_kind.as_simple_kind() {
        BenchmarkKind::BalancedProducerAndConsumerGroup
        | BenchmarkKind::BalancedConsumerGroup
        | BenchmarkKind::BalancedProducer => DEFAULT_BALANCED_NUMBER_OF_STREAMS.get(),
        _ => DEFAULT_PINNED_NUMBER_OF_STREAMS.get(),
    };
    if streams != default_streams {
        parts.push(format!("--streams {}", streams));
    }

    let partitions = args.number_of_partitions();
    let default_partitions = match args.benchmark_kind.as_simple_kind() {
        BenchmarkKind::BalancedProducerAndConsumerGroup
        | BenchmarkKind::BalancedConsumerGroup
        | BenchmarkKind::BalancedProducer => DEFAULT_BALANCED_NUMBER_OF_PARTITIONS.get(),
        _ => DEFAULT_PINNED_NUMBER_OF_PARTITIONS.get(),
    };
    if partitions != default_partitions {
        parts.push(format!("--partitions {}", partitions));
    }

    let consumer_groups = args.number_of_consumer_groups();
    if args.benchmark_kind.as_simple_kind() == BenchmarkKind::BalancedConsumerGroup
        || args.benchmark_kind.as_simple_kind() == BenchmarkKind::BalancedProducerAndConsumerGroup
            && consumer_groups != DEFAULT_NUMBER_OF_CONSUMER_GROUPS.get()
    {
        parts.push(format!("--consumer-groups {}", consumer_groups));
    }

    if let Some(max_topic_size) = args.max_topic_size() {
        parts.push(format!("--max-topic-size \'{}\'", max_topic_size));
    }

    let transport = args.transport().to_string().to_lowercase();
    parts.push(transport.clone());

    let default_address = match transport.as_str() {
        "tcp" => DEFAULT_TCP_SERVER_ADDRESS,
        "quic" => DEFAULT_QUIC_SERVER_ADDRESS,
        "http" => DEFAULT_HTTP_SERVER_ADDRESS,
        _ => "",
    };

    if server_address != default_address {
        parts.push(format!("--server-address {}", server_address));
    }

    parts.push("output".to_string());

    parts.push("-o performance_results".to_string());

    let remark = args.remark();
    if let Some(remark) = remark {
        parts.push(format!("--remark \'{}\'", remark));
    }
    parts.join(" ")
}
