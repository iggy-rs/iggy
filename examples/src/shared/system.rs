use crate::shared::args::Args;
use futures_util::StreamExt;
use iggy::client::Client;
use iggy::clients::client::IggyClient;
use iggy::compression::compression_algorithm::CompressionAlgorithm;
use iggy::consumer::{Consumer, ConsumerKind};
use iggy::error::IggyError;
use iggy::identifier::Identifier;
use iggy::messages::poll_messages::PollingStrategy;
use iggy::models::messages::PolledMessage;
use iggy::users::defaults::*;
use iggy::utils::expiry::IggyExpiry;
use iggy::utils::topic_size::MaxTopicSize;
use tracing::info;

type MessageHandler = dyn Fn(&PolledMessage) -> Result<(), Box<dyn std::error::Error>>;

pub async fn login_root(client: &dyn Client) {
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
}

pub async fn init_by_consumer(args: &Args, client: &dyn Client) {
    let (stream_id, topic_id, partition_id) = (args.stream_id, args.topic_id, args.partition_id);
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        info!("Validating if stream: {} exists..", stream_id);
        let stream = client.get_stream(&args.stream_id.try_into().unwrap()).await;
        if stream.is_ok() {
            info!("Stream: {} was found.", stream_id);
            break;
        }
        interval.tick().await;
    }
    loop {
        info!("Validating if topic: {} exists..", topic_id);
        let topic = client
            .get_topic(
                &stream_id.try_into().unwrap(),
                &topic_id.try_into().unwrap(),
            )
            .await;
        if topic.is_err() {
            interval.tick().await;
            continue;
        }

        info!("Topic: {} was found.", topic_id);
        let topic = topic.unwrap();
        if topic.partitions_count >= partition_id {
            break;
        }

        panic!(
            "Topic: {} has only {} partition(s), but partition: {} was requested.",
            topic_id, topic.partitions_count, partition_id
        );
    }
}

pub async fn init_by_producer(args: &Args, client: &dyn Client) -> Result<(), IggyError> {
    let stream = client.get_stream(&args.stream_id.try_into()?).await;
    if stream.is_ok() {
        return Ok(());
    }

    info!("Stream does not exist, creating...");
    client.create_stream("sample", Some(args.stream_id)).await?;
    client
        .create_topic(
            &args.stream_id.try_into()?,
            "orders",
            args.partitions_count,
            CompressionAlgorithm::from_code(args.compression_algorithm)?,
            None,
            Some(args.topic_id),
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await?;
    Ok(())
}

pub async fn consume_messages(
    args: &Args,
    client: &dyn Client,
    handle_message: &MessageHandler,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Messages will be polled by consumer: {} from stream: {}, topic: {}, partition: {} with interval {} ms.",
        args.consumer_id, args.stream_id, args.topic_id, args.partition_id, args.interval);

    let mut interval = tokio::time::interval(std::time::Duration::from_millis(args.interval));
    let mut consumed_batches = 0;
    let consumer = Consumer {
        kind: ConsumerKind::from_code(args.consumer_kind)?,
        id: Identifier::numeric(args.consumer_id).unwrap(),
    };

    loop {
        if args.message_batches_limit > 0 && consumed_batches == args.message_batches_limit {
            info!("Consumed {consumed_batches} batches of messages, exiting.");
            return Ok(());
        }

        let polled_messages = client
            .poll_messages(
                &args.stream_id.try_into()?,
                &args.topic_id.try_into()?,
                Some(args.partition_id),
                &consumer,
                &PollingStrategy::next(),
                args.messages_per_batch,
                true,
            )
            .await?;
        if polled_messages.messages.is_empty() {
            info!("No messages found.");
            interval.tick().await;
            continue;
        }
        consumed_batches += 1;
        for message in polled_messages.messages {
            handle_message(&message)?;
        }
        interval.tick().await;
    }
}

pub async fn consume_messages_iter(
    args: &Args,
    client: &IggyClient,
    handle_message: &MessageHandler,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Messages will be polled by consumer: {} from stream: {}, topic: {}, partition: {} with interval {} ms.",
        args.consumer_id, args.stream_id, args.topic_id, args.partition_id, args.interval);

    let mut interval = tokio::time::interval(std::time::Duration::from_millis(args.interval));
    let mut consumed_batches = 0;

    let mut consumer = match ConsumerKind::from_code(args.consumer_kind)? {
        ConsumerKind::Consumer => client.consumer().standalone(
            args.consumer_id.try_into()?,
            args.stream_id.try_into()?,
            args.topic_id.try_into()?,
            args.partition_id,
            PollingStrategy::next(),
            args.messages_per_batch,
            true,
        ),
        ConsumerKind::ConsumerGroup => client.consumer().group(
            args.consumer_id.try_into()?,
            args.stream_id.try_into()?,
            args.topic_id.try_into()?,
            PollingStrategy::next(),
            args.messages_per_batch,
            true,
        ),
    };

    while let Some(message) = consumer.next().await {
        if args.message_batches_limit > 0 && consumed_batches == args.message_batches_limit {
            info!("Consumed {consumed_batches} batches of messages, exiting.");
            return Ok(());
        }

        handle_message(&message?)?;
        consumed_batches += 1;
        interval.tick().await;
    }
    Ok(())
}
