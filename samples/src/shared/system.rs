use crate::shared::args::Args;
use iggy::client::Client;
use iggy::error::Error;
use iggy::identifier::Identifier;
use iggy::streams::create_stream::CreateStream;
use iggy::streams::get_stream::GetStream;
use iggy::topics::create_topic::CreateTopic;
use iggy::topics::get_topic::GetTopic;
use tracing::info;

pub async fn init_by_consumer(args: &Args, client: &dyn Client) {
    let (stream_id, topic_id, partition_id) = (args.stream_id, args.topic_id, args.partition_id);
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        info!("Validating if stream: {} exists..", stream_id);
        let stream = client
            .get_stream(&GetStream {
                stream_id: Identifier::numeric(args.stream_id).unwrap(),
            })
            .await;
        if stream.is_ok() {
            info!("Stream: {} was found.", stream_id);
            break;
        }
        interval.tick().await;
    }
    loop {
        info!("Validating if topic: {} exists..", topic_id);
        let topic = client
            .get_topic(&GetTopic {
                stream_id: Identifier::numeric(stream_id).unwrap(),
                topic_id: Identifier::numeric(args.topic_id).unwrap(),
            })
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

pub async fn init_by_producer(args: &Args, client: &dyn Client) -> Result<(), Error> {
    let stream = client
        .get_stream(&GetStream {
            stream_id: Identifier::numeric(args.stream_id)?,
        })
        .await;
    if stream.is_ok() {
        return Ok(());
    }

    info!("Stream does not exist, creating...");
    client
        .create_stream(&CreateStream {
            stream_id: args.stream_id,
            name: "sample".to_string(),
        })
        .await?;
    client
        .create_topic(&CreateTopic {
            stream_id: Identifier::numeric(args.stream_id).unwrap(),
            topic_id: args.topic_id,
            partitions_count: args.partitions_count,
            name: "orders".to_string(),
            message_expiry: None,
        })
        .await?;
    Ok(())
}
