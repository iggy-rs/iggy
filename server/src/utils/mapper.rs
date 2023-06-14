use sdk::stream::StreamDetails;
use sdk::topic::TopicDetails;
use streaming::streams::stream::Stream;
use streaming::topics::topic::Topic;

pub fn map_stream(stream: &Stream) -> StreamDetails {
    let mut stream_details = StreamDetails {
        id: stream.id,
        name: stream.name.clone(),
        topics_count: stream.get_topics().len() as u32,
        topics: stream
            .get_topics()
            .iter()
            .map(|topic| sdk::topic::Topic {
                id: topic.id,
                name: topic.name.clone(),
                partitions_count: topic.get_partitions().len() as u32,
            })
            .collect(),
    };
    stream_details.topics.sort_by(|a, b| a.id.cmp(&b.id));
    stream_details
}

pub fn map_streams(streams: &[&Stream]) -> Vec<sdk::stream::Stream> {
    let mut streams = streams
        .iter()
        .map(|stream| sdk::stream::Stream {
            id: stream.id,
            name: stream.name.clone(),
            topics_count: stream.get_topics().len() as u32,
        })
        .collect::<Vec<sdk::stream::Stream>>();
    streams.sort_by(|a, b| a.id.cmp(&b.id));
    streams
}

pub fn map_topics(topics: &[&Topic]) -> Vec<sdk::topic::Topic> {
    let mut topics = topics
        .iter()
        .map(|topic| sdk::topic::Topic {
            id: topic.id,
            name: topic.name.clone(),
            partitions_count: topic.get_partitions().len() as u32,
        })
        .collect::<Vec<sdk::topic::Topic>>();
    topics.sort_by(|a, b| a.id.cmp(&b.id));
    topics
}

pub async fn map_topic(topic: &Topic) -> TopicDetails {
    let mut topic_details = TopicDetails {
        id: topic.id,
        name: topic.name.clone(),
        partitions_count: topic.get_partitions().len() as u32,
        partitions: Vec::new(),
    };
    for partition in topic.get_partitions() {
        let partition = partition.read().await;
        topic_details.partitions.push(sdk::partition::Partition {
            id: partition.id,
            segments_count: partition.get_segments().len() as u32,
            current_offset: partition.current_offset,
            size_bytes: partition
                .get_segments()
                .iter()
                .map(|segment| segment.current_size_bytes as u64)
                .sum(),
        });
    }
    topic_details.partitions.sort_by(|a, b| a.id.cmp(&b.id));
    topic_details
}
