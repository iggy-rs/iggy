use crate::error::Error;
use crate::partition::Partition;
use crate::stream::{Stream, StreamDetails};
use crate::topic::{Topic, TopicDetails};
use std::str::from_utf8;

pub fn map_streams(payload: &[u8]) -> Result<Vec<Stream>, Error> {
    let mut streams = Vec::new();
    let length = payload.len();
    let mut position = 0;
    while position < length {
        let (stream, read_bytes) = map_to_stream(payload, position)?;
        streams.push(stream);
        position += read_bytes;
        if position >= length {
            break;
        }
    }
    Ok(streams)
}

pub fn map_stream(payload: &[u8]) -> Result<StreamDetails, Error> {
    let (stream, mut position) = map_to_stream(payload, 0)?;
    let mut topics = Vec::new();
    let length = payload.len();
    while position < length {
        let (topic, read_bytes) = map_to_topic(payload, position)?;
        topics.push(topic);
        position += read_bytes;
        if position >= length {
            break;
        }
    }

    topics.sort_by(|x, y| x.id.cmp(&y.id));
    let stream = StreamDetails {
        id: stream.id,
        topics_count: stream.topics_count,
        name: stream.name,
        topics,
    };
    Ok(stream)
}

fn map_to_stream(payload: &[u8], position: usize) -> Result<(Stream, usize), Error> {
    let id = u32::from_le_bytes(payload[position..position + 4].try_into()?);
    let topics_count = u32::from_le_bytes(payload[position + 4..position + 8].try_into()?);
    let name_length = u32::from_le_bytes(payload[position + 8..position + 12].try_into()?) as usize;
    let name = from_utf8(&payload[position + 12..position + 12 + name_length])?.to_string();
    let read_bytes = 4 + 4 + 4 + name_length;
    Ok((
        Stream {
            id,
            topics_count,
            name,
        },
        read_bytes,
    ))
}

pub fn map_topics(payload: &[u8]) -> Result<Vec<Topic>, Error> {
    let mut topics = Vec::new();
    let length = payload.len();
    let mut position = 0;
    while position < length {
        let (topic, read_bytes) = map_to_topic(payload, position)?;
        topics.push(topic);
        position += read_bytes;
        if position >= length {
            break;
        }
    }
    Ok(topics)
}

pub fn map_topic(payload: &[u8]) -> Result<TopicDetails, Error> {
    let (topic, mut position) = map_to_stream(payload, 0)?;
    let mut partitions = Vec::new();
    let length = payload.len();
    while position < length {
        let (partition, read_bytes) = map_to_partition(payload, position)?;
        partitions.push(partition);
        position += read_bytes;
        if position >= length {
            break;
        }
    }

    partitions.sort_by(|x, y| x.id.cmp(&y.id));
    let topic = TopicDetails {
        id: topic.id,
        name: topic.name,
        partitions_count: partitions.len() as u32,
        partitions,
    };
    Ok(topic)
}

fn map_to_topic(payload: &[u8], position: usize) -> Result<(Topic, usize), Error> {
    let id = u32::from_le_bytes(payload[position..position + 4].try_into()?);
    let partitions_count = u32::from_le_bytes(payload[position + 4..position + 8].try_into()?);
    let name_length = u32::from_le_bytes(payload[position + 8..position + 12].try_into()?) as usize;
    let name = from_utf8(&payload[position + 12..position + 12 + name_length])?.to_string();
    let read_bytes = 4 + 4 + 4 + name_length;
    Ok((
        Topic {
            id,
            partitions_count,
            name,
        },
        read_bytes,
    ))
}

fn map_to_partition(payload: &[u8], position: usize) -> Result<(Partition, usize), Error> {
    let id = u32::from_le_bytes(payload[position..position + 4].try_into()?);
    let segments_count = u32::from_le_bytes(payload[position + 4..position + 8].try_into()?);
    let current_offset = u64::from_le_bytes(payload[position + 8..position + 16].try_into()?);
    let size_bytes = u64::from_le_bytes(payload[position + 16..position + 24].try_into()?);
    let read_bytes = 4 + 4 + 8 + 8;
    Ok((
        Partition {
            id,
            segments_count,
            current_offset,
            size_bytes,
        },
        read_bytes,
    ))
}
