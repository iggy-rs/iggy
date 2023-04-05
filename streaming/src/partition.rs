use tokio::fs;
use tracing::info;
use crate::message::Message;
use crate::segment::{LOG_EXTENSION, Segment, SEGMENT_SIZE};
use crate::stream_error::StreamError;

#[derive(Debug)]
pub struct Partition {
    pub id: u32,
    pub path: String,
    pub segments: Vec<Segment>
}

impl Partition {
    pub fn create(id: u32, path: String, with_segment: bool) -> Partition {
        let mut partition = Partition {
            id,
            path,
            segments: vec![]
        };

        if with_segment {
            let segment = Segment::create(0, SEGMENT_SIZE, &partition.path);
            partition.segments.push(segment);
        }

        partition
    }

    pub fn empty() -> Partition {
        Partition::create(0, "".to_string(), false)
    }

    pub async fn save_on_disk(&self) -> Result<(), StreamError> {
        for segment in &self.segments {
            info!("Saving segment with start offset: {}", segment.start_offset);
            segment.save_on_disk().await?;
        }

        Ok(())
    }

    pub fn get_messages(&self, offset: u64, count: u32) -> Option<Vec<&Message>> {
        let mut end_offset = offset + (count - 1) as u64;
        let max_offset = self.segments.last().unwrap().end_offset;
        if end_offset > max_offset {
            end_offset = max_offset;
        }

        let segments = self.segments.iter()
            .filter(|segment| (segment.start_offset >= offset && segment.end_offset <= end_offset)
                || (segment.start_offset <= offset && segment.end_offset >= offset)
                || (segment.start_offset <= end_offset && segment.end_offset >= end_offset))
            .collect::<Vec<&Segment>>();

        if segments.is_empty() {
            return None;
        }

        let mut messages = Vec::new();
        for segment in segments {
            let segment_messages = segment.get_messages(offset, count);
            if segment_messages.is_none() {
                continue;
            }

            for message in segment_messages.unwrap() {
                messages.push(message);
            }
        }

        Some(messages)
    }

    pub async fn append_message(&mut self, message: Message) -> Result<(), StreamError> {
        let segment = self.segments.last_mut();
        if segment.is_none() {
            return Err(StreamError::SegmentNotFound);
        }

        let segment = segment.unwrap();
        if segment.is_full() {
            info!("Current segment is full, creating new segment for partition with ID: {}", self.id);
            let start_offset = segment.end_offset + 1;
            let new_segment = Segment::create(start_offset, SEGMENT_SIZE, &self.path);
            new_segment.save_on_disk().await?;
            self.segments.push(new_segment);
            self.segments.sort_by(|a, b| a.start_offset.cmp(&b.start_offset));
        }

        let segment = self.segments.last_mut();
        segment.unwrap().append_message(message).await
    }

    pub async fn load_from_disk(id: u32, path: &str) -> Result<Partition, StreamError> {
        info!("Loading partition with ID: {} from disk, for path: {}", id, path);
        let dir_files = fs::read_dir(&path).await;
        let mut dir_files = dir_files.unwrap();
        let mut partition = Partition::create(id, path.to_string(), false);
        loop {
            let dir_entry = dir_files.next_entry().await;
            if dir_entry.is_err() {
                break;
            }

            let dir_entry = dir_entry.unwrap();
            if dir_entry.is_none() {
                break;
            }

            let dir_entry = dir_entry.unwrap();
            if let Some(extension) = dir_entry.path().extension() {
                if extension != LOG_EXTENSION {
                    continue;
                }
            }

            let log_file_name = dir_entry.file_name().into_string().unwrap().replace(&format!(".{}", LOG_EXTENSION), "");
            let offset = log_file_name.parse::<u64>().unwrap();
            partition.segments.push(Segment::load_from_disk(offset, &partition.path).await?);
        }

        partition.segments.sort_by(|a, b| a.start_offset.cmp(&b.start_offset));

        Ok(partition)
    }
}