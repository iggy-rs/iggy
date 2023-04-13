use crate::config::PartitionConfig;
use crate::message::Message;
use crate::segments;
use crate::segments::segment::{Segment, LOG_EXTENSION};
use crate::stream_error::StreamError;
use std::sync::Arc;
use tokio::fs;
use tracing::info;

#[derive(Debug)]
pub struct Partition {
    pub id: u32,
    pub path: String,
    segments: Vec<Segment>,
    config: Arc<PartitionConfig>,
}

impl Partition {
    pub fn create(
        id: u32,
        path: String,
        with_segment: bool,
        config: Arc<PartitionConfig>,
    ) -> Partition {
        let mut partition = Partition {
            id,
            path,
            segments: vec![],
            config,
        };

        if with_segment {
            let segment = Segment::create(id, 0, &partition.path, partition.config.segment.clone());
            partition.segments.push(segment);
        }

        partition
    }

    pub fn empty(config: Arc<PartitionConfig>) -> Partition {
        Partition::create(0, "".to_string(), false, config)
    }

    pub fn get_segments(&self) -> &Vec<Segment> {
        &self.segments
    }

    pub fn get_segments_mut(&mut self) -> &mut Vec<Segment> {
        &mut self.segments
    }

    pub async fn save_on_disk(&mut self) -> Result<(), StreamError> {
        for segment in self.get_segments_mut() {
            info!("Saving segment with start offset: {}", segment.start_offset);
            segment.save_on_disk().await?;
        }

        Ok(())
    }

    pub fn get_messages(&self, offset: u64, count: u32) -> Option<Vec<&Message>> {
        if self.segments.is_empty() {
            return None;
        }

        let mut end_offset = offset + (count - 1) as u64;
        let max_offset = self.segments.last().unwrap().current_offset;
        if end_offset > max_offset {
            end_offset = max_offset;
        }

        let segments = self
            .segments
            .iter()
            .filter(|segment| {
                (segment.start_offset >= offset && segment.current_offset <= end_offset)
                    || (segment.start_offset <= offset && segment.current_offset >= offset)
                    || (segment.start_offset <= end_offset && segment.current_offset >= end_offset)
            })
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

    pub async fn append_messages(&mut self, message: Message) -> Result<(), StreamError> {
        let segment = self.segments.last_mut();
        if segment.is_none() {
            return Err(StreamError::SegmentNotFound);
        }

        let segment = segment.unwrap();
        if segment.is_full() {
            info!(
                "Current segment is full, creating new segment for partition with ID: {}",
                self.id
            );
            let start_offset = segment.end_offset + 1;
            let mut new_segment = Segment::create(
                self.id,
                start_offset,
                &self.path,
                self.config.segment.clone(),
            );
            new_segment.save_on_disk().await?;
            self.segments.push(new_segment);
            self.segments
                .sort_by(|a, b| a.start_offset.cmp(&b.start_offset));
        }

        let segment = self.segments.last_mut();
        segment.unwrap().append_messages(message).await
    }

    pub async fn load_from_disk(
        id: u32,
        path: &str,
        config: Arc<PartitionConfig>,
    ) -> Result<Partition, StreamError> {
        info!(
            "Loading partition with ID: {} from disk, for path: {}",
            id, path
        );
        let dir_files = fs::read_dir(&path).await;
        let mut dir_files = dir_files.unwrap();
        let mut partition = Partition::create(id, path.to_string(), false, config);
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

            let log_file_name = dir_entry
                .file_name()
                .into_string()
                .unwrap()
                .replace(&format!(".{}", LOG_EXTENSION), "");
            let offset = log_file_name.parse::<u64>().unwrap();
            partition.segments.push(
                segments::segment_file::load_from_disk(
                    id,
                    offset,
                    &partition.path,
                    partition.config.segment.clone(),
                )
                .await?,
            );
        }

        partition
            .segments
            .sort_by(|a, b| a.start_offset.cmp(&b.start_offset));

        let end_offsets = partition
            .segments
            .iter()
            .skip(1)
            .map(|segment| segment.start_offset - 1)
            .collect::<Vec<u64>>();

        let segments_count = partition.segments.len();
        for (end_offset_index, segment) in partition.get_segments_mut().iter_mut().enumerate() {
            if end_offset_index == segments_count - 1 {
                break;
            }

            segment.end_offset = end_offsets[end_offset_index];
        }

        let last_segment = partition.segments.last_mut().unwrap();
        if last_segment.is_full() {
            last_segment.end_offset = last_segment.current_offset;
        }

        Ok(partition)
    }
}
