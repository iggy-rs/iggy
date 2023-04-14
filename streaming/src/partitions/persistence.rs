use crate::error::Error;
use crate::partitions::partition::Partition;
use crate::segments::segment::{Segment, LOG_EXTENSION};
use tokio::fs;
use tracing::info;

impl Partition {
    pub async fn persist(&mut self) -> Result<(), Error> {
        for segment in self.get_segments_mut() {
            segment.persist().await?;
        }

        Ok(())
    }

    pub async fn load(&mut self) -> Result<(), Error> {
        info!(
            "Loading partition with ID: {} from disk, for path: {}",
            self.id, self.path
        );
        let dir_files = fs::read_dir(&self.path).await;
        let mut dir_files = dir_files.unwrap();
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

            let mut segment =
                Segment::create(self.id, offset, &self.path, self.config.segment.clone());
            segment.load().await?;
            self.segments.push(segment);
        }

        self.segments
            .sort_by(|a, b| a.start_offset.cmp(&b.start_offset));

        let end_offsets = self
            .segments
            .iter()
            .skip(1)
            .map(|segment| segment.start_offset - 1)
            .collect::<Vec<u64>>();

        let segments_count = self.segments.len();
        for (end_offset_index, segment) in self.get_segments_mut().iter_mut().enumerate() {
            if end_offset_index == segments_count - 1 {
                break;
            }

            segment.end_offset = end_offsets[end_offset_index];
        }

        let last_segment = self.segments.last_mut().unwrap();
        if last_segment.is_full() {
            last_segment.end_offset = last_segment.current_offset;
        }

        Ok(())
    }
}
