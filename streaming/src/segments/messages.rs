use crate::message::Message;
use crate::segments::index::{Index, IndexRange};
use crate::segments::segment::Segment;
use crate::segments::time_index::TimeIndex;
use crate::segments::*;
use crate::utils::file;
use shared::error::Error;
use std::sync::Arc;
use tracing::trace;

impl Segment {
    pub async fn get_messages(
        &self,
        mut offset: u64,
        count: u32,
    ) -> Result<Vec<Arc<Message>>, Error> {
        if offset < self.start_offset {
            offset = self.start_offset;
        }

        let mut end_offset = offset + (count - 1) as u64;
        if self.is_closed || end_offset > self.current_offset {
            end_offset = self.current_offset;
        }

        self.load_messages_from_disk(offset, end_offset).await
    }

    async fn load_messages_from_disk(
        &self,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<Vec<Arc<Message>>, Error> {
        trace!(
            "Loading messages from disk, segment start offset: {}, end offset: {}...",
            start_offset,
            end_offset
        );

        if let Some(indexes) = &self.indexes {
            let relative_start_offset = start_offset - self.start_offset;
            let relative_end_offset = end_offset - self.start_offset;
            let start_index = indexes.get(relative_start_offset as usize);
            let end_index = indexes.get(1 + relative_end_offset as usize);
            if start_index.is_some() {
                let start_position = start_index.unwrap().position;
                let end_position = match end_index {
                    Some(index) => index.position,
                    None => self.current_size_bytes,
                };

                let index_range = IndexRange {
                    start: Index {
                        relative_offset: relative_start_offset as u32,
                        position: start_position,
                    },
                    end: Index {
                        relative_offset: relative_end_offset as u32,
                        position: end_position,
                    },
                };

                return self.load_messages_from_segment_file(&index_range).await;
            }
        }

        let mut index_file = file::open_file(&self.index_path, false).await;
        let index_range = index::load_range(
            &mut index_file,
            self.start_offset,
            start_offset,
            end_offset,
            self.current_size_bytes,
        )
        .await?;

        self.load_messages_from_segment_file(&index_range).await
    }

    async fn load_messages_from_segment_file(
        &self,
        index_range: &IndexRange,
    ) -> Result<Vec<Arc<Message>>, Error> {
        let mut log_file = file::open_file(&self.log_path, false).await;
        let messages = log::load(&mut log_file, index_range).await?;
        trace!(
            "Loaded {} messages from disk, segment start offset: {}, end offset: {}.",
            messages.len(),
            self.start_offset,
            self.current_offset
        );

        Ok(messages)
    }

    pub async fn append_message(&mut self, message: Arc<Message>) -> Result<(), Error> {
        if self.is_closed {
            return Err(Error::SegmentClosed(self.start_offset, self.partition_id));
        }

        if self.unsaved_messages.is_none() {
            self.unsaved_messages = Some(Vec::new());
        }

        let relative_offset = (message.offset - self.start_offset) as u32;
        if let Some(indexes) = self.indexes.as_mut() {
            indexes.push(Index {
                relative_offset,
                position: self.current_size_bytes,
            });
        }

        if let Some(time_indexes) = self.time_indexes.as_mut() {
            time_indexes.push(TimeIndex {
                relative_offset,
                timestamp: message.timestamp,
            });
        }

        self.current_size_bytes += message.get_size_bytes();
        self.current_offset = message.offset;
        self.unsaved_messages.as_mut().unwrap().push(message);

        Ok(())
    }

    pub async fn persist_messages(&mut self) -> Result<(), Error> {
        if self.unsaved_messages.is_none() {
            return Ok(());
        }

        let unsaved_messages = self.unsaved_messages.as_ref().unwrap();
        if unsaved_messages.is_empty() {
            return Ok(());
        }

        trace!(
            "Saving {} messages on disk in segment with start offset: {} for partition with ID: {}...",
            unsaved_messages.len(),
            self.start_offset,
            self.partition_id
        );

        let mut log_file = file::open_file(&self.log_path, true).await;
        let mut index_file = file::open_file(&self.index_path, true).await;
        let mut time_index_file = file::open_file(&self.time_index_path, true).await;

        let saved_bytes = log::persist(&mut log_file, unsaved_messages).await?;
        let current_position = self.current_size_bytes - saved_bytes;
        index::persist(&mut index_file, current_position, unsaved_messages).await?;
        time_index::persist(&mut time_index_file, unsaved_messages).await?;

        trace!(
            "Saved {} messages on disk in segment with start offset: {} for partition with ID: {}, total bytes written: {}.",
            unsaved_messages.len(),
            self.start_offset,
            self.partition_id,
            saved_bytes
        );

        if self.is_full() {
            self.end_offset = self.current_offset;
            self.is_closed = true;
            self.unsaved_messages = None;
        } else {
            self.unsaved_messages.as_mut().unwrap().clear();
        }

        Ok(())
    }
}
