use crate::error::Error;
use crate::message::Message;
use crate::segments::segment::Segment;
use crate::segments::*;
use crate::timestamp;
use tracing::info;

impl Segment {
    // TODO: Load messages from cache and if not found, load them from disk.
    pub fn get_messages(&self, offset: u64, count: u32) -> Option<Vec<&Message>> {
        let mut end_offset: u64;
        if self.is_full() {
            end_offset = offset + (count - 1) as u64;
            if end_offset > self.end_offset {
                end_offset = self.end_offset;
            }
        } else {
            end_offset = self.current_offset;
        }

        let messages = self
            .messages
            .iter()
            .filter(|message| message.offset >= offset && message.offset <= end_offset)
            .collect::<Vec<&Message>>();

        if messages.is_empty() {
            return None;
        }

        Some(messages)
    }

    pub async fn append_messages(&mut self, mut message: Message) -> Result<(), Error> {
        if self.is_full() {
            return Err(Error::SegmentFull(self.start_offset, self.partition_id));
        }

        info!(
            "Appending the message, current segment size is {} bytes",
            self.current_size_bytes
        );

        // Do not increment offset for the very first message
        if self.should_increment_offset {
            self.current_offset += 1;
        } else {
            self.should_increment_offset = true;
        }

        message.offset = self.current_offset;
        message.timestamp = timestamp::get();
        self.current_size_bytes += message.get_size_bytes();
        self.messages.push(message);
        self.unsaved_messages_count += 1;

        info!(
            "Appended the message, current segment size is {} bytes",
            self.current_size_bytes
        );

        if self.unsaved_messages_count >= self.config.messages_required_to_save || self.is_full() {
            self.persist_messages().await?;
        }

        if self.is_full() {
            self.end_offset = self.current_offset;
            self.set_read_only_mode().await;
        }

        Ok(())
    }

    pub async fn persist_messages(&mut self) -> Result<(), Error> {
        if self.log_file.is_none() {
            return Err(Error::LogFileNotFound);
        }

        if self.unsaved_messages_count == 0 {
            if !self.is_full() {
                info!(
                    "No existing messages to save on disk in segment {} for partition {}",
                    self.start_offset, self.partition_id
                );
            }
            return Ok(());
        }

        info!(
            "Saving {} messages on disk in segment {} for partition {}",
            self.unsaved_messages_count, self.start_offset, self.partition_id
        );

        let messages_count = self.messages.len();
        let messages =
            &self.messages[messages_count - self.unsaved_messages_count as usize..messages_count];
        let current_bytes = self.saved_bytes;

        let saved_bytes = log::persist(self.log_file.as_mut().unwrap(), messages).await?;
        index::persist(self.index_file.as_mut().unwrap(), current_bytes, messages).await?;
        time_index::persist(self.time_index_file.as_mut().unwrap(), messages).await?;

        info!(
            "Saved {} messages on disk in segment {} for partition {}, total bytes written: {}",
            self.unsaved_messages_count, self.start_offset, self.partition_id, saved_bytes
        );

        self.unsaved_messages_count = 0;
        self.saved_bytes += saved_bytes;
        if self.is_full() {
            self.set_read_only_mode().await;
        }

        Ok(())
    }
}
