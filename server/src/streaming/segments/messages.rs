use crate::streaming::batching::batch_accumulator::BatchAccumulator;
use crate::streaming::batching::message_batch::{RetainedMessageBatch, RETAINED_BATCH_OVERHEAD};
use crate::streaming::io::buf::dma_buf::DmaBuf;
use crate::streaming::io::buf::IoBuf;
use crate::streaming::io::log::{LogReader, LogWriter};
use crate::streaming::io::stream::message_stream::RetainedMessageStream;
use crate::streaming::models::messages::RetainedMessage;
use crate::streaming::segments::index::{Index, IndexRange};
use crate::streaming::segments::segment::Segment;
use crate::streaming::sizeable::Sizeable;
use futures::{StreamExt, TryStreamExt};
use iggy::error::IggyError;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::sizeable::Sizeable;
use std::future;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::{error, info, trace};

const EMPTY_MESSAGES: Vec<RetainedMessage> = vec![];

impl Segment {
    pub fn get_messages_count(&self) -> u64 {
        if self.size_bytes == 0 {
            return 0;
        }

        self.current_offset - self.start_offset + 1
    }

    pub async fn get_messages(
        &self,
        mut offset: u64,
        count: u32,
    ) -> Result<Vec<Arc<RetainedMessage>>, IggyError> {
        if count == 0 {
            return Ok(EMPTY_MESSAGES.into_iter().map(Arc::new).collect());
        }

        if offset < self.start_offset {
            offset = self.start_offset;
        }

        let end_offset = offset + (count - 1) as u64;
        // In case that the partition messages buffer is disabled, we need to check the unsaved messages buffer
        if self.unsaved_messages.is_none() {
            return self.load_n_messages_from_file(offset, count).await;
        }

        let batch_accumulator = self.unsaved_messages.as_ref().unwrap();
        if batch_accumulator.is_empty() {
            return self.load_n_messages_from_file(offset, count).await;
        }

        let first_offset = batch_accumulator.batch_base_offset();
        if end_offset < first_offset {
            return self.load_n_messages_from_file(offset, count).await;
        }

        let last_offset = batch_accumulator.batch_max_offset();
        if offset >= first_offset && end_offset <= last_offset {
            return Ok(self.load_messages_from_unsaved_buffer(offset, end_offset));
        }

        // Can this be somehow improved? maybe with chain iterators
        let mut messages = self.load_n_messages_from_file(offset, count).await?;
        let mut buffered_messages = self.load_messages_from_unsaved_buffer(offset, last_offset);
        messages.append(&mut buffered_messages);

        Ok(messages)
    }

    pub async fn get_all_messages(&self) -> Result<Vec<Arc<RetainedMessage>>, IggyError> {
        self.get_messages(self.start_offset, self.get_messages_count() as u32)
            .await
    }

    pub async fn get_all_batches(&self) -> Result<Vec<RetainedMessageBatch>, IggyError> {
        self.storage
            .segment
            .load_message_batches(self, &IndexRange::max_range())
            .await
    }

    pub async fn get_newest_batches_by_size(
        &self,
        size_bytes: u64,
    ) -> Result<Vec<RetainedMessageBatch>, IggyError> {
        let messages = self
            .storage
            .segment
            .load_newest_batches_by_size(self, size_bytes)
            .await?;

        Ok(messages)
    }

    fn load_messages_from_unsaved_buffer(
        &self,
        start_offset: u64,
        end_offset: u64,
    ) -> Vec<Arc<RetainedMessage>> {
        let batch_accumulator = self.unsaved_messages.as_ref().unwrap();
        batch_accumulator.get_messages_by_offset(start_offset, end_offset)
    }

    async fn load_n_messages_from_file(
        &self,
        start_offset: u64,
        count: u32,
    ) -> Result<Vec<Arc<RetainedMessage>>, IggyError> {
        let end_offset = start_offset + (count - 1) as u64;
        if let Some(indices) = &self.indexes {
            let relative_start_offset = (start_offset - self.start_offset) as u32;
            let relative_end_offset = (end_offset - self.start_offset) as u32;
            let index_range = match self.load_highest_lower_bound_index(
                indices,
                relative_start_offset,
                relative_end_offset,
            ) {
                Ok(range) => range,
                Err(_) => {
                    error!(
                        "Cannot load messages from disk, index range not found: {} - {}.",
                        start_offset, end_offset
                    );
                    return Ok(EMPTY_MESSAGES.into_iter().map(Arc::new).collect());
                }
            };
            let start_position = index_range.start.position;
            return self
                .load_n_messages_from_disk(start_position, count, |msg| msg.offset >= start_offset)
                .await;
        }

        match self
            .storage
            .segment
            .load_index_range(self, start_offset, end_offset)
            .await?
        {
            Some(index_range) => {
                self.load_n_messages_from_disk(index_range.start.position, count, |msg| {
                    msg.offset >= start_offset
                })
                .await
            }
            None => Ok(EMPTY_MESSAGES.into_iter().map(Arc::new).collect()),
        }
    }

    pub async fn load_n_messages_from_disk<F>(
        &self,
        start_position: u32,
        count: u32,
        filter: F,
    ) -> Result<Vec<Arc<RetainedMessage>>, IggyError>
    where
        F: Fn(&RetainedMessage) -> bool,
    {
        let reader = self
            .log
            .read_blocks(start_position as _, self.size_bytes as u64)
            .into_async_read();
        let message_stream = RetainedMessageStream::new(reader, 4096);
        let messages = message_stream
            .try_filter(|msg| future::ready(filter(msg)))
            .take(count as _)
            .map_ok(Arc::new)
            .try_collect()
            .await?;

        Ok(messages)
    }

    pub async fn append_batch(
        &mut self,
        batch_size: IggyByteSize,
        messages_count: u32,
        batch: &[Arc<RetainedMessage>],
    ) -> Result<(), IggyError> {
        if self.is_closed {
            return Err(IggyError::SegmentClosed(
                self.start_offset,
                self.partition_id,
            ));
        }
        let messages_cap = self.config.partition.messages_required_to_save as usize;
        let batch_base_offset = batch.first().unwrap().offset;
        let batch_accumulator = self
            .unsaved_messages
            .get_or_insert_with(|| BatchAccumulator::new(batch_base_offset, messages_cap));
        batch_accumulator.append(batch_size, batch);
        let curr_offset = batch_accumulator.batch_max_offset();

        self.current_offset = curr_offset;
        self.size_bytes += batch_size;
        let batch_size = batch_size.as_bytes_u64();
        self.size_of_parent_stream
            .fetch_add(batch_size, Ordering::AcqRel);
        self.size_of_parent_topic
            .fetch_add(batch_size, Ordering::AcqRel);
        self.size_of_parent_partition
            .fetch_add(batch_size, Ordering::AcqRel);
        self.messages_count_of_parent_stream
            .fetch_add(messages_count as u64, Ordering::SeqCst);
        self.messages_count_of_parent_topic
            .fetch_add(messages_count as u64, Ordering::SeqCst);
        self.messages_count_of_parent_partition
            .fetch_add(messages_count as u64, Ordering::SeqCst);

        Ok(())
    }

    fn store_offset_and_timestamp_index_for_batch(
        &mut self,
        batch_last_offset: u64,
        batch_max_timestamp: u64,
    ) -> Index {
        let relative_offset = (batch_last_offset - self.start_offset) as u32;
        trace!("Storing index for relative_offset: {relative_offset}");
        let index = Index {
            offset: relative_offset,
            position: self.last_index_position,
            timestamp: batch_max_timestamp,
        };
        self.indexes.as_mut().unwrap().push(index);
        index
    }

    pub async fn persist_messages(&mut self, fsync: bool) -> Result<usize, IggyError> {
        let sector_size = 4096;
        let index_storage = self.storage.segment.clone();
        if self.unsaved_messages.is_none() {
            return Ok(0);
        }

        let mut batch_accumulator = self.unsaved_messages.take().unwrap();
        if batch_accumulator.is_empty() {
            return Ok(0);
        }
        let batch_max_offset = batch_accumulator.batch_max_offset();
        let batch_max_timestamp = batch_accumulator.batch_max_timestamp();
        let index =
            self.store_offset_and_timestamp_index_for_batch(batch_max_offset, batch_max_timestamp);

        let unsaved_messages_number = batch_accumulator.unsaved_messages_count();
        trace!(
            "Saving {} messages on disk in segment with start offset: {} for partition with ID: {}...",
            unsaved_messages_number,
            self.start_offset,
            self.partition_id
        );

        let (has_remainder, batch) = batch_accumulator.materialize_batch_and_maybe_update_state();
        let batch_size = batch.get_size_bytes();
        let sectors = batch_size.div_ceil(sector_size);
        let adjusted_size = sector_size * sectors;
        if has_remainder {
            self.unsaved_messages = Some(batch_accumulator);
        }
        let mut bytes = DmaBuf::with_capacity(adjusted_size as usize);
        let diff = bytes.len() as u32 - batch_size;
        batch.extend2(bytes.as_mut());
        let saved_bytes = self.log.write_block(bytes).await?;
        index_storage.save_index(&self.index_path, index).await?;
        self.last_index_position += adjusted_size;
        let size_increment = RETAINED_BATCH_OVERHEAD + diff;
        self.size_bytes += size_increment;
        self.size_of_parent_stream
            .fetch_add(size_increment as u64, Ordering::AcqRel);
        self.size_of_parent_topic
            .fetch_add(size_increment as u64, Ordering::AcqRel);
        self.size_of_parent_partition
            .fetch_add(size_increment as u64, Ordering::AcqRel);

        trace!(
            "Saved {} messages on disk in segment with start offset: {} for partition with ID: {}, total bytes written: {}.",
            unsaved_messages_number,
            self.start_offset,
            self.partition_id,
            saved_bytes
        );

        if self.is_full().await {
            self.end_offset = self.current_offset;
            self.is_closed = true;
            info!(
                "Closed segment with start offset: {} for partition with ID: {}.",
                self.start_offset, self.partition_id
            );
        }
        Ok(unsaved_messages_number)
    }
}
