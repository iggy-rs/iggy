use std::{io::{SeekFrom, Write}, os::unix::fs::OpenOptionsExt};

use bytes::{BufMut, BytesMut};
use iggy::error::IggyError;
use tracing::warn;
use tokio::{fs::OpenOptions, io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader}};
use crate::streaming::batching::message_batch::{RetainedMessageBatch, RETAINED_BATCH_OVERHEAD};

#[derive(Debug, Default)]
pub struct DirectIOStorage {
}

impl DirectIOStorage {
    pub async fn read_batches(&self, file_path: &str, start_position: u64, end_offset: u64) -> Result<Vec<RetainedMessageBatch>, IggyError> {
        let file = OpenOptions::new().read(true).custom_flags(libc::O_DIRECT).open(file_path).await?;
        warn!("start_position: {}", start_position);

        let sector_size = 4096;
        let mut batches = Vec::new();
        let file_size = file.metadata().await?.len();
        if file_size == 0 {
            warn!("file_size is 0");
            return Ok(batches);
        }

        let mut reader = BufReader::with_capacity(4096 * 1000, file);
        reader
            .seek(SeekFrom::Start(start_position as u64))
            .await?;

        let mut read_bytes = start_position as u64;
        let mut last_batch_to_read = false;
        while !last_batch_to_read {
            let Ok(batch_base_offset) = reader.read_u64_le().await else {
                break;
            };
            let batch_length = reader
                .read_u32_le()
                .await
                .map_err(|_| IggyError::CannotReadBatchLength)?;
            let last_offset_delta = reader
                .read_u32_le()
                .await
                .map_err(|_| IggyError::CannotReadLastOffsetDelta)?;
            let max_timestamp = reader
                .read_u64_le()
                .await
                .map_err(|_| IggyError::CannotReadMaxTimestamp)?;

            let last_offset = batch_base_offset + (last_offset_delta as u64);
            let total_batch_size = batch_length + RETAINED_BATCH_OVERHEAD;
            let sectors = total_batch_size.div_ceil(sector_size);
            let adjusted_size = sector_size * sectors;
            warn!("adjusted_size: {}", adjusted_size);
            let diff = adjusted_size - total_batch_size;

            let payload_len = batch_length as usize;
            let mut payload = BytesMut::with_capacity(payload_len);
            payload.put_bytes(0, payload_len);
            if let Err(error) = reader.read_exact(&mut payload).await {
                warn!(
                    "Cannot read batch payload for batch with base offset: {batch_base_offset}, last offset delta: {last_offset_delta}, max timestamp: {max_timestamp}, batch length: {batch_length} and payload length: {payload_len}.\nProbably OS hasn't flushed the data yet, try setting `enforce_fsync = true` for partition configuration if this issue occurs again.\n{error}",
                );
                break;
            }
            // TEMP
            let mut temp = BytesMut::with_capacity(diff as _);
            temp.put_bytes(0, diff as _);
            if let Err(e) = reader.read_exact(&mut temp).await {
                warn!("lol error reading padding");
            }

            read_bytes += 8 + 4 + 4 + 8 + payload_len as u64;
            last_batch_to_read = read_bytes >= file_size || last_offset == end_offset;

            let batch = RetainedMessageBatch::new(
                batch_base_offset,
                last_offset_delta,
                max_timestamp,
                batch_length,
                payload.freeze(),
            );
            batches.push(batch);
        }
        Ok(batches)
    }

    pub async fn write_batches(&self, file_path: &str, bytes: &[u8]) -> Result<u32, IggyError> {
        //let mut std_file = std::fs::File::options().append(true).custom_flags(libc::O_DIRECT).open(file_path)?;
        let mut file = OpenOptions::new().append(true).custom_flags(libc::O_DIRECT).open(file_path).await?;
        if let Err(e) = file.write_all(bytes).await {
            warn!("error writing: {}", e);
        }
        Ok(bytes.len() as _)
    }
}
