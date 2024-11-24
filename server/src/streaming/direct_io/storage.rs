use std::{
    alloc::{self, Layout},
    io::{Read, Seek, SeekFrom, Write},
    os::unix::fs::OpenOptionsExt,
};

use crate::streaming::batching::message_batch::{RetainedMessageBatch, RETAINED_BATCH_OVERHEAD};
use bytes::{BufMut, Bytes, BytesMut};
use iggy::error::IggyError;
use tokio::{
    fs::OpenOptions,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader},
    task::spawn_blocking,
};
use tracing::warn;

#[derive(Debug, Default)]
pub struct DirectIOStorage {}

impl DirectIOStorage {
    pub async fn read_batches(
        &self,
        file_path: &str,
        start_position: u64,
        end_position: u64,
    ) -> Result<Vec<RetainedMessageBatch>, IggyError> {
        //let mut file = OpenOptions::new().read(true).custom_flags(libc::O_DIRECT).open(file_path).await?;
        let mut file = std::fs::File::options()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(file_path)?;
        file.seek(SeekFrom::Start(start_position))?;
        let mut batches = Vec::new();
        let file_size = file.metadata()?.len();
        if file_size == 0 {
            return Ok(batches);
        }
        // Aloc the buf
        let buf_size = if start_position == end_position {
            file_size - start_position
        } else {
            end_position - start_position
        };
        let sector_size = 4096;
        let alignment = buf_size % sector_size;
        assert!(alignment == 0);

        let layout = Layout::from_size_align(buf_size as _, sector_size as _).unwrap();
        let ptr = unsafe { alloc::alloc(layout) };
        // Not sure if this is required
        let mut bytes = unsafe { Vec::from_raw_parts(ptr, buf_size as _, buf_size as _) };
        let result = spawn_blocking(move || {
            if let Err(e) = file.read_exact(&mut bytes) {
                warn!("error reading batch: {}", e);
            }
            Self::serialize_batches(bytes, &mut batches);
            Ok(batches)
        })
        .await
        .unwrap();
        result
    }

    fn serialize_batches(bytes: Vec<u8>, batches: &mut Vec<RetainedMessageBatch>) {
        let len = bytes.len();
        let mut read_bytes = 0;
        let sector_size = 4096;

        while read_bytes < len {
            // Read batch_base_offset
            let batch_base_offset = u64::from_le_bytes(
                bytes[read_bytes..read_bytes + 8]
                    .try_into()
                    .expect("Failed to read batch_base_offset"),
            );
            read_bytes += 8;

            // Read batch_length
            let batch_length = u32::from_le_bytes(
                bytes[read_bytes..read_bytes + 4]
                    .try_into()
                    .expect("Failed to read batch_length"),
            );
            read_bytes += 4;

            // Read last_offset_delta
            let last_offset_delta = u32::from_le_bytes(
                bytes[read_bytes..read_bytes + 4]
                    .try_into()
                    .expect("Failed to read last_offset_delta"),
            );
            read_bytes += 4;

            // Read max_timestamp
            let max_timestamp = u64::from_le_bytes(
                bytes[read_bytes..read_bytes + 8]
                    .try_into()
                    .expect("Failed to read max_timestamp"),
            );
            read_bytes += 8;

            // Calculate last_offset and other values
            let total_batch_size = batch_length + RETAINED_BATCH_OVERHEAD;
            let sectors = total_batch_size.div_ceil(sector_size);
            let adjusted_size = sector_size * sectors;
            let diff = adjusted_size - total_batch_size;

            // Read payload
            let payload_len = batch_length as usize;
            let payload_start = read_bytes;
            let payload_end = read_bytes + payload_len;
            if payload_end > len {
                warn!(
                    "Cannot read batch payload for batch with base offset: {batch_base_offset}, last offset delta: {last_offset_delta}, max timestamp: {max_timestamp}, batch length: {batch_length} and payload length: {payload_len}.\nProbably OS hasn't flushed the data yet, try setting `enforce_fsync = true` for partition configuration if this issue occurs again."
                );
                break;
            }
            // Ergh....
            let payload = Bytes::copy_from_slice(&bytes[payload_start..payload_end]);
            read_bytes = payload_end + diff as usize;
            let batch = RetainedMessageBatch::new(
                batch_base_offset,
                last_offset_delta,
                max_timestamp,
                batch_length,
                payload,
            );
            batches.push(batch);
        }
    }

    pub async fn write_batches(&self, file_path: &str, bytes: Vec<u8>) -> Result<u32, IggyError> {
        let mut std_file = std::fs::File::options()
            .append(true)
            .custom_flags(libc::O_DIRECT)
            .open(file_path)?;
        //let mut file = OpenOptions::new().append(true).custom_flags(libc::O_DIRECT).open(file_path).await?;
        let size = bytes.len() as _;
        spawn_blocking(move || {
            if let Err(e) = std_file.write_all(&bytes) {
                warn!("error writing: {}", e);
            }
        })
        .await
        .unwrap();
        Ok(size)
    }
}
