use crate::message::Message;
use crate::segments::segment::Segment;
use shared::error::Error;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{info, trace};

impl Segment {
    pub async fn load(&mut self) -> Result<(), Error> {
        info!(
            "Loading segment from disk for offset: {} and partition with ID: {}...",
            self.start_offset, self.partition_id
        );
        let mut messages = vec![];
        let mut log_file = Segment::open_file(&self.log_path, false).await;
        let mut index_file = Segment::open_file(&self.index_path, false).await;
        let mut time_index_file = Segment::open_file(&self.time_index_path, false).await;
        let mut offset_buffer = [0; 8];
        let mut timestamp_buffer = [0; 8];
        let mut length_buffer = [0; 8];

        info!(
            "Loading messages from segment log file for start offset: {} and partition ID: {}...",
            self.start_offset, self.partition_id
        );
        while log_file.read_exact(&mut offset_buffer).await.is_ok() {
            if log_file.read_exact(&mut timestamp_buffer).await.is_err() {
                return Err(Error::CannotReadMessageTimestamp);
            }

            if log_file.read_exact(&mut length_buffer).await.is_err() {
                return Err(Error::CannotReadMessageLength);
            }

            let length = u64::from_le_bytes(length_buffer);
            let mut payload = vec![0; length as usize];
            if log_file.read_exact(&mut payload).await.is_err() {
                return Err(Error::CannotReadMessagePayload);
            }

            let offset = u64::from_le_bytes(offset_buffer);
            let timestamp = u64::from_le_bytes(timestamp_buffer);
            let message = Message::create(offset, timestamp, payload);
            messages.push(message);
            self.current_offset = offset;
        }

        // TODO: Cleanup and refactor loading indexes from disk

        let index_file_len = index_file.metadata().await.unwrap().len();
        let index_file_buffer = &mut vec![0; index_file_len as usize];
        let _ = index_file.read(index_file_buffer).await.unwrap();
        let indexes = index_file_buffer
            .chunks(8)
            .map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap()))
            .collect::<Vec<u64>>();

        trace!("Indexes per offset: {:?}", indexes);

        let time_index_file_len = time_index_file.metadata().await.unwrap().len();
        let time_index_file_buffer = &mut vec![0; time_index_file_len as usize];
        let _ = time_index_file.read(time_index_file_buffer).await.unwrap();
        let timestamps = time_index_file_buffer
            .chunks(8)
            .map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap()))
            .collect::<Vec<u64>>();

        trace!("Timestamps per offset: {:?}", timestamps);

        self.messages = messages;
        self.should_increment_offset = self.current_offset > 0;
        self.current_size_bytes = log_file.metadata().await.unwrap().len();
        self.saved_bytes = self.current_size_bytes;

        info!(
            "Loaded {} bytes from segment log file with start offset {} and partition ID: {}.",
            self.current_size_bytes, self.start_offset, self.partition_id
        );

        Ok(())
    }

    pub async fn persist(&mut self) -> Result<(), Error> {
        info!("Saving segment with start offset: {}", self.start_offset);
        if File::create(&self.log_path).await.is_err() {
            return Err(Error::CannotCreatePartitionSegmentLogFile(
                self.log_path.clone(),
            ));
        }

        if File::create(&self.time_index_path).await.is_err() {
            return Err(Error::CannotCreatePartitionSegmentTimeIndexFile(
                self.log_path.clone(),
            ));
        }

        let index_file = File::create(&self.index_path).await;
        if index_file.is_err() {
            return Err(Error::CannotCreatePartitionSegmentIndexFile(
                self.log_path.clone(),
            ));
        }

        let mut index_file = index_file.unwrap();
        let zero_index = 0u64.to_le_bytes();
        if index_file.write_all(&zero_index).await.is_err() {
            return Err(Error::CannotSaveIndexToSegment);
        }

        info!(
            "Created partition segment log file for start offset: {} and partition with ID: {} and path: {}.",
            self.start_offset, self.partition_id, self.log_path
        );

        Ok(())
    }

    pub async fn open_file(path: &str, append: bool) -> File {
        OpenOptions::new()
            .read(true)
            .append(append)
            .open(path)
            .await
            .unwrap()
    }
}
