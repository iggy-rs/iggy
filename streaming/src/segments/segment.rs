use crate::config::SegmentConfig;
use crate::message::Message;
use std::sync::Arc;
use tokio::fs::File;

pub const LOG_EXTENSION: &str = "log";
pub const INDEX_EXTENSION: &str = "index";
pub const TIME_INDEX_EXTENSION: &str = "timeindex";

#[derive(Debug)]
pub struct Segment {
    pub partition_id: u32,
    pub start_offset: u64,
    pub current_offset: u64,
    pub end_offset: u64,
    pub partition_path: String,
    pub index_path: String,
    pub log_path: String,
    pub time_index_path: String,
    pub messages: Vec<Message>,
    pub unsaved_messages_count: u64,
    pub current_size_bytes: u64,
    pub saved_bytes: u64,
    pub should_increment_offset: bool,
    pub log_file: Option<File>,
    pub index_file: Option<File>,
    pub time_index_file: Option<File>,
    pub config: Arc<SegmentConfig>,
}

impl Segment {
    pub fn create(
        partition_id: u32,
        start_offset: u64,
        partition_path: &str,
        config: Arc<SegmentConfig>,
    ) -> Segment {
        let index_path = format!(
            "{}/{:0>20}.{}",
            partition_path, start_offset, INDEX_EXTENSION
        );
        let time_index_path = format!(
            "{}/{:0>20}.{}",
            partition_path, start_offset, TIME_INDEX_EXTENSION
        );
        let log_path = format!("{}/{:0>20}.{}", partition_path, start_offset, LOG_EXTENSION);

        Segment {
            partition_id,
            start_offset,
            current_offset: start_offset,
            end_offset: 0,
            partition_path: partition_path.to_string(),
            index_path,
            time_index_path,
            log_path,
            messages: vec![],
            unsaved_messages_count: 0,
            current_size_bytes: 0,
            saved_bytes: 0,
            should_increment_offset: false,
            log_file: None,
            index_file: None,
            time_index_file: None,
            config,
        }
    }

    pub fn is_full(&self) -> bool {
        self.current_size_bytes >= self.config.size_bytes
    }
}
