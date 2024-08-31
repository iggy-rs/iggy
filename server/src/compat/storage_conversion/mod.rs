mod converter;
mod persistency;

use crate::compat::storage_conversion::persistency::{personal_access_tokens, streams, users};
use crate::configs::system::SystemConfig;
use crate::state::system::{PartitionState, StreamState, TopicState};
use crate::state::State;
use crate::streaming::batching::message_batch::RetainedMessageBatch;
use crate::streaming::partitions::partition::{ConsumerOffset, Partition};
use crate::streaming::persistence::persister::Persister;
use crate::streaming::segments::index::{Index, IndexRange};
use crate::streaming::segments::segment::Segment;
use crate::streaming::segments::time_index::TimeIndex;
use crate::streaming::storage::{
    PartitionStorage, SegmentStorage, StreamStorage, SystemInfoStorage, SystemStorage, TopicStorage,
};
use crate::streaming::streams::stream::Stream;
use crate::streaming::systems::info::SystemInfo;
use crate::streaming::topics::topic::Topic;
use async_trait::async_trait;
use iggy::consumer::ConsumerKind;
use iggy::error::IggyError;
use std::path::Path;
use std::sync::Arc;
use tokio::fs::{read_dir, rename};
use tracing::{error, info};

pub async fn init(
    config: Arc<SystemConfig>,
    metadata: Arc<dyn State>,
    storage: Arc<SystemStorage>,
) -> Result<(), IggyError> {
    if Path::new(&config.get_state_log_path()).exists() {
        info!("State log already exists, skipping storage migration");
        return Ok(());
    }

    let path = config.get_database_path();
    if path.is_none() {
        info!("No database path configured, skipping storage migration");
        return Ok(());
    }

    let database_path = path.unwrap();
    if !Path::new(&database_path).exists() {
        error!("Database directory: {database_path} does not exist - cannot migrate storage.");
        return Err(IggyError::CannotOpenDatabase(database_path));
    }

    let db_file = format!("{database_path}/db");
    if !Path::new(&db_file).exists() {
        error!("Database file at path: {db_file} does not exist - cannot migrate storage.");
        return Err(IggyError::CannotOpenDatabase(db_file));
    }

    info!("Starting storage migration, database path: {database_path}");
    let db = sled::open(&database_path);
    if db.is_err() {
        panic!("Cannot open database at: {database_path}");
    }
    let db = db.unwrap();
    let mut streams = Vec::new();
    let dir_entries = read_dir(&config.get_streams_path()).await;
    if let Err(error) = dir_entries {
        error!("Cannot read streams directory: {}", error);
        return Err(IggyError::CannotReadStreams);
    }

    let noop_storage = SystemStorage {
        info: Arc::new(NoopSystemInfoStorage {}),
        stream: Arc::new(NoopStreamStorage {}),
        topic: Arc::new(NoopTopicStorage {}),
        partition: Arc::new(NoopPartitionStorage {}),
        segment: Arc::new(NoopSegmentStorage {}),
        persister: Arc::new(NoopPersister {}),
    };
    let noop_storage = Arc::new(noop_storage);
    let mut dir_entries = dir_entries.unwrap();
    while let Some(dir_entry) = dir_entries.next_entry().await.unwrap_or(None) {
        let name = dir_entry.file_name().into_string().unwrap();
        let stream_id = name.parse::<u32>();
        if stream_id.is_err() {
            error!("Invalid stream ID file with name: '{}'.", name);
            continue;
        }

        let stream_id = stream_id.unwrap();
        let mut stream = Stream::empty(stream_id, "stream", config.clone(), noop_storage.clone());
        streams::load(&config, &db, &mut stream).await?;
        streams.push(stream);
    }

    let users = users::load_all(&db).await?;
    let personal_access_tokens = personal_access_tokens::load_all(&db).await?;
    converter::convert(metadata, storage, streams, users, personal_access_tokens).await?;
    let old_database_path = format!("{database_path}_old");
    rename(&database_path, &old_database_path).await?;
    info!("Storage migration has completed, new state log was cacreated and old database was moved to: {old_database_path} (now it can be safely deleted).");
    Ok(())
}

struct NoopPersister {}
struct NoopSystemInfoStorage {}
struct NoopStreamStorage {}
struct NoopTopicStorage {}
struct NoopPartitionStorage {}
struct NoopSegmentStorage {}

#[async_trait]
impl Persister for NoopPersister {
    async fn append(&self, _path: &str, _bytes: &[u8]) -> Result<(), IggyError> {
        Ok(())
    }

    async fn overwrite(&self, _path: &str, _bytes: &[u8]) -> Result<(), IggyError> {
        Ok(())
    }

    async fn delete(&self, _path: &str) -> Result<(), IggyError> {
        Ok(())
    }
}

#[async_trait]
impl SystemInfoStorage for NoopSystemInfoStorage {
    async fn load(&self) -> Result<SystemInfo, IggyError> {
        Ok(SystemInfo::default())
    }

    async fn save(&self, _system_info: &SystemInfo) -> Result<(), IggyError> {
        Ok(())
    }
}

#[async_trait]
impl StreamStorage for NoopStreamStorage {
    async fn load(&self, _stream: &mut Stream, _state: StreamState) -> Result<(), IggyError> {
        Ok(())
    }

    async fn save(&self, _stream: &Stream) -> Result<(), IggyError> {
        Ok(())
    }

    async fn delete(&self, _stream: &Stream) -> Result<(), IggyError> {
        Ok(())
    }
}

#[async_trait]
impl TopicStorage for NoopTopicStorage {
    async fn load(&self, _topic: &mut Topic, _state: TopicState) -> Result<(), IggyError> {
        Ok(())
    }

    async fn save(&self, _topic: &Topic) -> Result<(), IggyError> {
        Ok(())
    }

    async fn delete(&self, _topic: &Topic) -> Result<(), IggyError> {
        Ok(())
    }
}

#[async_trait]
impl PartitionStorage for NoopPartitionStorage {
    async fn load(
        &self,
        _partition: &mut Partition,
        _state: PartitionState,
    ) -> Result<(), IggyError> {
        Ok(())
    }

    async fn save(&self, _partition: &Partition) -> Result<(), IggyError> {
        Ok(())
    }

    async fn delete(&self, _partition: &Partition) -> Result<(), IggyError> {
        Ok(())
    }

    async fn save_consumer_offset(&self, _offset: &ConsumerOffset) -> Result<(), IggyError> {
        Ok(())
    }

    async fn load_consumer_offsets(
        &self,
        _kind: ConsumerKind,
        _path: &str,
    ) -> Result<Vec<ConsumerOffset>, IggyError> {
        Ok(vec![])
    }

    async fn delete_consumer_offsets(&self, _path: &str) -> Result<(), IggyError> {
        Ok(())
    }

    async fn delete_consumer_offset(&self, _path: &str) -> Result<(), IggyError> {
        Ok(())
    }
}

#[async_trait]
impl SegmentStorage for NoopSegmentStorage {
    async fn load(&self, _segment: &mut Segment) -> Result<(), IggyError> {
        Ok(())
    }

    async fn save(&self, _segment: &Segment) -> Result<(), IggyError> {
        Ok(())
    }

    async fn delete(&self, _segment: &Segment) -> Result<(), IggyError> {
        Ok(())
    }

    async fn load_message_batches(
        &self,
        _segment: &Segment,
        _index_range: &IndexRange,
    ) -> Result<Vec<RetainedMessageBatch>, IggyError> {
        Ok(vec![])
    }

    async fn load_newest_batches_by_size(
        &self,
        _segment: &Segment,
        _size: u64,
    ) -> Result<Vec<RetainedMessageBatch>, IggyError> {
        Ok(vec![])
    }

    async fn save_batches(
        &self,
        _segment: &Segment,
        _batch: RetainedMessageBatch,
    ) -> Result<u32, IggyError> {
        Ok(0)
    }

    async fn load_message_ids(&self, _segment: &Segment) -> Result<Vec<u128>, IggyError> {
        Ok(vec![])
    }

    async fn load_checksums(&self, _segment: &Segment) -> Result<(), IggyError> {
        Ok(())
    }

    async fn load_all_indexes(&self, _segment: &Segment) -> Result<Vec<Index>, IggyError> {
        Ok(vec![])
    }

    async fn load_index_range(
        &self,
        _segment: &Segment,
        _index_start_offset: u64,
        _index_end_offset: u64,
    ) -> Result<Option<IndexRange>, IggyError> {
        Ok(None)
    }

    async fn save_index(&self, _index_path: &str, _index: Index) -> Result<(), IggyError> {
        Ok(())
    }

    async fn try_load_time_index_for_timestamp(
        &self,
        _segment: &Segment,
        _timestamp: u64,
    ) -> Result<Option<TimeIndex>, IggyError> {
        Ok(None)
    }

    async fn load_all_time_indexes(&self, _segment: &Segment) -> Result<Vec<TimeIndex>, IggyError> {
        Ok(vec![])
    }

    async fn load_last_time_index(
        &self,
        _segment: &Segment,
    ) -> Result<Option<TimeIndex>, IggyError> {
        Ok(None)
    }

    async fn save_time_index(&self, _index_path: &str, _index: TimeIndex) -> Result<(), IggyError> {
        Ok(())
    }
}
