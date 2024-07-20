use super::batching::message_batch::RetainedMessageBatch;
use super::persistence::persister::PersistenceStorage;
use super::segments::index::{Index, IndexRange};
use crate::configs::system::SystemConfig;
use crate::state::system::{PartitionState, StreamState, TopicState};
use crate::streaming::partitions::partition::{ConsumerOffset, Partition};
use crate::streaming::partitions::storage::FilePartitionStorage;
use crate::streaming::segments::segment::Segment;
use crate::streaming::segments::storage::FileSegmentStorage;
use crate::streaming::segments::time_index::TimeIndex;
use crate::streaming::streams::storage::FileStreamStorage;
use crate::streaming::streams::stream::Stream;
use crate::streaming::topics::storage::FileTopicStorage;
use crate::streaming::topics::topic::Topic;
use crate::tpc::shard::info::SystemInfo;
use crate::tpc::shard::storage::FileSystemInfoStorage;
use iggy::consumer::ConsumerKind;
use iggy::error::IggyError;
use std::fmt::Debug;
use std::rc::Rc;
use std::sync::Arc;

pub trait SystemInfoStorage {
    async fn load(&self) -> Result<SystemInfo, IggyError>;
    async fn save(&self, system_info: &SystemInfo) -> Result<(), IggyError>;
}

pub trait StreamStorage {
    async fn load(&self, stream: &mut Stream, state: StreamState) -> Result<(), IggyError>;
    async fn save(&self, stream: &Stream) -> Result<(), IggyError>;
    async fn delete(&self, stream: &Stream) -> Result<(), IggyError>;
}

pub trait TopicStorage {
    async fn load(&self, topic: &mut Topic, state: TopicState) -> Result<(), IggyError>;
    async fn save(&self, topic: &Topic) -> Result<(), IggyError>;
    async fn delete(&self, topic: &Topic) -> Result<(), IggyError>;
}

pub trait PartitionStorage {
    async fn load(&self, partition: &mut Partition, state: PartitionState)
        -> Result<(), IggyError>;
    async fn save(&self, partition: &Partition) -> Result<(), IggyError>;
    async fn delete(&self, partition: &Partition) -> Result<(), IggyError>;
    async fn save_consumer_offset(&self, offset: &ConsumerOffset) -> Result<(), IggyError>;
    async fn load_consumer_offsets(
        &self,
        kind: ConsumerKind,
        path: &str,
    ) -> Result<Vec<ConsumerOffset>, IggyError>;
    async fn delete_consumer_offsets(&self, path: &str) -> Result<(), IggyError>;
    async fn delete_consumer_offset(&self, path: &str) -> Result<(), IggyError>;
}

pub trait SegmentStorage {
    async fn load(&self, segment: &mut Segment) -> Result<(), IggyError>;
    async fn save(&self, segment: &Segment) -> Result<(), IggyError>;
    async fn delete(&self, segment: &Segment) -> Result<(), IggyError>;
    async fn load_message_batches(
        &self,
        segment: &Segment,
        index_range: &IndexRange,
    ) -> Result<Vec<RetainedMessageBatch>, IggyError>;
    async fn load_newest_batches_by_size(
        &self,
        segment: &Segment,
        size_bytes: u64,
    ) -> Result<Vec<RetainedMessageBatch>, IggyError>;
    async fn save_batches(
        &self,
        segment: &Segment,
        batches: &[Rc<RetainedMessageBatch>],
    ) -> Result<u32, IggyError>;
    async fn load_message_ids(&self, segment: &Segment) -> Result<Vec<u128>, IggyError>;
    async fn load_checksums(&self, segment: &Segment) -> Result<(), IggyError>;
    async fn load_all_indexes(&self, segment: &Segment) -> Result<Vec<Index>, IggyError>;
    async fn load_index_range(
        &self,
        segment: &Segment,
        index_start_offset: u64,
        index_end_offset: u64,
    ) -> Result<Option<IndexRange>, IggyError>;
    async fn save_indexes(&self, segment: &Segment) -> Result<(), IggyError>;
    async fn try_load_time_index_for_timestamp(
        &self,
        segment: &Segment,
        timestamp: u64,
    ) -> Result<Option<TimeIndex>, IggyError>;
    async fn load_all_time_indexes(&self, segment: &Segment) -> Result<Vec<TimeIndex>, IggyError>;
    async fn load_last_time_index(&self, segment: &Segment)
        -> Result<Option<TimeIndex>, IggyError>;
    async fn save_time_indexes(&self, segment: &Segment) -> Result<(), IggyError>;
}

#[derive(Debug)]
pub struct SystemStorage {
    pub info: Rc<FileSystemInfoStorage>,
    pub stream: Rc<FileStreamStorage>,
    pub topic: Rc<FileTopicStorage>,
    pub partition: Rc<FilePartitionStorage>,
    pub segment: Rc<FileSegmentStorage>,
    pub persister: Rc<PersistenceStorage>,
}

impl SystemStorage {
    pub fn new(config: Arc<SystemConfig>, persister: Rc<PersistenceStorage>) -> Self {
        Self {
            info: Rc::new(FileSystemInfoStorage::new(
                config.get_state_info_path(),
                persister.clone(),
            )),
            stream: Rc::new(FileStreamStorage::new()),
            topic: Rc::new(FileTopicStorage::new()),
            partition: Rc::new(FilePartitionStorage::new(persister.clone())),
            segment: Rc::new(FileSegmentStorage::new(persister.clone())),
            persister,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::{
        streaming::{
            partitions::storage::FilePartitionStorage, persistence::persister::PersistenceStorage,
            segments::storage::FileSegmentStorage, streams::storage::FileStreamStorage,
            topics::storage::FileTopicStorage,
        },
        tpc::shard::storage::FileSystemInfoStorage,
    };
    use std::rc::Rc;

    use super::SystemStorage;

    pub fn get_test_system_storage() -> SystemStorage {
        SystemStorage {
            info: Rc::new(FileSystemInfoStorage::new(
                "".to_string(),
                Rc::new(PersistenceStorage::Test),
            )),
            stream: Rc::new(FileStreamStorage::noop()),
            topic: Rc::new(FileTopicStorage::noop()),
            partition: Rc::new(FilePartitionStorage::new(Rc::new(PersistenceStorage::Test))),
            segment: Rc::new(FileSegmentStorage::new(Rc::new(PersistenceStorage::Test))),
            persister: Rc::new(PersistenceStorage::Test),
        }
    }
}
