use super::batching::message_batch::RetainedMessageBatch;
use crate::configs::system::SystemConfig;
use crate::state::system::{PartitionState, StreamState, TopicState};
use crate::streaming::partitions::partition::{ConsumerOffset, Partition};
use crate::streaming::partitions::storage::FilePartitionStorage;
use crate::streaming::persistence::persister::Persister;
use crate::streaming::segments::index::{Index, IndexRange};
use crate::streaming::segments::segment::Segment;
use crate::streaming::segments::storage::FileSegmentStorage;
use crate::streaming::segments::time_index::TimeIndex;
use crate::streaming::streams::storage::FileStreamStorage;
use crate::streaming::streams::stream::Stream;
use crate::streaming::systems::info::SystemInfo;
use crate::streaming::systems::storage::FileSystemInfoStorage;
use crate::streaming::topics::storage::FileTopicStorage;
use crate::streaming::topics::topic::Topic;
use async_trait::async_trait;
use iggy::consumer::ConsumerKind;
use iggy::error::IggyError;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

#[async_trait]
pub trait SystemInfoStorage: Sync + Send {
    async fn load(&self) -> Result<SystemInfo, IggyError>;
    async fn save(&self, system_info: &SystemInfo) -> Result<(), IggyError>;
}

#[async_trait]
pub trait StreamStorage: Send + Sync {
    async fn load(&self, stream: &mut Stream, state: StreamState) -> Result<(), IggyError>;
    async fn save(&self, stream: &Stream) -> Result<(), IggyError>;
    async fn delete(&self, stream: &Stream) -> Result<(), IggyError>;
}

#[async_trait]
pub trait TopicStorage: Send + Sync {
    async fn load(&self, topic: &mut Topic, state: TopicState) -> Result<(), IggyError>;
    async fn save(&self, topic: &Topic) -> Result<(), IggyError>;
    async fn delete(&self, topic: &Topic) -> Result<(), IggyError>;
}

#[async_trait]
pub trait PartitionStorage: Send + Sync {
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

#[async_trait]
pub trait SegmentStorage: Send + Sync {
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
        batch: RetainedMessageBatch,
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
    async fn save_index(&self, index_path: &str, index: Index) -> Result<(), IggyError>;
    async fn try_load_time_index_for_timestamp(
        &self,
        segment: &Segment,
        timestamp: u64,
    ) -> Result<Option<TimeIndex>, IggyError>;
    async fn load_all_time_indexes(&self, segment: &Segment) -> Result<Vec<TimeIndex>, IggyError>;
    async fn load_last_time_index(&self, segment: &Segment)
        -> Result<Option<TimeIndex>, IggyError>;
    async fn save_time_index(&self, index_path: &str, index: TimeIndex) -> Result<(), IggyError>;
}

#[derive(Debug)]
pub struct SystemStorage {
    pub info: Arc<dyn SystemInfoStorage>,
    pub stream: Arc<dyn StreamStorage>,
    pub topic: Arc<dyn TopicStorage>,
    pub partition: Arc<dyn PartitionStorage>,
    pub segment: Arc<dyn SegmentStorage>,
    pub persister: Arc<dyn Persister>,
}

impl SystemStorage {
    pub fn new(config: Arc<SystemConfig>, persister: Arc<dyn Persister>) -> Self {
        Self {
            info: Arc::new(FileSystemInfoStorage::new(
                config.get_state_info_path(),
                persister.clone(),
            )),
            stream: Arc::new(FileStreamStorage),
            topic: Arc::new(FileTopicStorage),
            partition: Arc::new(FilePartitionStorage::new(persister.clone())),
            segment: Arc::new(FileSegmentStorage::new(persister.clone())),
            persister,
        }
    }
}

impl Debug for dyn SystemInfoStorage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SystemInfoStorage")
    }
}

impl Debug for dyn StreamStorage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "StreamStorage")
    }
}

impl Debug for dyn TopicStorage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TopicStorage")
    }
}

impl Debug for dyn PartitionStorage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "PartitionStorage")
    }
}

impl Debug for dyn SegmentStorage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SegmentStorage")
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::streaming::partitions::partition::Partition;
    use crate::streaming::segments::index::{Index, IndexRange};
    use crate::streaming::segments::segment::Segment;
    use crate::streaming::segments::time_index::TimeIndex;
    use crate::streaming::storage::*;
    use crate::streaming::streams::stream::Stream;
    use crate::streaming::topics::topic::Topic;
    use async_trait::async_trait;
    use std::sync::Arc;

    struct TestPersister {}
    struct TestSystemInfoStorage {}
    struct TestStreamStorage {}
    struct TestTopicStorage {}
    struct TestPartitionStorage {}
    struct TestSegmentStorage {}

    #[async_trait]
    impl Persister for TestPersister {
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
    impl SystemInfoStorage for TestSystemInfoStorage {
        async fn load(&self) -> Result<SystemInfo, IggyError> {
            Ok(SystemInfo::default())
        }

        async fn save(&self, _system_info: &SystemInfo) -> Result<(), IggyError> {
            Ok(())
        }
    }

    #[async_trait]
    impl StreamStorage for TestStreamStorage {
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
    impl TopicStorage for TestTopicStorage {
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
    impl PartitionStorage for TestPartitionStorage {
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
    impl SegmentStorage for TestSegmentStorage {
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

        async fn load_all_time_indexes(
            &self,
            _segment: &Segment,
        ) -> Result<Vec<TimeIndex>, IggyError> {
            Ok(vec![])
        }

        async fn load_last_time_index(
            &self,
            _segment: &Segment,
        ) -> Result<Option<TimeIndex>, IggyError> {
            Ok(None)
        }

        async fn save_time_index(
            &self,
            _index_path: &str,
            _index: TimeIndex,
        ) -> Result<(), IggyError> {
            Ok(())
        }
    }

    pub fn get_test_system_storage() -> SystemStorage {
        SystemStorage {
            info: Arc::new(TestSystemInfoStorage {}),
            stream: Arc::new(TestStreamStorage {}),
            topic: Arc::new(TestTopicStorage {}),
            partition: Arc::new(TestPartitionStorage {}),
            segment: Arc::new(TestSegmentStorage {}),
            persister: Arc::new(TestPersister {}),
        }
    }
}
