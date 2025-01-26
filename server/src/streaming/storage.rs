use super::batching::message_batch::RetainedMessageBatch;
use super::persistence::persister::PersisterKind;
use crate::configs::system::SystemConfig;
use crate::state::system::{PartitionState, StreamState, TopicState};
use crate::streaming::partitions::partition::{ConsumerOffset, Partition};
use crate::streaming::partitions::storage::FilePartitionStorage;
use crate::streaming::segments::index::{Index, IndexRange};
use crate::streaming::segments::segment::Segment;
use crate::streaming::segments::storage::FileSegmentStorage;
use crate::streaming::streams::storage::FileStreamStorage;
use crate::streaming::streams::stream::Stream;
use crate::streaming::systems::info::SystemInfo;
use crate::streaming::systems::storage::FileSystemInfoStorage;
use crate::streaming::topics::storage::FileTopicStorage;
use crate::streaming::topics::topic::Topic;
use iggy::confirmation::Confirmation;
use iggy::consumer::ConsumerKind;
use iggy::error::IggyError;
use iggy::utils::byte_size::IggyByteSize;
#[cfg(test)]
use mockall::automock;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;

macro_rules! forward_async_methods {
    (
        $(
            async fn $method_name:ident(
                &self $(, $arg:ident : $arg_ty:ty )*
            ) -> $ret:ty ;
        )*
    ) => {
        $(
            pub async fn $method_name(&self, $( $arg: $arg_ty ),* ) -> $ret {
                match self {
                    Self::File(d) => d.$method_name($( $arg ),*).await,
                    #[cfg(test)]
                    Self::Mock(s) => s.$method_name($( $arg ),*).await,
                }
            }
        )*
    }
}

#[derive(Debug)]
pub enum SystemInfoStorageKind {
    File(FileSystemInfoStorage),
    #[cfg(test)]
    Mock(MockSystemInfoStorage),
}

#[derive(Debug)]
pub enum StreamStorageKind {
    File(FileStreamStorage),
    #[cfg(test)]
    Mock(MockStreamStorage),
}

#[derive(Debug)]
pub enum TopicStorageKind {
    File(FileTopicStorage),
    #[cfg(test)]
    Mock(MockTopicStorage),
}

#[derive(Debug)]
pub enum PartitionStorageKind {
    File(FilePartitionStorage),
    #[cfg(test)]
    Mock(MockPartitionStorage),
}

#[derive(Debug)]
pub enum SegmentStorageKind {
    File(FileSegmentStorage),
    #[cfg(test)]
    Mock(Box<MockSegmentStorage>),
}

#[cfg_attr(test, automock)]
pub trait SystemInfoStorage: Send {
    fn load(&self) -> impl Future<Output = Result<SystemInfo, IggyError>> + Send;
    fn save(&self, system_info: &SystemInfo) -> impl Future<Output = Result<(), IggyError>> + Send;
}

#[cfg_attr(test, automock)]
pub trait StreamStorage: Send {
    fn load(
        &self,
        stream: &mut Stream,
        state: StreamState,
    ) -> impl Future<Output = Result<(), IggyError>> + Send;
    fn save(&self, stream: &Stream) -> impl Future<Output = Result<(), IggyError>> + Send;
    fn delete(&self, stream: &Stream) -> impl Future<Output = Result<(), IggyError>> + Send;
}

#[cfg_attr(test, automock)]
pub trait TopicStorage: Send {
    fn load(
        &self,
        topic: &mut Topic,
        state: TopicState,
    ) -> impl Future<Output = Result<(), IggyError>> + Send;
    fn save(&self, topic: &Topic) -> impl Future<Output = Result<(), IggyError>> + Send;
    fn delete(&self, topic: &Topic) -> impl Future<Output = Result<(), IggyError>> + Send;
}

#[cfg_attr(test, automock)]
pub trait PartitionStorage: Send {
    fn load(
        &self,
        partition: &mut Partition,
        state: PartitionState,
    ) -> impl Future<Output = Result<(), IggyError>> + Send;
    fn save(&self, partition: &Partition) -> impl Future<Output = Result<(), IggyError>> + Send;
    fn delete(&self, partition: &Partition) -> impl Future<Output = Result<(), IggyError>> + Send;
    fn save_consumer_offset(
        &self,
        offset: &ConsumerOffset,
    ) -> impl Future<Output = Result<(), IggyError>> + Send;
    fn load_consumer_offsets(
        &self,
        kind: ConsumerKind,
        path: &str,
    ) -> impl Future<Output = Result<Vec<ConsumerOffset>, IggyError>> + Send;
    fn delete_consumer_offsets(
        &self,
        path: &str,
    ) -> impl Future<Output = Result<(), IggyError>> + Send;
    fn delete_consumer_offset(
        &self,
        path: &str,
    ) -> impl Future<Output = Result<(), IggyError>> + Send;
}

#[cfg_attr(test, automock)]
pub trait SegmentStorage: Send {
    fn load(&self, segment: &mut Segment) -> impl Future<Output = Result<(), IggyError>> + Send;
    fn save(&self, segment: &Segment) -> impl Future<Output = Result<(), IggyError>> + Send;
    fn delete(&self, segment: &Segment) -> impl Future<Output = Result<(), IggyError>> + Send;
    fn load_message_batches(
        &self,
        segment: &Segment,
        index_range: &IndexRange,
    ) -> impl Future<Output = Result<Vec<RetainedMessageBatch>, IggyError>> + Send;
    fn load_newest_batches_by_size(
        &self,
        segment: &Segment,
        size_bytes: u64,
    ) -> impl Future<Output = Result<Vec<RetainedMessageBatch>, IggyError>> + Send;
    fn save_batches(
        &self,
        segment: &Segment,
        batch: RetainedMessageBatch,
        confirmation: Confirmation,
    ) -> impl Future<Output = Result<IggyByteSize, IggyError>> + Send;
    fn load_message_ids(
        &self,
        segment: &Segment,
    ) -> impl Future<Output = Result<Vec<u128>, IggyError>> + Send;
    fn load_checksums(
        &self,
        segment: &Segment,
    ) -> impl Future<Output = Result<(), IggyError>> + Send;
    fn load_all_indexes(
        &self,
        segment: &Segment,
    ) -> impl Future<Output = Result<Vec<Index>, IggyError>> + Send;
    fn load_index_range(
        &self,
        segment: &Segment,
        index_start_offset: u64,
        index_end_offset: u64,
    ) -> impl Future<Output = Result<Option<IndexRange>, IggyError>> + Send;
    fn save_index(
        &self,
        index_path: &str,
        index: Index,
    ) -> impl Future<Output = Result<(), IggyError>> + Send;
    fn try_load_index_for_timestamp(
        &self,
        segment: &Segment,
        timestamp: u64,
    ) -> impl Future<Output = Result<Option<Index>, IggyError>> + Send;
    fn persister(&self) -> Arc<PersisterKind>;
}

#[derive(Debug)]
pub struct SystemStorage {
    pub info: Arc<SystemInfoStorageKind>,
    pub stream: Arc<StreamStorageKind>,
    pub topic: Arc<TopicStorageKind>,
    pub partition: Arc<PartitionStorageKind>,
    pub segment: Arc<SegmentStorageKind>,
    pub persister: Arc<PersisterKind>,
}

impl SystemStorage {
    pub fn new(config: Arc<SystemConfig>, persister: Arc<PersisterKind>) -> Self {
        Self {
            info: Arc::new(SystemInfoStorageKind::File(FileSystemInfoStorage::new(
                config.get_state_info_path(),
                persister.clone(),
            ))),
            stream: Arc::new(StreamStorageKind::File(FileStreamStorage)),
            topic: Arc::new(TopicStorageKind::File(FileTopicStorage)),
            partition: Arc::new(PartitionStorageKind::File(FilePartitionStorage::new(
                persister.clone(),
            ))),
            segment: Arc::new(SegmentStorageKind::File(FileSegmentStorage::new(
                persister.clone(),
            ))),
            persister,
        }
    }
}

impl SystemInfoStorageKind {
    forward_async_methods! {
        async fn load(&self) -> Result<SystemInfo, IggyError>;
        async fn save(&self, system_info: &SystemInfo) -> Result<(), IggyError>;
    }
}

impl StreamStorageKind {
    forward_async_methods! {
        async fn load(&self, stream: &mut Stream, state: StreamState) -> Result<(), IggyError>;
        async fn save(&self, stream: &Stream) -> Result<(), IggyError>;
        async fn delete(&self, stream: &Stream) -> Result<(), IggyError>;
    }
}

impl TopicStorageKind {
    forward_async_methods! {
        async fn load(&self, topic: &mut Topic, state: TopicState) -> Result<(), IggyError>;
        async fn save(&self, topic: &Topic) -> Result<(), IggyError>;
        async fn delete(&self, topic: &Topic) -> Result<(), IggyError>;
    }
}

impl PartitionStorageKind {
    forward_async_methods! {
        async fn load(&self, partition: &mut Partition, state: PartitionState)
            -> Result<(), IggyError>;
        async fn save(&self, partition: &Partition) -> Result<(), IggyError>;
        async fn delete(&self, partition: &Partition) -> Result<(), IggyError>;
        async fn save_consumer_offset(&self, offset: &ConsumerOffset) -> Result<(), IggyError>;
        async fn load_consumer_offsets(
            &self,
            kind: ConsumerKind,
            path: &str
        ) -> Result<Vec<ConsumerOffset>, IggyError>;
        async fn delete_consumer_offsets(&self, path: &str) -> Result<(), IggyError>;
        async fn delete_consumer_offset(&self, path: &str) -> Result<(), IggyError>;
    }
}

impl SegmentStorageKind {
    forward_async_methods! {
        async fn load(&self, segment: &mut Segment) -> Result<(), IggyError>;
        async fn save(&self, segment: &Segment) -> Result<(), IggyError>;
        async fn delete(&self, segment: &Segment) -> Result<(), IggyError>;
        async fn load_message_batches(
            &self,
            segment: &Segment,
            index_range: &IndexRange
        ) -> Result<Vec<RetainedMessageBatch>, IggyError>;
        async fn load_newest_batches_by_size(
            &self,
            segment: &Segment,
            size_bytes: u64
        ) -> Result<Vec<RetainedMessageBatch>, IggyError>;
        async fn save_batches(
            &self,
            segment: &Segment,
            batch: RetainedMessageBatch,
            confirmation: Confirmation
        ) -> Result<IggyByteSize, IggyError>;
        async fn load_message_ids(&self, segment: &Segment) -> Result<Vec<u128>, IggyError>;
        async fn load_checksums(&self, segment: &Segment) -> Result<(), IggyError>;
        async fn load_all_indexes(&self, segment: &Segment) -> Result<Vec<Index>, IggyError>;
        async fn load_index_range(
            &self,
            segment: &Segment,
            index_start_offset: u64,
            index_end_offset: u64
        ) -> Result<Option<IndexRange>, IggyError>;
        async fn save_index(&self, index_path: &str, index: Index) -> Result<(), IggyError>;
        async fn try_load_index_for_timestamp(
            &self,
            segment: &Segment,
            timestamp: u64
        ) -> Result<Option<Index>, IggyError>;
    }

    pub fn persister(&self) -> Arc<PersisterKind> {
        match self {
            Self::File(s) => s.persister(),
            #[cfg(test)]
            Self::Mock(s) => s.persister(),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::streaming::persistence::persister::MockPersister;
    use crate::streaming::storage::*;
    use std::sync::Arc;

    pub fn get_test_system_storage() -> SystemStorage {
        let persister = Arc::new(PersisterKind::Mock(MockPersister::new()));

        let system_info_storage = create_mock_system_info_storage();
        let stream_storage = create_mock_stream_storage();
        let topic_storage = create_mock_topic_storage();
        let partition_storage = create_mock_partition_storage();
        let segment_storage = create_mock_segment_storage();

        SystemStorage {
            info: Arc::new(SystemInfoStorageKind::Mock(system_info_storage)),
            stream: Arc::new(StreamStorageKind::Mock(stream_storage)),
            topic: Arc::new(TopicStorageKind::Mock(topic_storage)),
            partition: Arc::new(PartitionStorageKind::Mock(partition_storage)),
            segment: Arc::new(SegmentStorageKind::Mock(Box::new(segment_storage))),
            persister,
        }
    }

    fn create_mock_system_info_storage() -> MockSystemInfoStorage {
        let mut mock = MockSystemInfoStorage::new();
        mock.expect_load()
            .returning(|| Box::pin(async { Ok(SystemInfo::default()) }));
        mock.expect_save().returning(|_| Box::pin(async { Ok(()) }));
        mock
    }

    fn create_mock_stream_storage() -> MockStreamStorage {
        let mut mock = MockStreamStorage::new();
        mock.expect_load()
            .returning(|_, _| Box::pin(async { Ok(()) }));
        mock.expect_save().returning(|_| Box::pin(async { Ok(()) }));
        mock.expect_delete()
            .returning(|_| Box::pin(async { Ok(()) }));
        mock
    }

    fn create_mock_topic_storage() -> MockTopicStorage {
        let mut mock = MockTopicStorage::new();
        mock.expect_load()
            .returning(|_, _| Box::pin(async { Ok(()) }));
        mock.expect_save().returning(|_| Box::pin(async { Ok(()) }));
        mock.expect_delete()
            .returning(|_| Box::pin(async { Ok(()) }));
        mock
    }

    fn create_mock_partition_storage() -> MockPartitionStorage {
        let mut mock = MockPartitionStorage::new();
        mock.expect_load()
            .returning(|_, _| Box::pin(async { Ok(()) }));
        mock.expect_save().returning(|_| Box::pin(async { Ok(()) }));
        mock.expect_delete()
            .returning(|_| Box::pin(async { Ok(()) }));
        mock.expect_save_consumer_offset()
            .returning(|_| Box::pin(async { Ok(()) }));
        mock.expect_load_consumer_offsets()
            .returning(|_, _| Box::pin(async { Ok(vec![]) }));
        mock.expect_delete_consumer_offsets()
            .returning(|_| Box::pin(async { Ok(()) }));
        mock.expect_delete_consumer_offset()
            .returning(|_| Box::pin(async { Ok(()) }));
        mock
    }

    fn create_mock_segment_storage() -> MockSegmentStorage {
        let mut mock = MockSegmentStorage::new();
        mock.expect_load().returning(|_| Box::pin(async { Ok(()) }));
        mock.expect_save().returning(|_| Box::pin(async { Ok(()) }));
        mock.expect_delete()
            .returning(|_| Box::pin(async { Ok(()) }));
        mock.expect_load_message_batches()
            .returning(|_, _| Box::pin(async { Ok(vec![]) }));
        mock.expect_load_newest_batches_by_size()
            .returning(|_, _| Box::pin(async { Ok(vec![]) }));
        mock.expect_save_batches()
            .returning(|_, _, _| Box::pin(async { Ok(IggyByteSize::default()) }));
        mock.expect_load_message_ids()
            .returning(|_| Box::pin(async { Ok(vec![]) }));
        mock.expect_load_checksums()
            .returning(|_| Box::pin(async { Ok(()) }));
        mock.expect_load_all_indexes()
            .returning(|_| Box::pin(async { Ok(vec![]) }));
        mock.expect_load_index_range()
            .returning(|_, _, _| Box::pin(async { Ok(None) }));
        mock.expect_save_index()
            .returning(|_, _| Box::pin(async { Ok(()) }));
        mock.expect_try_load_index_for_timestamp()
            .returning(|_, _| Box::pin(async { Ok(None) }));
        mock
    }
}
