use crate::message::Message;
use crate::partitions::partition::{ConsumerOffset, Partition};
use crate::partitions::storage::FilePartitionStorage;
use crate::persister::{FilePersister, Persister};
use crate::segments::index::{Index, IndexRange};
use crate::segments::segment::Segment;
use crate::segments::storage::FileSegmentStorage;
use crate::segments::time_index::TimeIndex;
use crate::streams::storage::FileStreamStorage;
use crate::streams::stream::Stream;
use crate::topics::storage::FileTopicStorage;
use crate::topics::topic::Topic;
use async_trait::async_trait;
use sdk::error::Error;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

#[async_trait]
pub trait Storage<T>: Sync + Send {
    async fn load(&self, component: &mut T) -> Result<(), Error>;
    async fn save(&self, component: &T) -> Result<(), Error>;
    async fn delete(&self, component: &T) -> Result<(), Error>;
}

#[async_trait]
pub trait StreamStorage: Storage<Stream> {}

#[async_trait]
pub trait TopicStorage: Storage<Topic> {}

#[async_trait]
pub trait PartitionStorage: Storage<Partition> {
    async fn save_offset(&self, offset: &ConsumerOffset) -> Result<(), Error>;
}

#[async_trait]
pub trait SegmentStorage: Storage<Segment> {
    async fn load_messages(
        &self,
        segment: &Segment,
        index_range: &IndexRange,
    ) -> Result<Vec<Arc<Message>>, Error>;
    async fn save_messages(
        &self,
        segment: &Segment,
        messages: &[Arc<Message>],
    ) -> Result<u32, Error>;
    async fn load_message_ids(&self, segment: &Segment) -> Result<Vec<u128>, Error>;
    async fn load_checksums(&self, segment: &Segment) -> Result<(), Error>;
    async fn load_all_indexes(&self, segment: &Segment) -> Result<Vec<Index>, Error>;
    async fn load_index_range(
        &self,
        segment: &Segment,
        segment_start_offset: u64,
        index_start_offset: u64,
        index_end_offset: u64,
    ) -> Result<Option<IndexRange>, Error>;
    async fn save_index(
        &self,
        segment: &Segment,
        current_position: u32,
        messages: &[Arc<Message>],
    ) -> Result<(), Error>;
    async fn load_all_time_indexes(&self, segment: &Segment) -> Result<Vec<TimeIndex>, Error>;
    async fn load_last_time_index(&self, segment: &Segment) -> Result<Option<TimeIndex>, Error>;
    async fn save_time_index(
        &self,
        segment: &Segment,
        messages: &[Arc<Message>],
    ) -> Result<(), Error>;
}

#[derive(Debug)]
pub struct SystemStorage {
    pub stream: Arc<dyn StreamStorage>,
    pub topic: Arc<dyn TopicStorage>,
    pub partition: Arc<dyn PartitionStorage>,
    pub segment: Arc<dyn SegmentStorage>,
}

impl SystemStorage {
    pub fn new(persister: Arc<dyn Persister>) -> Self {
        Self {
            stream: Arc::new(FileStreamStorage::new(persister.clone())),
            topic: Arc::new(FileTopicStorage::new(persister.clone())),
            partition: Arc::new(FilePartitionStorage::new(persister.clone())),
            segment: Arc::new(FileSegmentStorage::new(persister.clone())),
        }
    }
}

impl Default for SystemStorage {
    fn default() -> Self {
        Self::new(Arc::new(FilePersister {}))
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
    use crate::message::Message;
    use crate::partitions::partition::Partition;
    use crate::segments::index::{Index, IndexRange};
    use crate::segments::segment::Segment;
    use crate::segments::time_index::TimeIndex;
    use crate::storage::*;
    use crate::streams::stream::Stream;
    use crate::topics::topic::Topic;
    use async_trait::async_trait;
    use std::sync::Arc;

    struct TestStreamStorage {}
    struct TestTopicStorage {}
    struct TestPartitionStorage {}
    struct TestSegmentStorage {}

    #[async_trait]
    impl Storage<Stream> for TestStreamStorage {
        async fn load(&self, _stream: &mut Stream) -> Result<(), Error> {
            Ok(())
        }

        async fn save(&self, _stream: &Stream) -> Result<(), Error> {
            Ok(())
        }

        async fn delete(&self, _stream: &Stream) -> Result<(), Error> {
            Ok(())
        }
    }

    impl StreamStorage for TestStreamStorage {}

    #[async_trait]
    impl Storage<Topic> for TestTopicStorage {
        async fn load(&self, _topic: &mut Topic) -> Result<(), Error> {
            Ok(())
        }

        async fn save(&self, _topic: &Topic) -> Result<(), Error> {
            Ok(())
        }

        async fn delete(&self, _topic: &Topic) -> Result<(), Error> {
            Ok(())
        }
    }

    impl TopicStorage for TestTopicStorage {}

    #[async_trait]
    impl Storage<Partition> for TestPartitionStorage {
        async fn load(&self, _partition: &mut Partition) -> Result<(), Error> {
            Ok(())
        }

        async fn save(&self, _partition: &Partition) -> Result<(), Error> {
            Ok(())
        }

        async fn delete(&self, _partition: &Partition) -> Result<(), Error> {
            Ok(())
        }
    }

    #[async_trait]
    impl PartitionStorage for TestPartitionStorage {
        async fn save_offset(&self, _offset: &ConsumerOffset) -> Result<(), Error> {
            Ok(())
        }
    }

    #[async_trait]
    impl Storage<Segment> for TestSegmentStorage {
        async fn load(&self, _segment: &mut Segment) -> Result<(), Error> {
            Ok(())
        }

        async fn save(&self, _segment: &Segment) -> Result<(), Error> {
            Ok(())
        }

        async fn delete(&self, _segment: &Segment) -> Result<(), Error> {
            Ok(())
        }
    }

    #[async_trait]
    impl SegmentStorage for TestSegmentStorage {
        async fn load_messages(
            &self,
            _segment: &Segment,
            _index_range: &IndexRange,
        ) -> Result<Vec<Arc<Message>>, Error> {
            Ok(vec![])
        }

        async fn save_messages(
            &self,
            _segment: &Segment,
            _messages: &[Arc<Message>],
        ) -> Result<u32, Error> {
            Ok(0)
        }

        async fn load_message_ids(&self, _segment: &Segment) -> Result<Vec<u128>, Error> {
            Ok(vec![])
        }

        async fn load_checksums(&self, _segment: &Segment) -> Result<(), Error> {
            Ok(())
        }

        async fn load_all_indexes(&self, _segment: &Segment) -> Result<Vec<Index>, Error> {
            Ok(vec![])
        }

        async fn load_index_range(
            &self,
            _segment: &Segment,
            _segment_start_offset: u64,
            _index_start_offset: u64,
            _index_end_offset: u64,
        ) -> Result<Option<IndexRange>, Error> {
            Ok(None)
        }

        async fn save_index(
            &self,
            _segment: &Segment,
            _current_position: u32,
            _messages: &[Arc<Message>],
        ) -> Result<(), Error> {
            Ok(())
        }

        async fn load_all_time_indexes(&self, _segment: &Segment) -> Result<Vec<TimeIndex>, Error> {
            Ok(vec![])
        }

        async fn load_last_time_index(
            &self,
            _segment: &Segment,
        ) -> Result<Option<TimeIndex>, Error> {
            Ok(None)
        }

        async fn save_time_index(
            &self,
            _segment: &Segment,
            _messages: &[Arc<Message>],
        ) -> Result<(), Error> {
            Ok(())
        }
    }

    pub fn get_test_system_storage() -> SystemStorage {
        SystemStorage {
            stream: Arc::new(TestStreamStorage {}),
            topic: Arc::new(TestTopicStorage {}),
            partition: Arc::new(TestPartitionStorage {}),
            segment: Arc::new(TestSegmentStorage {}),
        }
    }
}
