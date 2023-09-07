use crate::partitions::partition::{ConsumerOffset, Partition};
use crate::partitions::storage::FilePartitionStorage;
use crate::persister::Persister;
use crate::segments::index::{Index, IndexRange};
use crate::segments::segment::Segment;
use crate::segments::storage::FileSegmentStorage;
use crate::segments::time_index::TimeIndex;
use crate::streams::storage::FileStreamStorage;
use crate::streams::stream::Stream;
use crate::systems::info::SystemInfo;
use crate::systems::storage::FileSystemInfoStorage;
use crate::topics::consumer_group::ConsumerGroup;
use crate::topics::storage::FileTopicStorage;
use crate::topics::topic::Topic;
use crate::users::storage::FileUserStorage;
use crate::users::user::User;
use async_trait::async_trait;
use iggy::error::Error;
use iggy::models::messages::Message;
use sled::Db;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

#[async_trait]
pub trait Storage<T>: Sync + Send {
    async fn load(&self, component: &mut T) -> Result<(), Error>;
    async fn save(&self, component: &T) -> Result<(), Error>;
    async fn delete(&self, component: &T) -> Result<(), Error>;
}

#[async_trait]
pub trait SystemInfoStorage: Storage<SystemInfo> {}

#[async_trait]
pub trait UserStorage: Storage<User> {
    async fn load_by_username(&self, username: &str) -> Result<User, Error>;
    async fn load_all(&self) -> Result<Vec<User>, Error>;
}

#[async_trait]
pub trait StreamStorage: Storage<Stream> {}

#[async_trait]
pub trait TopicStorage: Storage<Topic> {
    async fn save_partitions(&self, topic: &Topic, partition_ids: &[u32]) -> Result<(), Error>;
    async fn delete_partitions(
        &self,
        topic: &mut Topic,
        partition_ids: &[u32],
    ) -> Result<(), Error>;
    async fn save_consumer_group(
        &self,
        topic: &Topic,
        consumer_group: &ConsumerGroup,
    ) -> Result<(), Error>;
    async fn load_consumer_groups(&self, topic: &mut Topic) -> Result<(), Error>;
    async fn delete_consumer_group(
        &self,
        topic: &Topic,
        consumer_group: &ConsumerGroup,
    ) -> Result<(), Error>;
}

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
    pub info: Arc<dyn SystemInfoStorage>,
    pub user: Arc<dyn UserStorage>,
    pub stream: Arc<dyn StreamStorage>,
    pub topic: Arc<dyn TopicStorage>,
    pub partition: Arc<dyn PartitionStorage>,
    pub segment: Arc<dyn SegmentStorage>,
}

impl SystemStorage {
    pub fn new(db: Arc<Db>, persister: Arc<dyn Persister>) -> Self {
        Self {
            info: Arc::new(FileSystemInfoStorage::new(db.clone())),
            user: Arc::new(FileUserStorage::new(db.clone())),
            stream: Arc::new(FileStreamStorage::new(db.clone())),
            topic: Arc::new(FileTopicStorage::new(db.clone(), persister.clone())),
            partition: Arc::new(FilePartitionStorage::new(db.clone(), persister.clone())),
            segment: Arc::new(FileSegmentStorage::new(persister.clone())),
        }
    }
}

impl Debug for dyn SystemInfoStorage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SystemInfoStorage")
    }
}

impl Debug for dyn UserStorage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "UserStorage")
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
    use crate::partitions::partition::Partition;
    use crate::segments::index::{Index, IndexRange};
    use crate::segments::segment::Segment;
    use crate::segments::time_index::TimeIndex;
    use crate::storage::*;
    use crate::streams::stream::Stream;
    use crate::topics::topic::Topic;
    use async_trait::async_trait;
    use iggy::models::messages::Message;
    use std::sync::Arc;

    struct TestSystemInfoStorage {}
    struct TestUserStorage {}
    struct TestStreamStorage {}
    struct TestTopicStorage {}
    struct TestPartitionStorage {}
    struct TestSegmentStorage {}

    #[async_trait]
    impl Storage<SystemInfo> for TestSystemInfoStorage {
        async fn load(&self, _system_info: &mut SystemInfo) -> Result<(), Error> {
            Ok(())
        }

        async fn save(&self, _system_info: &SystemInfo) -> Result<(), Error> {
            Ok(())
        }

        async fn delete(&self, _system_info: &SystemInfo) -> Result<(), Error> {
            Ok(())
        }
    }

    #[async_trait]
    impl SystemInfoStorage for TestSystemInfoStorage {}

    #[async_trait]
    impl Storage<User> for TestUserStorage {
        async fn load(&self, _user: &mut User) -> Result<(), Error> {
            Ok(())
        }

        async fn save(&self, _user: &User) -> Result<(), Error> {
            Ok(())
        }

        async fn delete(&self, _user: &User) -> Result<(), Error> {
            Ok(())
        }
    }

    #[async_trait]
    impl UserStorage for TestUserStorage {
        async fn load_by_username(&self, _username: &str) -> Result<User, Error> {
            Ok(User::default())
        }

        async fn load_all(&self) -> Result<Vec<User>, Error> {
            Ok(vec![])
        }
    }

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

    #[async_trait]
    impl TopicStorage for TestTopicStorage {
        async fn save_partitions(
            &self,
            _topic: &Topic,
            _partition_ids: &[u32],
        ) -> Result<(), Error> {
            Ok(())
        }

        async fn delete_partitions(
            &self,
            _topic: &mut Topic,
            _partition_ids: &[u32],
        ) -> Result<(), Error> {
            Ok(())
        }

        async fn save_consumer_group(
            &self,
            _topic: &Topic,
            _consumer_group: &ConsumerGroup,
        ) -> Result<(), Error> {
            Ok(())
        }

        async fn load_consumer_groups(&self, _topic: &mut Topic) -> Result<(), Error> {
            Ok(())
        }

        async fn delete_consumer_group(
            &self,
            _topic: &Topic,
            _consumer_group: &ConsumerGroup,
        ) -> Result<(), Error> {
            Ok(())
        }
    }

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
            info: Arc::new(TestSystemInfoStorage {}),
            user: Arc::new(TestUserStorage {}),
            stream: Arc::new(TestStreamStorage {}),
            topic: Arc::new(TestTopicStorage {}),
            partition: Arc::new(TestPartitionStorage {}),
            segment: Arc::new(TestSegmentStorage {}),
        }
    }
}
