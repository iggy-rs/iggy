use crate::streaming::partitions::partition::{ConsumerOffset, Partition};
use crate::streaming::partitions::storage::FilePartitionStorage;
use crate::streaming::persistence::persister::Persister;
use crate::streaming::personal_access_tokens::personal_access_token::PersonalAccessToken;
use crate::streaming::personal_access_tokens::storage::FilePersonalAccessTokenStorage;
use crate::streaming::segments::index::{Index, IndexRange};
use crate::streaming::segments::segment::Segment;
use crate::streaming::segments::storage::FileSegmentStorage;
use crate::streaming::segments::time_index::TimeIndex;
use crate::streaming::streams::storage::FileStreamStorage;
use crate::streaming::streams::stream::Stream;
use crate::streaming::systems::info::SystemInfo;
use crate::streaming::systems::storage::FileSystemInfoStorage;
use crate::streaming::topics::consumer_group::ConsumerGroup;
use crate::streaming::topics::storage::FileTopicStorage;
use crate::streaming::topics::topic::Topic;
use crate::streaming::users::storage::FileUserStorage;
use crate::streaming::users::user::User;
use async_trait::async_trait;
use iggy::consumer::ConsumerKind;
use iggy::error::IggyError;
use iggy::models::polled_messages::PolledMessage;
use iggy::models::user_info::UserId;
use sled::Db;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

#[async_trait]
pub trait Storage<T>: Sync + Send {
    async fn load(&self, component: &mut T) -> Result<(), IggyError>;
    async fn save(&self, component: &T) -> Result<(), IggyError>;
    async fn delete(&self, component: &T) -> Result<(), IggyError>;
}

#[async_trait]
pub trait SystemInfoStorage: Storage<SystemInfo> {}

#[async_trait]
pub trait UserStorage: Storage<User> {
    async fn load_by_id(&self, id: UserId) -> Result<User, IggyError>;
    async fn load_by_username(&self, username: &str) -> Result<User, IggyError>;
    async fn load_all(&self) -> Result<Vec<User>, IggyError>;
}

#[async_trait]
pub trait PersonalAccessTokenStorage: Storage<PersonalAccessToken> {
    async fn load_all(&self) -> Result<Vec<PersonalAccessToken>, IggyError>;
    async fn load_for_user(&self, user_id: UserId) -> Result<Vec<PersonalAccessToken>, IggyError>;
    async fn load_by_token(&self, token: &str) -> Result<PersonalAccessToken, IggyError>;
    async fn load_by_name(
        &self,
        user_id: UserId,
        name: &str,
    ) -> Result<PersonalAccessToken, IggyError>;
    async fn delete_for_user(&self, user_id: UserId, name: &str) -> Result<(), IggyError>;
}

#[async_trait]
pub trait StreamStorage: Storage<Stream> {}

#[async_trait]
pub trait TopicStorage: Storage<Topic> {
    async fn save_consumer_group(
        &self,
        topic: &Topic,
        consumer_group: &ConsumerGroup,
    ) -> Result<(), IggyError>;
    async fn load_consumer_groups(&self, topic: &Topic) -> Result<Vec<ConsumerGroup>, IggyError>;
    async fn delete_consumer_group(
        &self,
        topic: &Topic,
        consumer_group: &ConsumerGroup,
    ) -> Result<(), IggyError>;
}

#[async_trait]
pub trait PartitionStorage: Storage<Partition> {
    async fn save_consumer_offset(&self, offset: &ConsumerOffset) -> Result<(), IggyError>;
    async fn load_consumer_offsets(
        &self,
        kind: ConsumerKind,
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
    ) -> Result<Vec<ConsumerOffset>, IggyError>;
    async fn delete_consumer_offsets(
        &self,
        kind: ConsumerKind,
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
    ) -> Result<(), IggyError>;
}

#[async_trait]
pub trait SegmentStorage: Storage<Segment> {
    async fn load_messages(
        &self,
        segment: &Segment,
        index_range: &IndexRange,
    ) -> Result<Vec<Arc<PolledMessage>>, IggyError>;
    async fn load_newest_messages_by_size(
        &self,
        segment: &Segment,
        size_bytes: u64,
    ) -> Result<Vec<Arc<PolledMessage>>, IggyError>;
    async fn save_messages(
        &self,
        segment: &Segment,
        messages: &[Arc<PolledMessage>],
    ) -> Result<u32, IggyError>;
    async fn load_message_ids(&self, segment: &Segment) -> Result<Vec<u128>, IggyError>;
    async fn load_checksums(&self, segment: &Segment) -> Result<(), IggyError>;
    async fn load_all_indexes(&self, segment: &Segment) -> Result<Vec<Index>, IggyError>;
    async fn load_index_range(
        &self,
        segment: &Segment,
        segment_start_offset: u64,
        index_start_offset: u64,
        index_end_offset: u64,
    ) -> Result<Option<IndexRange>, IggyError>;
    async fn save_index(
        &self,
        segment: &Segment,
        current_position: u32,
        messages: &[Arc<PolledMessage>],
    ) -> Result<(), IggyError>;
    async fn load_all_time_indexes(&self, segment: &Segment) -> Result<Vec<TimeIndex>, IggyError>;
    async fn load_last_time_index(&self, segment: &Segment)
        -> Result<Option<TimeIndex>, IggyError>;
    async fn save_time_index(
        &self,
        segment: &Segment,
        messages: &[Arc<PolledMessage>],
    ) -> Result<(), IggyError>;
}

#[derive(Debug)]
pub struct SystemStorage {
    pub info: Arc<dyn SystemInfoStorage>,
    pub user: Arc<dyn UserStorage>,
    pub personal_access_token: Arc<dyn PersonalAccessTokenStorage>,
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
            personal_access_token: Arc::new(FilePersonalAccessTokenStorage::new(db.clone())),
            stream: Arc::new(FileStreamStorage::new(db.clone())),
            topic: Arc::new(FileTopicStorage::new(db.clone())),
            partition: Arc::new(FilePartitionStorage::new(db.clone())),
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

impl Debug for dyn PersonalAccessTokenStorage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "PersonalAccessTokenStorage")
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
    use iggy::models::polled_messages::PolledMessage;
    use std::sync::Arc;

    struct TestSystemInfoStorage {}
    struct TestUserStorage {}
    struct TestPersonalAccessTokenStorage {}
    struct TestStreamStorage {}
    struct TestTopicStorage {}
    struct TestPartitionStorage {}
    struct TestSegmentStorage {}

    #[async_trait]
    impl Storage<SystemInfo> for TestSystemInfoStorage {
        async fn load(&self, _system_info: &mut SystemInfo) -> Result<(), IggyError> {
            Ok(())
        }

        async fn save(&self, _system_info: &SystemInfo) -> Result<(), IggyError> {
            Ok(())
        }

        async fn delete(&self, _system_info: &SystemInfo) -> Result<(), IggyError> {
            Ok(())
        }
    }

    #[async_trait]
    impl SystemInfoStorage for TestSystemInfoStorage {}

    #[async_trait]
    impl Storage<User> for TestUserStorage {
        async fn load(&self, _user: &mut User) -> Result<(), IggyError> {
            Ok(())
        }

        async fn save(&self, _user: &User) -> Result<(), IggyError> {
            Ok(())
        }

        async fn delete(&self, _user: &User) -> Result<(), IggyError> {
            Ok(())
        }
    }

    #[async_trait]
    impl UserStorage for TestUserStorage {
        async fn load_by_id(&self, _id: UserId) -> Result<User, IggyError> {
            Ok(User::default())
        }

        async fn load_by_username(&self, _username: &str) -> Result<User, IggyError> {
            Ok(User::default())
        }

        async fn load_all(&self) -> Result<Vec<User>, IggyError> {
            Ok(vec![])
        }
    }

    #[async_trait]
    impl Storage<PersonalAccessToken> for TestPersonalAccessTokenStorage {
        async fn load(
            &self,
            _personal_access_token: &mut PersonalAccessToken,
        ) -> Result<(), IggyError> {
            Ok(())
        }

        async fn save(
            &self,
            _personal_access_token: &PersonalAccessToken,
        ) -> Result<(), IggyError> {
            Ok(())
        }

        async fn delete(
            &self,
            _personal_access_token: &PersonalAccessToken,
        ) -> Result<(), IggyError> {
            Ok(())
        }
    }

    #[async_trait]
    impl PersonalAccessTokenStorage for TestPersonalAccessTokenStorage {
        async fn load_all(&self) -> Result<Vec<PersonalAccessToken>, IggyError> {
            Ok(vec![])
        }

        async fn load_for_user(
            &self,
            _user_id: UserId,
        ) -> Result<Vec<PersonalAccessToken>, IggyError> {
            Ok(vec![])
        }

        async fn load_by_token(&self, _token: &str) -> Result<PersonalAccessToken, IggyError> {
            Ok(PersonalAccessToken::default())
        }

        async fn load_by_name(
            &self,
            _user_id: UserId,
            _name: &str,
        ) -> Result<PersonalAccessToken, IggyError> {
            Ok(PersonalAccessToken::default())
        }

        async fn delete_for_user(&self, _user_id: UserId, _name: &str) -> Result<(), IggyError> {
            Ok(())
        }
    }

    #[async_trait]
    impl Storage<Stream> for TestStreamStorage {
        async fn load(&self, _stream: &mut Stream) -> Result<(), IggyError> {
            Ok(())
        }

        async fn save(&self, _stream: &Stream) -> Result<(), IggyError> {
            Ok(())
        }

        async fn delete(&self, _stream: &Stream) -> Result<(), IggyError> {
            Ok(())
        }
    }

    impl StreamStorage for TestStreamStorage {}

    #[async_trait]
    impl Storage<Topic> for TestTopicStorage {
        async fn load(&self, _topic: &mut Topic) -> Result<(), IggyError> {
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
    impl TopicStorage for TestTopicStorage {
        async fn save_consumer_group(
            &self,
            _topic: &Topic,
            _consumer_group: &ConsumerGroup,
        ) -> Result<(), IggyError> {
            Ok(())
        }

        async fn load_consumer_groups(
            &self,
            _topic: &Topic,
        ) -> Result<Vec<ConsumerGroup>, IggyError> {
            Ok(vec![])
        }

        async fn delete_consumer_group(
            &self,
            _topic: &Topic,
            _consumer_group: &ConsumerGroup,
        ) -> Result<(), IggyError> {
            Ok(())
        }
    }

    #[async_trait]
    impl Storage<Partition> for TestPartitionStorage {
        async fn load(&self, _partition: &mut Partition) -> Result<(), IggyError> {
            Ok(())
        }

        async fn save(&self, _partition: &Partition) -> Result<(), IggyError> {
            Ok(())
        }

        async fn delete(&self, _partition: &Partition) -> Result<(), IggyError> {
            Ok(())
        }
    }

    #[async_trait]
    impl PartitionStorage for TestPartitionStorage {
        async fn save_consumer_offset(&self, _offset: &ConsumerOffset) -> Result<(), IggyError> {
            Ok(())
        }

        async fn load_consumer_offsets(
            &self,
            _kind: ConsumerKind,
            _stream_id: u32,
            _topic_id: u32,
            _partition_id: u32,
        ) -> Result<Vec<ConsumerOffset>, IggyError> {
            Ok(vec![])
        }

        async fn delete_consumer_offsets(
            &self,
            _kind: ConsumerKind,
            _stream_id: u32,
            _topic_id: u32,
            _partition_id: u32,
        ) -> Result<(), IggyError> {
            Ok(())
        }
    }

    #[async_trait]
    impl Storage<Segment> for TestSegmentStorage {
        async fn load(&self, _segment: &mut Segment) -> Result<(), IggyError> {
            Ok(())
        }

        async fn save(&self, _segment: &Segment) -> Result<(), IggyError> {
            Ok(())
        }

        async fn delete(&self, _segment: &Segment) -> Result<(), IggyError> {
            Ok(())
        }
    }

    #[async_trait]
    impl SegmentStorage for TestSegmentStorage {
        async fn load_messages(
            &self,
            _segment: &Segment,
            _index_range: &IndexRange,
        ) -> Result<Vec<Arc<PolledMessage>>, IggyError> {
            Ok(vec![])
        }

        async fn load_newest_messages_by_size(
            &self,
            _segment: &Segment,
            _size: u64,
        ) -> Result<Vec<Arc<PolledMessage>>, IggyError> {
            Ok(vec![])
        }

        async fn save_messages(
            &self,
            _segment: &Segment,
            _messages: &[Arc<PolledMessage>],
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
            _segment_start_offset: u64,
            _index_start_offset: u64,
            _index_end_offset: u64,
        ) -> Result<Option<IndexRange>, IggyError> {
            Ok(None)
        }

        async fn save_index(
            &self,
            _segment: &Segment,
            _current_position: u32,
            _messages: &[Arc<PolledMessage>],
        ) -> Result<(), IggyError> {
            Ok(())
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
            _segment: &Segment,
            _messages: &[Arc<PolledMessage>],
        ) -> Result<(), IggyError> {
            Ok(())
        }
    }

    pub fn get_test_system_storage() -> SystemStorage {
        SystemStorage {
            info: Arc::new(TestSystemInfoStorage {}),
            user: Arc::new(TestUserStorage {}),
            personal_access_token: Arc::new(TestPersonalAccessTokenStorage {}),
            stream: Arc::new(TestStreamStorage {}),
            topic: Arc::new(TestTopicStorage {}),
            partition: Arc::new(TestPartitionStorage {}),
            segment: Arc::new(TestSegmentStorage {}),
        }
    }
}
