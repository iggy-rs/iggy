use super::batching::message_batch::RetainedMessageBatch;
use super::persistence::persister::StoragePersister;
use crate::streaming::partitions::partition::{ConsumerOffset, Partition};
use crate::streaming::partitions::storage::FilePartitionStorage;
use crate::streaming::personal_access_tokens::personal_access_token::PersonalAccessToken;
use crate::streaming::personal_access_tokens::storage::FilePersonalAccessTokenStorage;
use crate::streaming::segments::index::{Index, IndexRange};
use crate::streaming::segments::segment::Segment;
use crate::streaming::segments::storage::FileSegmentStorage;
use crate::streaming::segments::time_index::TimeIndex;
use crate::streaming::streams::storage::FileStreamStorage;
use crate::streaming::streams::stream::Stream;
use crate::streaming::topics::consumer_group::ConsumerGroup;
use crate::streaming::topics::storage::FileTopicStorage;
use crate::streaming::topics::topic::Topic;
use crate::streaming::users::storage::FileUserStorage;
use crate::streaming::users::user::User;
use crate::tpc::shard::info::SystemInfo;
use crate::tpc::shard::storage::FileSystemInfoStorage;
use iggy::consumer::ConsumerKind;
use iggy::error::IggyError;
use iggy::models::user_info::UserId;
use sled::Db;
use std::fmt::Debug;
use std::sync::Arc;

pub trait Storage<T> {
    async fn load(&self, component: &mut T) -> Result<(), IggyError>;
    async fn save(&self, component: &T) -> Result<(), IggyError>;
    async fn delete(&self, component: &T) -> Result<(), IggyError>;
}

pub trait SystemInfoStorage: Storage<SystemInfo> {}

pub trait UserStorage: Storage<User> {
    async fn load_by_id(&self, id: UserId) -> Result<User, IggyError>;
    async fn load_by_username(&self, username: &str) -> Result<User, IggyError>;
    async fn load_all(&self) -> Result<Vec<User>, IggyError>;
}

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

pub trait StreamStorage: Storage<Stream> {}

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

pub trait SegmentStorage: Storage<Segment> {
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
        batches: &[Arc<RetainedMessageBatch>],
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
    pub info: Arc<FileSystemInfoStorage>,
    pub user: Arc<FileUserStorage>,
    pub personal_access_token: Arc<FilePersonalAccessTokenStorage>,
    pub stream: Arc<FileStreamStorage>,
    pub topic: Arc<FileTopicStorage>,
    pub partition: Arc<FilePartitionStorage>,
    pub segment: Arc<FileSegmentStorage>,
}

impl SystemStorage {
    pub fn new(db: Arc<Db>, persister: Arc<StoragePersister>) -> Self {
        Self {
            info: Arc::new(FileSystemInfoStorage::new(db.clone())),
            user: Arc::new(FileUserStorage::new(db.clone())),
            personal_access_token: Arc::new(FilePersonalAccessTokenStorage::new(db.clone())),
            stream: Arc::new(FileStreamStorage::new(db.clone())),
            topic: Arc::new(FileTopicStorage::new(db.clone())),
            partition: Arc::new(FilePartitionStorage::new(db.clone())),
            segment: Arc::new(FileSegmentStorage::new(persister)),
        }
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
    use std::sync::Arc;

    pub fn get_test_system_storage() -> SystemStorage {
        let db = sled::Config::new().temporary(true).open().unwrap();
        let persister = Arc::new(StoragePersister::Test);
        SystemStorage::new(Arc::new(db), persister)
    }

    struct TestSystemInfoStorage {}
    struct TestUserStorage {}
    struct TestPersonalAccessTokenStorage {}
    struct TestStreamStorage {}
    struct TestTopicStorage {}
    struct TestPartitionStorage {}
    struct TestSegmentStorage {}

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

    impl SystemInfoStorage for TestSystemInfoStorage {}

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

    impl SegmentStorage for TestSegmentStorage {
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
            _batches: &[Arc<RetainedMessageBatch>],
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

        async fn save_indexes(&self, _segment: &Segment) -> Result<(), IggyError> {
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

        async fn save_time_indexes(&self, _segment: &Segment) -> Result<(), IggyError> {
            Ok(())
        }
    }
}
