use crate::configs::system::SystemConfig;
use crate::streaming::cache::buffer::SmartCache;
use crate::streaming::cache::memory_tracker::CacheMemoryTracker;
use crate::streaming::deduplication::message_deduplicator::MessageDeduplicator;
use crate::streaming::segments::*;
use crate::streaming::storage::SystemStorage;
use dashmap::DashMap;
use iggy::consumer::ConsumerKind;
use iggy::models::stats::CacheMetrics;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::duration::IggyDuration;
use iggy::utils::expiry::IggyExpiry;
use iggy::utils::sizeable::Sizeable;
use iggy::utils::timestamp::IggyTimestamp;
use std::fmt;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub struct Partition {
    pub stream_id: u32,
    pub topic_id: u32,
    pub partition_id: u32,
    pub partition_path: String,
    pub offsets_path: String,
    pub consumer_offsets_path: String,
    pub consumer_group_offsets_path: String,
    pub current_offset: u64,
    //TODO: Fix me, get rid of this cache impl.
    //pub cache: Option<SmartCache<Arc<()>>>,
    //pub cached_memory_tracker: Option<Arc<CacheMemoryTracker>>,
    pub message_deduplicator: Option<MessageDeduplicator>,
    pub unsaved_messages_count: u32,
    pub should_increment_offset: bool,
    pub created_at: IggyTimestamp,
    pub avg_timestamp_delta: IggyDuration,
    pub messages_count_of_parent_stream: Arc<AtomicU64>,
    pub messages_count_of_parent_topic: Arc<AtomicU64>,
    pub messages_count: Arc<AtomicU64>,
    pub size_of_parent_stream: Arc<AtomicU64>,
    pub size_of_parent_topic: Arc<AtomicU64>,
    pub size_bytes: Arc<AtomicU64>,
    pub segments_count_of_parent_stream: Arc<AtomicU32>,
    pub(crate) message_expiry: IggyExpiry,
    pub(crate) consumer_offsets: DashMap<u32, ConsumerOffset>,
    pub(crate) consumer_group_offsets: DashMap<u32, ConsumerOffset>,
    pub(crate) segments: Vec<Segment>,
    pub(crate) config: Arc<SystemConfig>,
    pub(crate) storage: Arc<SystemStorage>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ConsumerOffset {
    pub kind: ConsumerKind,
    pub consumer_id: u32,
    pub offset: u64,
    pub path: Arc<String>,
}

impl ConsumerOffset {
    pub fn new(kind: ConsumerKind, consumer_id: u32, offset: u64, path: &str) -> ConsumerOffset {
        ConsumerOffset {
            kind,
            consumer_id,
            offset,
            path: Arc::new(format!("{path}/{consumer_id}")),
        }
    }
}

impl Partition {
    #[allow(clippy::too_many_arguments)]
    pub async fn create(
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
        with_segment: bool,
        config: Arc<SystemConfig>,
        storage: Arc<SystemStorage>,
        message_expiry: IggyExpiry,
        messages_count_of_parent_stream: Arc<AtomicU64>,
        messages_count_of_parent_topic: Arc<AtomicU64>,
        size_of_parent_stream: Arc<AtomicU64>,
        size_of_parent_topic: Arc<AtomicU64>,
        segments_count_of_parent_stream: Arc<AtomicU32>,
        created_at: IggyTimestamp,
    ) -> Partition {
        let partition_path = config.get_partition_path(stream_id, topic_id, partition_id);
        let offsets_path = config.get_offsets_path(stream_id, topic_id, partition_id);
        let consumer_offsets_path =
            config.get_consumer_offsets_path(stream_id, topic_id, partition_id);
        let consumer_group_offsets_path =
            config.get_consumer_group_offsets_path(stream_id, topic_id, partition_id);
        //TODO: Fix me
        /*
        let (cached_memory_tracker, messages) = match config.cache.enabled {
            false => (None, None),
            true => (
                CacheMemoryTracker::initialize(&config.cache),
                Some(SmartCache::new()),
            ),
        };
        */

        let mut partition = Partition {
            stream_id,
            topic_id,
            partition_id,
            partition_path,
            offsets_path,
            consumer_offsets_path,
            consumer_group_offsets_path,
            message_expiry,
            //cache: messages,
            //cached_memory_tracker,
            message_deduplicator: match config.message_deduplication.enabled {
                true => Some(MessageDeduplicator::new(
                    if config.message_deduplication.max_entries > 0 {
                        Some(config.message_deduplication.max_entries)
                    } else {
                        None
                    },
                    {
                        if config.message_deduplication.expiry.is_zero() {
                            None
                        } else {
                            Some(config.message_deduplication.expiry)
                        }
                    },
                )),
                false => None,
            },
            segments: vec![],
            current_offset: 0,
            unsaved_messages_count: 0,
            should_increment_offset: false,
            consumer_offsets: DashMap::new(),
            consumer_group_offsets: DashMap::new(),
            config,
            storage,
            created_at,
            avg_timestamp_delta: IggyDuration::default(),
            size_of_parent_stream,
            size_of_parent_topic,
            size_bytes: Arc::new(AtomicU64::new(0)),
            messages_count_of_parent_stream,
            messages_count_of_parent_topic,
            messages_count: Arc::new(AtomicU64::new(0)),
            segments_count_of_parent_stream,
        };

        if with_segment {
            let segment = Segment::create(
                stream_id,
                topic_id,
                partition_id,
                0,
                partition.config.clone(),
                partition.message_expiry,
                partition.size_of_parent_stream.clone(),
                partition.size_of_parent_topic.clone(),
                partition.size_bytes.clone(),
                partition.messages_count_of_parent_stream.clone(),
                partition.messages_count_of_parent_topic.clone(),
                partition.messages_count.clone(),
            );
            partition.segments.push(segment);
            partition
                .segments_count_of_parent_stream
                .fetch_add(1, Ordering::SeqCst);
        }

        partition
    }

    pub fn get_cache_metrics(&self) -> CacheMetrics {
        //TODO: Fix me
        /*
        if let Some(cache) = self.cache.as_ref() {
            let cache_metrics = cache.get_metrics();
            CacheMetrics {
                hits: cache_metrics.hits,
                misses: cache_metrics.misses,
                hit_ratio: cache_metrics.hit_ratio,
            }
        } else {
            CacheMetrics {
                hits: 0,
                misses: 0,
                hit_ratio: 0.0,
            }
        }
        */
        Default::default()
    }
}

impl Sizeable for Partition {
    fn get_size_bytes(&self) -> IggyByteSize {
        IggyByteSize::from(self.size_bytes.load(Ordering::SeqCst))
    }
}

impl fmt::Display for Partition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Partition {{ stream_id: {}, topic_id: {}, partition_id: {}, path: {}, current_offset: {} }}",
            self.stream_id,
            self.topic_id,
            self.partition_id,
            self.partition_path,
            self.current_offset,
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::configs::system::{CacheConfig, SystemConfig};
    use crate::streaming::partitions::partition::Partition;
    use crate::streaming::persistence::persister::{FileWithSyncPersister, PersisterKind};
    use crate::streaming::storage::SystemStorage;
    use iggy::utils::duration::IggyDuration;
    use iggy::utils::expiry::IggyExpiry;
    use iggy::utils::timestamp::IggyTimestamp;
    use std::sync::atomic::{AtomicU32, AtomicU64};
    use std::sync::Arc;

    #[tokio::test]
    async fn should_be_created_with_a_single_segment_given_valid_parameters() {
        let tempdir = tempfile::TempDir::new().unwrap();
        let config = Arc::new(SystemConfig {
            path: tempdir.path().to_str().unwrap().to_string(),
            ..Default::default()
        });
        let storage = Arc::new(SystemStorage::new(
            config.clone(),
            Arc::new(PersisterKind::FileWithSync(FileWithSyncPersister {})),
        ));

        let stream_id = 1;
        let topic_id = 2;
        let partition_id = 3;
        let with_segment = true;
        let path = config.get_partition_path(stream_id, topic_id, partition_id);
        let message_expiry = IggyExpiry::ExpireDuration(IggyDuration::from(10));
        let partition = Partition::create(
            stream_id,
            topic_id,
            partition_id,
            with_segment,
            config,
            storage,
            message_expiry,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU32::new(0)),
            IggyTimestamp::now(),
        )
        .await;

        assert_eq!(partition.stream_id, stream_id);
        assert_eq!(partition.topic_id, topic_id);
        assert_eq!(partition.partition_id, partition_id);
        assert_eq!(partition.partition_path, path);
        assert_eq!(partition.current_offset, 0);
        assert_eq!(partition.unsaved_messages_count, 0);
        assert_eq!(partition.segments.len(), 1);
        assert!(!partition.should_increment_offset);
        let consumer_offsets = partition.consumer_offsets;
        assert_eq!(partition.message_expiry, message_expiry);
        assert!(consumer_offsets.is_empty());
    }

    #[tokio::test]
    async fn should_not_initialize_cache_given_zero_capacity() {
        // TODO: Fix me
        /*
        let tempdir = tempfile::TempDir::new().unwrap();
        let config = Arc::new(SystemConfig {
            path: tempdir.path().to_str().unwrap().to_string(),
            ..Default::default()
        });
        let storage = Arc::new(SystemStorage::new(
            config.clone(),
            Arc::new(PersisterKind::FileWithSync(FileWithSyncPersister {})),
        ));

        let partition = Partition::create(
            1,
            1,
            1,
            true,
            Arc::new(SystemConfig {
                cache: CacheConfig {
                    enabled: false,
                    size: "0".parse().unwrap(),
                },
                ..Default::default()
            }),
            storage,
            IggyExpiry::NeverExpire,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU32::new(0)),
            IggyTimestamp::now(),
        )
        .await;
        assert!(partition.cache.is_none());
        */
    }

    #[tokio::test]
    async fn should_not_initialize_segments_given_false_with_segment_parameter() {
        let tempdir = tempfile::TempDir::new().unwrap();
        let config = Arc::new(SystemConfig {
            path: tempdir.path().to_str().unwrap().to_string(),
            ..Default::default()
        });
        let storage = Arc::new(SystemStorage::new(
            config.clone(),
            Arc::new(PersisterKind::FileWithSync(FileWithSyncPersister {})),
        ));

        let topic_id = 1;
        let partition = Partition::create(
            1,
            topic_id,
            1,
            false,
            Arc::new(SystemConfig::default()),
            storage,
            IggyExpiry::NeverExpire,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU32::new(0)),
            IggyTimestamp::now(),
        )
        .await;
        assert!(partition.segments.is_empty());
    }
}
