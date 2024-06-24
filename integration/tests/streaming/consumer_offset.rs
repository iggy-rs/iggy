use crate::streaming::common::test_setup::TestSetup;
use iggy::consumer::ConsumerKind;
use server::streaming::partitions::partition::ConsumerOffset;
use server::streaming::storage::PartitionStorage;

/*
#[tokio::test]
async fn should_persist_consumer_offsets_and_then_load_them_from_disk() {
    let setup = TestSetup::init().await;
    let storage = setup.storage.partition.as_ref();
    assert_persisted_offsets(storage, ConsumerKind::Consumer).await;
    assert_persisted_offsets(storage, ConsumerKind::ConsumerGroup).await;
}

async fn assert_persisted_offsets(storage: &dyn PartitionStorage, kind: ConsumerKind) {
    let stream_id = 1;
    let topic_id = 2;
    let partition_id = 3;
    let consumer_ids_count = 50;
    let offsets_count = 500;
    for consumer_id in 1..=consumer_ids_count {
        let expected_offsets_count = consumer_id;
        for offset in 0..=offsets_count {
            let consumer_offset =
                ConsumerOffset::new(kind, consumer_id, offset, stream_id, topic_id, partition_id);
            assert_persisted_offset(storage, &consumer_offset, expected_offsets_count).await;
        }
    }
}

async fn assert_persisted_offset(
    storage: &dyn PartitionStorage,
    consumer_offset: &ConsumerOffset,
    expected_offsets_count: u32,
) {
    let parts = get_parts(&consumer_offset.key);
    storage.save_consumer_offset(consumer_offset).await.unwrap();
    let consumer_offsets = storage
        .load_consumer_offsets(
            consumer_offset.kind,
            parts.stream_id,
            parts.topic_id,
            parts.partition_id,
        )
        .await
        .unwrap();
    let expected_offsets_count = expected_offsets_count as usize;
    assert_eq!(consumer_offsets.len(), expected_offsets_count);
    let loaded_consumer_offset = consumer_offsets.get(expected_offsets_count - 1).unwrap();
    assert_eq!(loaded_consumer_offset, consumer_offset);
}

fn get_parts(key: &str) -> ConsumerOffsetParts {
    let parts: Vec<&str> = key.split(':').collect();
    ConsumerOffsetParts {
        stream_id: parts[1].parse().unwrap(),
        topic_id: parts[2].parse().unwrap(),
        partition_id: parts[3].parse().unwrap(),
    }
}

#[derive(Debug)]
struct ConsumerOffsetParts {
    pub stream_id: u32,
    pub topic_id: u32,
    pub partition_id: u32,
}

*/
