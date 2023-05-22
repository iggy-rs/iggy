mod common;

use crate::common::TestSetup;
use streaming::topics::topic::{Topic, TOPIC_INFO};
use tokio::fs;

#[tokio::test]
async fn should_persist_topics_with_partitions_directories_and_info_file() {
    let setup = TestSetup::init().await;
    let partitions_count = 3;
    let topic_ids = get_topic_ids();
    for topic_id in topic_ids {
        let name = format!("test-{}", topic_id);
        let topic = Topic::create(
            topic_id,
            &name,
            partitions_count,
            &setup.path,
            setup.config.stream.topic.clone(),
        );

        topic.persist().await.unwrap();

        assert_persisted_topic(&topic.path, 3).await;
    }
}

#[tokio::test]
async fn should_load_existing_topic_from_disk() {
    let setup = TestSetup::init().await;
    let partitions_count = 3;
    let topic_ids = get_topic_ids();
    for topic_id in topic_ids {
        let name = format!("test-{}", topic_id);
        let topic = Topic::create(
            topic_id,
            &name,
            partitions_count,
            &setup.path,
            setup.config.stream.topic.clone(),
        );
        topic.persist().await.unwrap();
        assert_persisted_topic(&topic.path, partitions_count).await;

        let mut loaded_topic =
            Topic::empty(topic_id, &setup.path, setup.config.stream.topic.clone());
        loaded_topic.load().await.unwrap();

        assert_eq!(loaded_topic.id, topic.id);
        assert_eq!(loaded_topic.name, topic.name);
        assert_eq!(loaded_topic.path, topic.path);
        assert_eq!(loaded_topic.get_partitions().len() as u32, partitions_count);
    }
}

#[tokio::test]
async fn should_delete_existing_topic_from_disk() {
    let setup = TestSetup::init().await;
    let partitions_count = 3;
    let topic_ids = get_topic_ids();
    for topic_id in topic_ids {
        let name = format!("test-{}", topic_id);
        let topic = Topic::create(
            topic_id,
            &name,
            partitions_count,
            &setup.path,
            setup.config.stream.topic.clone(),
        );
        topic.persist().await.unwrap();
        assert_persisted_topic(&topic.path, partitions_count).await;

        topic.delete().await.unwrap();

        assert!(fs::metadata(&topic.path).await.is_err());
    }
}

async fn assert_persisted_topic(topic_path: &str, partitions_count: u32) {
    let topic_metadata = fs::metadata(topic_path).await.unwrap();
    assert!(topic_metadata.is_dir());
    let topic_info_path = format!("{}/{}", topic_path, TOPIC_INFO);
    let topic_info_metadata = fs::metadata(&topic_info_path).await.unwrap();
    assert!(topic_info_metadata.is_file());
    for partition_id in 1..=partitions_count {
        let partition_path = format!("{}/{}", topic_path, partition_id);
        let partition_metadata = fs::metadata(&partition_path).await.unwrap();
        assert!(partition_metadata.is_dir());
    }
}

fn get_topic_ids() -> Vec<u32> {
    vec![1, 2, 3, 5, 10, 100, 1000, 99999]
}
