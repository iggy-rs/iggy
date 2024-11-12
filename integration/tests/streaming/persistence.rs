use std::{sync::Arc, time::Duration};

use server::streaming::persistence::persister::{FilePersister, Persister};

use bytes::Bytes;
use iggy::{confirmation::Confirmation, utils::duration::IggyDuration};
use server::configs::system::SystemConfig;
use tempfile::NamedTempFile;
use tokio::{io::AsyncReadExt, time::sleep};

#[tokio::test]
async fn test_append_nowait() {
    let config = SystemConfig::default();

    let temp_out_file = NamedTempFile::new().unwrap();
    let file_path = temp_out_file.path().to_path_buf();

    let bytes = b"test data";

    let persister = FilePersister::new(Arc::new(config)).await;
    let err = persister
        .append(
            file_path.to_str().unwrap(),
            Bytes::copy_from_slice(bytes),
            Some(Confirmation::Nowait),
        )
        .await;
    assert!(err.is_ok());

    sleep(Duration::from_millis(100)).await;

    let mut file = tokio::fs::File::open(&file_path).await.unwrap();
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await.unwrap();

    assert_eq!(buffer, bytes);
}

#[tokio::test]
async fn test_task_removal_on_idle_timeout_and_persistence_of_active_task() {
    let mut config = SystemConfig::default();
    config.state.idle_timeout = IggyDuration::new(Duration::from_millis(100));

    let temp_out_file_1 = NamedTempFile::new().unwrap();
    let file_path_1 = temp_out_file_1.path().to_path_buf();
    let file_path_str_1 = file_path_1.to_str().unwrap();

    let temp_out_file_2 = NamedTempFile::new().unwrap();
    let file_path_2 = temp_out_file_2.path().to_path_buf();
    let file_path_str_2 = file_path_2.to_str().unwrap();

    let persister = FilePersister::new(Arc::new(config)).await;

    assert!(
        !persister.is_task_active(file_path_str_1),
        "Task 1 should not be active initially"
    );
    assert!(
        !persister.is_task_active(file_path_str_2),
        "Task 2 should not be active initially"
    );

    // Activate the first task by issuing an append command
    let err = persister
        .append(file_path_str_1, Bytes::new(), Some(Confirmation::Nowait))
        .await;
    assert!(err.is_ok());
    assert!(
        persister.is_task_active(file_path_str_1),
        "Task 1 should be active after appending"
    );

    // Wait 50 ms, then activate the second task to refresh its timeout
    sleep(Duration::from_millis(50)).await;
    let err = persister
        .append(file_path_str_2, Bytes::new(), Some(Confirmation::Nowait))
        .await;
    assert!(err.is_ok());
    assert!(
        persister.is_task_active(file_path_str_2),
        "Task 2 should be active after appending"
    );

    // Wait another 70 ms, so the total time for the first task (120 ms) exceeds idle_timeout,
    // but the second task remains active since its timeout was refreshed
    sleep(Duration::from_millis(70)).await;

    // Ensure the second task is still active after its recent append
    assert!(
        persister.is_task_active(file_path_str_2),
        "Task 2 should still be active after recent append"
    );

    // Confirm that the first task has been terminated due to idle timeout expiration
    assert!(
        !persister.is_task_active(file_path_str_1),
        "Task 1 should no longer be active after timeout"
    );

    // Wait another 150 ms to confirm that the second task also terminates after its own idle timeout
    sleep(Duration::from_millis(150)).await;
    assert!(
        !persister.is_task_active(file_path_str_2),
        "Task 2 should no longer be active after idle timeout"
    );
}
