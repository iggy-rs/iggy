use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
};
use tokio::fs::{read_dir, File, OpenOptions};

pub async fn open(path: &str) -> Result<File, std::io::Error> {
    OpenOptions::new().read(true).open(path).await
}

pub async fn append(path: &str) -> Result<File, std::io::Error> {
    OpenOptions::new().read(true).append(true).open(path).await
}

pub async fn write(path: &str) -> Result<File, std::io::Error> {
    OpenOptions::new().create(true).write(true).open(path).await
}

pub async fn folder_size<P>(path: P) -> std::io::Result<u64>
where
    P: Into<PathBuf> + AsRef<Path>,
{
    let mut total_size = 0;
    let mut queue: VecDeque<PathBuf> = VecDeque::new();
    queue.push_back(path.into());

    while let Some(current_path) = queue.pop_front() {
        let mut entries = read_dir(&current_path).await.unwrap();

        while let Some(entry) = entries.next_entry().await.unwrap() {
            let metadata = entry.metadata().await.unwrap();

            if metadata.is_file() {
                total_size += metadata.len();
            } else if metadata.is_dir() {
                queue.push_back(entry.path());
            }
        }
    }
    Ok(total_size)
}
