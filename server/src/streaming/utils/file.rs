use atone::Vc;
use monoio::fs::{File, OpenOptions};
use std::fs::remove_file;
use std::path::{Path, PathBuf};

pub async fn open(path: &str) -> Result<File, std::io::Error> {
    OpenOptions::new().read(true).open(path).await
}

pub async fn append(path: &str) -> Result<File, std::io::Error> {
    OpenOptions::new().read(true).append(true).open(path).await
}

pub async fn overwrite(path: &str) -> Result<File, std::io::Error> {
    OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .await
}

pub async fn remove(path: &str) -> Result<(), std::io::Error> {
    remove_file(path)
}

pub async fn rename(old_path: &str, new_path: &str) -> Result<(), std::io::Error> {
    std::fs::rename(Path::new(old_path), Path::new(new_path))
}

pub async fn folder_size<P>(path: P) -> std::io::Result<u64>
where
    P: Into<PathBuf> + AsRef<Path>,
{
    let mut total_size = 0;
    let mut queue: Vc<PathBuf> = Vc::new();
    queue.push_back(path.into());

    while let Some(current_path) = queue.pop_front() {
        let mut entries = std::fs::read_dir(&current_path).unwrap();
        while let Some(entry) = entries.next() {
            let entry = entry.unwrap();
            let metadata = entry.metadata().unwrap();
            if metadata.is_file() {
                total_size += metadata.len();
            } else if metadata.is_dir() {
                queue.push_back(entry.path());
            }
        }
    }
    Ok(total_size)
}
