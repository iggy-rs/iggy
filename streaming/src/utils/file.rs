use tokio::fs::{File, OpenOptions};

pub async fn create_file(path: &str) -> File {
    OpenOptions::new()
        .create(true)
        .write(true)
        .open(path)
        .await
        .unwrap()
}

pub async fn open_file(path: &str, append: bool) -> File {
    OpenOptions::new()
        .read(true)
        .append(append)
        .open(path)
        .await
        .unwrap()
}
