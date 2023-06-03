use tokio::fs::{File, OpenOptions};

pub async fn create_file(path: &str) -> Result<File, std::io::Error> {
    OpenOptions::new().create(true).write(true).open(path).await
}

pub async fn open_file(path: &str, append: bool) -> Result<File, std::io::Error> {
    OpenOptions::new()
        .read(true)
        .append(append)
        .open(path)
        .await
}
