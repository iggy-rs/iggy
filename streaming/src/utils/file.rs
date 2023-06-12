use tokio::fs::{File, OpenOptions};

pub async fn open(path: &str) -> Result<File, std::io::Error> {
    OpenOptions::new().read(true).open(path).await
}

pub async fn append(path: &str) -> Result<File, std::io::Error> {
    OpenOptions::new().read(true).append(true).open(path).await
}

pub async fn write(path: &str) -> Result<File, std::io::Error> {
    OpenOptions::new().create(true).write(true).open(path).await
}
