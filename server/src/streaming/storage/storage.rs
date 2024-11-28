use super::Storage;
use crate::streaming::io::buf::dma_buf::DmaBuf;
use iggy::error::IggyError;
use std::{
    future::Future,
    io::Write,
    os::unix::fs::{FileExt, OpenOptionsExt},
    pin::Pin,
    task::Poll,
};
use tokio::task::{spawn_blocking, JoinHandle};

#[derive(Debug, Clone)]
pub struct DmaStorage {
    file_path: &'static str,
}

impl DmaStorage {
    pub fn new(file_path: &'static str) -> Self {
        Self { file_path }
    }
}

impl Storage<DmaBuf> for DmaStorage {
    type ReadResult = ReadSectors;

    fn read_sectors(&self, position: u64, buf: DmaBuf) -> Self::ReadResult {
        let mut buf = buf;
        let file_path = self.file_path;
        let handle = spawn_blocking(move || {
            let file = std::fs::File::options()
                .read(true)
                .custom_flags(libc::O_DIRECT)
                .open(file_path)
                .unwrap();
            file.read_exact_at(buf.as_mut(), position)?;
            drop(file);
            Ok(buf)
        });
        ReadSectors { handle }
    }

    async fn write_sectors(&mut self, buf: DmaBuf) -> Result<u32, IggyError> {
        let mut std_file = std::fs::File::options()
            .append(true)
            .custom_flags(libc::O_DIRECT)
            .open(self.file_path)?;
        //let mut file = OpenOptions::new().append(true).custom_flags(libc::O_DIRECT).open(file_path).await?;
        let size = buf.as_ref().len() as _;
        spawn_blocking(move || std_file.write_all(buf.as_ref()))
            .await
            .expect("write_sectors - Failed to join the task")?;
        Ok(size)
    }
}

pub struct ReadSectors {
    handle: JoinHandle<Result<DmaBuf, std::io::Error>>,
}

impl Future for ReadSectors {
    type Output = Result<DmaBuf, std::io::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = &mut self;
        match futures::ready!(Pin::new(&mut this.handle).poll(cx)) {
            Ok(result) => Poll::Ready(result),
            Err(err) => panic!("read_sectors, failed to join the task, error : {}", err),
        }
    }
}
