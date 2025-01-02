use super::Storage;
use crate::{server_error::IoError, streaming::io::buf::dma_buf::DmaBuf};
use std::{
    future::Future,
    io::Write,
    os::unix::fs::{FileExt, OpenOptionsExt},
    pin::Pin,
    task::Poll,
};
use tokio::task::{spawn_blocking, JoinHandle};

const O_DIRECT: i32 = 0x4000;

#[derive(Debug)]
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
        let file_path = self.file_path;
        let file = std::fs::File::options()
            .read(true)
            .custom_flags(O_DIRECT)
            .open(file_path)
            .unwrap();
        let handle = spawn_blocking(move || {
            let mut buf = buf;
            file.read_exact_at(buf.as_mut(), position)?;
            drop(file);
            Ok(buf)
        });
        ReadSectors { handle }
    }

    async fn write_sectors(&mut self, buf: DmaBuf) -> Result<u32, IoError> {
        let mut std_file = std::fs::File::options()
            .append(true)
            .custom_flags(O_DIRECT)
            .open(self.file_path)?;
        let size = buf.as_ref().len() as _;
        spawn_blocking(move || {
            let buf = buf;
            std_file.write_all(buf.as_ref())
        })
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
            Err(err) => panic!("read_sectors, failed to join the task, error: {}", err),
        }
    }
}
