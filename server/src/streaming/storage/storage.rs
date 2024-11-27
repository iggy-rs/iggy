
use super::Storage;
use crate::streaming::io::buf::dma_buf::DmaBuf;
use futures::{
    future::poll_fn, stream::IntoAsyncRead, AsyncReadExt, FutureExt, Stream, StreamExt, TryStream,
};
use iggy::error::IggyError;
use pin_project::pin_project;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{
    future::{Future, IntoFuture},
    io::{Read, Seek, SeekFrom},
    os::unix::fs::{FileExt, OpenOptionsExt},
    pin::Pin,
    sync::{Arc, Mutex},
    task::Poll,
};
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncReadExt as _, AsyncSeekExt, BufReader},
    task::{spawn_blocking, JoinHandle},
};
use tracing::{error, warn};

#[derive(Debug)]
pub struct DmaStorage {
    file_path: &'static str,
    block_size: usize,
    storage: TestStorage,
}

#[derive(Debug, Clone)]
pub struct TestStorage {
    file_path: &'static str,
}

impl TestStorage {
    pub fn new(file_path: &'static str) -> Self {
        Self { file_path }
    }
    pub async fn read_sectors(&self, position: u64, mut buf: DmaBuf) -> ReadSectors {
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
            Err(err) => todo!(),
        }
    }
}

impl DmaStorage {
    pub fn new(file_path: String, block_size: usize) -> Self {
        let path = file_path.leak();
        let storage = TestStorage::new(path);
        Self {
            file_path: path,
            block_size,
            storage,
        }
    }
}

impl Storage<DmaBuf> for DmaStorage {
    fn read_blocks(
        &self,
        position: u64,
        limit: u64,
    ) -> impl Stream<Item = Result<DmaBuf, std::io::Error>> {
        //let state = State::init(file);
        let storage = self.storage.clone();
        BlockStream::new(storage, position, self.block_size, limit)
    }
}

#[pin_project]
struct BlockStream {
    #[pin]
    pending: Option<ReadSectors>,
    storage: TestStorage,
    position: u64,
    size: usize,
    limit: u64,
}

impl BlockStream {
    pub fn new(storage: TestStorage, position: u64, size: usize, limit: u64) -> Self {
        BlockStream {
            pending: None,
            storage,
            position,
            size,
            limit,
        }
    }
}

impl Stream for BlockStream {
    type Item = Result<DmaBuf, std::io::Error>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if *this.position > *this.limit {
            return Poll::Ready(None);
        }

        if let Some(fut) = this.pending.as_mut().as_pin_mut() {
            let result = futures::ready!(fut.poll(cx));
            *this.position += *this.size as u64;
            this.pending.set(None);
            return Poll::Ready(Some(result));
        } else {
            let buf_size = std::cmp::min(*this.size, (*this.limit - *this.position) as usize);
            let buf = DmaBuf::new(buf_size);
            let mut fut = this
                .storage
                .read_sectors(*this.position, buf);
            match fut.poll_unpin(cx) {
                Poll::Ready(result) => {
                    *this.position += *this.size as u64;
                    Poll::Ready(Some(result))
                }
                Poll::Pending => {
                    this.pending.set(Some(fut));
                    Poll::Pending
                }
            }
        }
    }
}
