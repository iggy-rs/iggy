use super::Storage;
use crate::streaming::io::buf::dma_buf::DmaBuf;
use futures::{FutureExt, Stream, TryStream};
use std::{
    future::Future,
    io::{Read, Seek, SeekFrom},
    os::unix::fs::{FileExt, OpenOptionsExt},
    pin::Pin,
    sync::{Arc, Mutex},
    task::Poll,
};
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncReadExt, BufReader},
    task::{spawn_blocking, JoinHandle},
};
use tracing::warn;

#[derive(Debug)]
pub struct DmaStorage {
    file_path: &'static str,
    block_size: usize,
}

impl DmaStorage {
    pub fn new(file_path: String, block_size: usize) -> Self {
        Self {
            file_path: file_path.leak(),
            block_size,
        }
    }
}

impl Storage<DmaBuf> for DmaStorage {
    fn read_blocks(
        &self,
        position: u64,
        limit: u64,
    ) -> impl Stream<Item = Result<DmaBuf, std::io::Error>> {
        let file = std::fs::File::options()
            .read(true)
            .append(true)
            .custom_flags(libc::O_DIRECT)
            .open(self.file_path)
            .unwrap();
        let state = State::init(file);
        BlockStream {
            state: Arc::new(Mutex::new(state)),
            position: Arc::new(Mutex::new(position)),
            limit,
            size: self.block_size,
        }
    }
}

struct BlockStream<R>
where
    R: FileExt + Read + Unpin,
{
    state: Arc<Mutex<State<R>>>,
    position: Arc<Mutex<u64>>,
    size: usize,
    limit: u64,
}

struct State<R>
where
    R: FileExt + Read,
{
    reader: R,
    result: Option<(usize, Result<Option<DmaBuf>, std::io::Error>)>,
}

impl<R> State<R>
where
    R: FileExt + Read,
{
    fn init(reader: R) -> Self {
        Self {
            reader,
            result: None,
        }
    }
}

impl<R> Stream for BlockStream<R>
where
    R: Seek + FileExt + Read + Send + Sync + Unpin + 'static,
{
    type Item = Result<DmaBuf, std::io::Error>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut state = self.state.lock().unwrap();
        if let Some(result) = state.result.take() {
            let mut position = self.position.lock().unwrap();
            *position += result.0 as u64;
            return Poll::Ready(result.1.transpose());
        }
        let waker = cx.waker().clone();
        drop(state);

        let size = self.size;
        let limit = self.limit;
        let position = self.position.clone();
        let state = Arc::clone(&self.state);
        spawn_blocking(move || {
            let mut state = state.lock().unwrap();
            let position = position.lock().unwrap();
            let buf_size = std::cmp::min(size, (limit - *position) as usize);
            if buf_size > 0 {
                let mut buf = DmaBuf::new(buf_size);
                let result = match state.reader.read_exact_at(buf.as_mut(), *position) {
                    Ok(_) => Ok(Some(buf)),
                    Err(err) => Err(err),
                };
                drop(position);
                state.result = Some((buf_size, result));
                waker.wake();
            } else {
                state.result = Some((0, Ok(None)));
            }
        });
        Poll::Pending
    }
}
