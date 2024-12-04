use super::{LogReader, LogWriter};
use crate::streaming::io::buf::dma_buf::{AreczekDmaBuf, DmaBuf};
use crate::streaming::{io::buf::IoBuf, storage::Storage};
use futures::task::Poll;
use futures::{Future, FutureExt, Stream};
use pin_project::pin_project;
use std::{marker::PhantomData, pin::Pin};


#[derive(Debug)]
pub struct Log<S, Buf>
where
    Buf: IoBuf,
    S: Storage<Buf>,
{
    storage: S,
    block_size: usize,
    _phantom: PhantomData<Buf>,
}

impl<S, Buf> Log<S, Buf>
where
    Buf: IoBuf,
    S: Storage<Buf>,
{
    pub fn new(storage: S, block_size: usize) -> Self {
        Self {
            storage,
            block_size,
            _phantom: PhantomData,
        }
    }
}

impl<S, Buf> LogReader<Buf, AreczekDmaBuf> for Log<S, Buf>
where
    Buf: IoBuf,
    S: Storage<Buf>,
{
    fn read_blocks(
        &self,
        position: u64,
        limit: u64,
    ) -> impl Stream<Item = Result<AreczekDmaBuf, std::io::Error>> {
        BlockStream {
            position,
            limit,
            future: None,
            storage: &self.storage,
            size: self.block_size,
        }
    }
}

impl<S, Buf> LogWriter<Buf> for Log<S, Buf>
where
    Buf: IoBuf,
    S: Storage<Buf>,
{
    fn write_block(
        &mut self,
        buf: Buf,
    ) -> impl Future<Output = Result<u32, iggy::error::IggyError>> {
        self.storage.write_sectors(buf)
    }
}

#[pin_project]
struct BlockStream<'a, S, Buf>
where
    Buf: IoBuf,
    S: Storage<Buf>,
{
    position: u64,
    limit: u64,
    size: usize,
    storage: &'a S,
    #[pin]
    future: Option<S::ReadResult>,
}

impl<'a, S, Buf> Stream for BlockStream<'a, S, Buf>
where
    Buf: IoBuf,
    S: Storage<Buf>,
{
    type Item = Result<AreczekDmaBuf, std::io::Error>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if *this.position >= *this.limit {
            return Poll::Ready(None);
        }
        if let Some(buf) = this.storage.get_from_cache(&*this.position) {
            return Poll::Ready(Some(Ok(buf)));
        }

        if let Some(fut) = this.future.as_mut().as_pin_mut() {
            let result = futures::ready!(fut.poll(cx));
            match result {
                Ok(buf) => {
                    let areczek = buf.into_areczek();
                    this.storage.put_into_cache(*this.position, areczek.clone());
                    *this.position += *this.size as u64;
                    this.future.set(None);
                    return Poll::Ready(Some(Ok(areczek)));
                }
                Err(err) => {
                    return Poll::Ready(Some(Err(err)));
                }
            }
        } else {
            let buf_size = std::cmp::min(*this.size, (*this.limit - *this.position) as usize);
            //*this.size = buf_size;
            let buf = Buf::new(buf_size);
            let mut fut = this.storage.read_sectors(*this.position, buf);
            match fut.poll_unpin(cx) {
                Poll::Ready(result) => match result {
                    Ok(buf) => {
                        let areczek = buf.into_areczek();
                        this.storage.put_into_cache(*this.position, areczek.clone());
                        *this.position += *this.size as u64;
                        this.future.set(None);
                        return Poll::Ready(Some(Ok(areczek)));
                    }
                    Err(err) => {
                        return Poll::Ready(Some(Err(err)));
                    }
                },
                Poll::Pending => {
                    this.future.set(Some(fut));
                    Poll::Pending
                }
            }
        }
    }
}
