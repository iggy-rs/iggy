use std::{marker::PhantomData, pin::Pin};
use futures::task::Poll;
use futures::{Stream, Future, FutureExt};
use pin_project::pin_project;
use crate::streaming::{storage::Storage, io::buf::IoBuf};
use super::LogReader;

#[derive(Debug)]
pub struct Log<S, Buf> 
    where Buf: IoBuf, S: Storage<Buf>
{
    storage: S,
    block_size: usize,
    _phantom: PhantomData<Buf>
}

impl<S, Buf> Log<S, Buf> 
    where Buf: IoBuf, S: Storage<Buf>
{
    pub fn new(storage: S, block_size: usize) -> Self {
        Self {
            storage,
            block_size,
            _phantom: PhantomData,
        }
    }

}

impl<S,Buf> LogReader<Buf> for Log<S,Buf> 
    where Buf: IoBuf, S: Storage<Buf>
{
    fn read_blocks(
        &self,
        position: u64,
        limit: u64,
    ) -> impl Stream<Item = Result<Buf, std::io::Error>> {
        BlockStream {
            position,
            limit,
            future: None,
            storage: &self.storage,
            size: self.block_size,
        }
    }
}

#[pin_project]
struct BlockStream<'a, S, Buf>
    where Buf: IoBuf, S: Storage<Buf>, 
 {
    position: u64,
    limit: u64,
    size: usize,
    storage: &'a S,
    #[pin]
    future: Option<S::ReadResult>,
}

impl<'a, S, Buf> Stream for BlockStream<'a, S, Buf> 
    where Buf: IoBuf, S: Storage<Buf>
{
    type Item = Result<Buf, std::io::Error>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if *this.position > *this.limit {
            return Poll::Ready(None);
        }

        if let Some(fut) = this.future.as_mut().as_pin_mut() {
            let result = futures::ready!(fut.poll(cx));
            *this.position += *this.size as u64;
            this.future.set(None);
            return Poll::Ready(Some(result));
        } else {
            let buf_size = std::cmp::min(*this.size, (*this.limit - *this.position) as usize);
            let buf = Buf::with_capacity(buf_size);
            let mut fut = this
                .storage
                .read_sectors(*this.position, buf);
            match fut.poll_unpin(cx) {
                Poll::Ready(result) => {
                    *this.position += *this.size as u64;
                    Poll::Ready(Some(result))
                }
                Poll::Pending => {
                    this.future.set(Some(fut));
                    Poll::Pending
                }
            }
        }
    }
}