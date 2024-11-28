use super::buf::IoBuf;
use futures::{Future, Stream};
use iggy::error::IggyError;

pub mod log;

pub trait LogReader<Buf>
where
    Buf: IoBuf,
{
    fn read_blocks(
        &self,
        position: u64,
        limit: u64,
    ) -> impl Stream<Item = Result<Buf, std::io::Error>>;
}

pub trait LogWriter<Buf>
where
    Buf: IoBuf,
{
    //TODO: when `write_vectored` is supported in the `Storage` layer, rename to `write_blocks` and accept
    // iterator of bufs instead of buf.
    fn write_block(&mut self, buf: Buf) -> impl Future<Output = Result<u32, IggyError>>;
}
