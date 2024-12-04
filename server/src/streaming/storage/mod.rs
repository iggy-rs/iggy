use super::io::buf::{IoBuf, dma_buf::AreczekDmaBuf};
use futures::Future;
use iggy::error::IggyError;

pub mod storage;

pub trait Storage<Buf>
where
    Buf: IoBuf,
{
    type ReadResult: Future<Output = Result<Buf, std::io::Error>> + Unpin;
    //TODO: support taking as an input Iterator<Item = B> and used write_vectored.
    // At this moment the mutable borrow of self is unnecessary, but I'll leave it like that,
    // in case when `File` would be stored directly in the storage, rather than being created every time
    // this method is called.
    fn write_sectors(&mut self, buf: Buf) -> impl Future<Output = Result<u32, IggyError>>;
    fn read_sectors(&self, position: u64, buf: Buf) -> Self::ReadResult;

    fn get_from_cache(&self, position: &u64) -> Option<AreczekDmaBuf>;
    fn put_into_cache(&self, position: u64, buf: AreczekDmaBuf);
}
