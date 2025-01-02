use super::io::buf::IoBuf;
use crate::server_error::IoError;
use futures::Future;

pub mod dma_storage;

pub trait Storage<Buf>
where
    Buf: IoBuf,
{
    type ReadResult: Future<Output = Result<Buf, std::io::Error>> + Unpin;
    //TODO: support taking as an input Iterator<Item = B> and used write_vectored.
    // At this moment the mutable borrow of self is unnecessary, but I'll leave it like that,
    // in case when `File` would be stored directly in the storage, rather than being created every time
    // this method is called.
    fn write_sectors(&mut self, buf: Buf) -> impl Future<Output = Result<u32, IoError>>;
    fn read_sectors(&self, position: u64, buf: Buf) -> Self::ReadResult;
}
