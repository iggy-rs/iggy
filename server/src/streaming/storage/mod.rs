use super::io::buf::IoBuf;
use futures::Future;

pub mod storage;

pub trait Storage<Buf>
    where Buf: IoBuf
{
    type ReadResult: Future<Output = Result<Buf, std::io::Error>> + Unpin;
    //TODO: support taking as an input Iterator<Item = B> and used write_vectored.
    //fn write_blocks(&self, buffer: B) -> Result<usize, IggyError>;
    fn read_sectors(
        &self,
        position: u64,
        buf: Buf,
    ) -> Self::ReadResult;
}
