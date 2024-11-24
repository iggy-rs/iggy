use super::io::buf::IoBuf;
use futures::Stream;

pub mod storage;

pub trait Storage<B>
where
    B: IoBuf,
{
    //TODO: support taking as an input Iterator<Item = B> and used write_vectored.
    //fn write_blocks(&self, buffer: B) -> Result<usize, IggyError>;
    fn read_blocks(
        &self,
        position: u64,
        limit: u64,
    ) -> impl Stream<Item = Result<B, std::io::Error>>;
}
