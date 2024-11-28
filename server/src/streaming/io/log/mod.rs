use futures::Stream;
use super::buf::IoBuf;

pub mod log;

pub trait LogReader<Buf> 
    where Buf: IoBuf
{
    fn read_blocks(
        &self,
        position: u64,
        limit: u64,
    ) -> impl Stream<Item = Result<Buf, std::io::Error>>; 
}