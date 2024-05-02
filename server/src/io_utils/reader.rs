use futures::Future;
use monoio::{
    buf::{IoBufMut, IoVecBufMut},
    fs::File,
    io::{AsyncReadRent, BufReader},
    BufResult,
};
use std::io::SeekFrom;

pub struct IggyReader<R: AsyncReadRent> {
    inner: BufReader<R>,
    file: IggyFile,
}

pub struct IggyFile {
    file: File,
    position: u64,
}

impl IggyFile {
    pub fn new(file: File) -> Self {
        Self { file, position: 0 }
    }

    // Maintaining the api compatibility, this is mostly used with the Start variant.
    /// This method doesn't verify the file bounds, it just sets the position.
    pub fn seek(&mut self, pos: SeekFrom) {
        self.position = match pos {
            SeekFrom::Start(n) => n as i64,
            SeekFrom::End(n) => n,
            SeekFrom::Current(n) => self.position as i64 + n,
        } as u64;
    }
}

impl IggyReader<IggyFile> {
    pub fn new(file: File) -> Self {
        let file = IggyFile::new(file);
        Self {
            inner: BufReader::new(file),
            file,
        }
    }

    pub fn with_capacity(capacity: usize, file: File) -> Self {
        let file = IggyFile::new(file);
        Self {
            inner: BufReader::with_capacity(capacity, file),
            file,
        }
    }

    // Maintaining the api compatibility, this is mostly used with the Start variant.
    /// This method doesn't verify the file bounds, it just sets the position.
    pub fn seek(&mut self, pos: SeekFrom) {
        self.file.position = match pos {
            SeekFrom::Start(n) => n as i64,
            SeekFrom::End(n) => n,
            SeekFrom::Current(n) => self.file.position as i64 + n,
        } as u64;
    }
}

impl AsyncReadRent for IggyFile {
    async fn read<T: IoBufMut>(&mut self, buf: T) -> BufResult<usize, T> {
        let (res, buf) = self.file.read_at(buf, self.position).await;
        let n = match res {
            Ok(n) => n,
            Err(e) => return (Err(e), buf),
        };
        self.position += n as u64;
        (Ok(n), buf)
    }

    //TODO(numinex) - maybe implement this ?
    fn readv<T: IoVecBufMut>(&mut self, buf: T) -> impl Future<Output = BufResult<usize, T>> {
        async move { (Ok(0), buf) }
    }
}

impl<R> AsyncReadRent for IggyReader<R>
where
    R: AsyncReadRent,
{
    fn read<T: IoBufMut>(&mut self, buf: T) -> impl Future<Output = BufResult<usize, T>> {
        self.inner.read(buf)
    }

    fn readv<T: IoVecBufMut>(&mut self, buf: T) -> impl Future<Output = BufResult<usize, T>> {
        self.inner.readv(buf)
    }
}
