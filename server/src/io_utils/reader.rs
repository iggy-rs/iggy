use futures::Future;
use monoio::{
    buf::{IoBufMut, IoVecBufMut, IoVecWrapper},
    fs::File,
    io::{AsyncReadRent, AsyncWriteRent, BufReader, BufWriter},
    BufResult,
};
use std::io::SeekFrom;

pub struct IggyReader<R: AsyncReadRent> {
    inner: BufReader<R>,
}

pub struct IggyWriter<W: AsyncWriteRent> {
    inner: BufWriter<W>,
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
        }
    }

    pub fn with_capacity(capacity: usize, file: File) -> Self {
        let file = IggyFile::new(file);
        Self {
            inner: BufReader::with_capacity(capacity, file),
        }
    }

    // Maintaining the api compatibility, this is mostly used with the Start variant.
    /// This method doesn't verify the file bounds, it just sets the position.
    pub fn seek(&mut self, pos: SeekFrom) {
        self.inner.get_mut().seek(pos);
    }
}

impl IggyWriter<IggyFile> {
    pub fn new(file: File) -> Self {
        let file = IggyFile::new(file);
        Self {
            inner: BufWriter::new(file),
        }
    }

    pub fn with_capacity(capacity: usize, file: File) -> Self {
        let file = IggyFile::new(file);
        Self {
            inner: BufWriter::with_capacity(capacity, file),
        }
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

impl AsyncWriteRent for IggyFile {
    async fn write<T: monoio::buf::IoBuf>(
        &mut self,
        buf: T,
    ) -> BufResult<usize, T> {
        let (res, buf) =  self.file.write_at(buf, self.position).await;
        let n = match res {
            Ok(n) => n,
            Err(e) => return (Err(e), buf),
        };
        self.position += n as u64;
        (Ok(n), buf)
    }

    async fn writev<T: monoio::buf::IoVecBuf>(&mut self, buf_vec: T) -> BufResult<usize, T> {
        let slice = match IoVecWrapper::new(buf_vec) {
            Ok(slice) => slice,
            Err(buf) => return (Ok(0), buf),
        };

        let (result, slice) = self.write(slice).await;
        (result, slice.into_inner())
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        self.file.sync_all().await
    }

    //TODO(numinex) - How to implement this ? 
    async fn shutdown(&mut self) -> std::io::Result<()> {
        panic!("shutdown is not supported for IggyFile");
        /*
        self.file.sync_all().await;
        unsafe {
            let file = *self.file;
            file.close().await
        }
*/
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


impl<W> AsyncWriteRent for IggyWriter<W>
where W: AsyncWriteRent {
    fn write<T: monoio::buf::IoBuf>(&mut self, buf: T) -> impl Future<Output = BufResult<usize, T>> {
        self.inner.write(buf)
    }

    fn writev<T: monoio::buf::IoVecBuf>(&mut self, buf_vec: T) -> impl Future<Output = BufResult<usize, T>> {
        self.inner.writev(buf_vec)
    }

    fn flush(&mut self) -> impl Future<Output = std::io::Result<()>> {
        self.inner.flush()
    }

    fn shutdown(&mut self) -> impl Future<Output = std::io::Result<()>> {
        self.inner.shutdown()
    }
}