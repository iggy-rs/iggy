use futures::task::Poll;
use std::pin::Pin;

use crate::streaming::{
    batching::message_batch::RETAINED_BATCH_OVERHEAD, models::messages::RetainedMessage,
};
use bytes::{BufMut, BytesMut};
use futures::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, FutureExt, Stream};
use iggy::error::IggyError;

pub struct RetainedMessageStream<R>
where
    R: AsyncBufRead + Unpin,
{
    header_read: bool,
    batch_length: usize,
    sector_size: usize,
    read_bytes: usize,
    reader: R,
}

impl<R> RetainedMessageStream<R>
where
    R: AsyncBufRead + Unpin,
{
    pub fn new(reader: R, sector_size: usize) -> Self {
        Self {
            header_read: false,
            batch_length: 0,
            read_bytes: 0,
            sector_size,
            reader,
        }
    }
}

impl<R> Stream for RetainedMessageStream<R>
where
    R: AsyncBufRead + Unpin,
{
    type Item = Result<RetainedMessage, IggyError>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let mut read_exact =
            |buf: &mut [u8], cx: &mut std::task::Context<'_>| -> Poll<Result<(), IggyError>> {
                let mut read_offset = 0;
                while read_offset < buf.len() {
                    let n = match this.reader.read(&mut buf[read_offset..]).poll_unpin(cx)? {
                        Poll::Ready(val) => val,
                        Poll::Pending => {
                            continue;
                        }
                    };
                    read_offset += n;
                }
                Poll::Ready(Ok(()))
            };
        if !this.header_read {
            let mut buf = [0u8; RETAINED_BATCH_OVERHEAD as _];
            if let Err(e) = futures::ready!(read_exact(&mut buf, cx)) {
                return Some(Err(e.into())).into();
            }

            //TODO: maybe we could use more of those fields ??
            let batch_length = u32::from_le_bytes(buf[8..12].try_into().unwrap());
            this.batch_length = batch_length as usize;
            this.read_bytes = 0;
            this.header_read = true;
        }
        assert!(this.batch_length > 0);

        let mut buf = [0u8; 4];
        if let Err(e) = futures::ready!(read_exact(&mut buf, cx)) {
            return Some(Err(e.into())).into();
        }
        let length = u32::from_le_bytes(buf[0..4].try_into().unwrap());

        let mut payload = BytesMut::with_capacity(length as _);
        payload.put_bytes(0, length as _);
        if let Err(e) = futures::ready!(read_exact(&mut payload, cx)) {
            return Some(Err(e.into())).into();
        }
        this.read_bytes += length as usize + 4;
        if this.read_bytes >= this.batch_length {
            // This is a temp solution, to the padding that Direct I/O requires.
            // Later on, we could encode that information in our batch header
            // for example Header { batch_length: usize, padding: usize }
            // and use the padding to advance the reader further.
            let total_batch_length = this.batch_length + RETAINED_BATCH_OVERHEAD as usize;
            let sectors = total_batch_length.div_ceil(this.sector_size);
            let adjusted_size = this.sector_size * sectors;
            let diff = adjusted_size - total_batch_length;
            this.reader.consume_unpin(diff);
            this.header_read = false;
        }

        let message = RetainedMessage::try_from_bytes(payload.freeze()).unwrap();
        Poll::Ready(Some(Ok(message)))
    }
}
