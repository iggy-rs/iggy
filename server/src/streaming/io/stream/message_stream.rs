use futures::task::Poll;
use pin_project::pin_project;
use std::pin::Pin;
use tokio::task::yield_now;

use crate::streaming::{
    batching::message_batch::RETAINED_BATCH_OVERHEAD, io, models::messages::RetainedMessage,
};
use bytes::{BufMut, BytesMut};
use futures::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, FutureExt, Stream};
use iggy::error::IggyError;

use tracing::error;

#[pin_project]
pub struct RetainedMessageStream<R>
where
    R: AsyncBufRead + Unpin,
{
    header_read: bool,
    batch_length: u64,
    sector_size: u64,
    read_bytes: u64,
    message_length: u32,
    state: State,
    #[pin]
    reader: R,
}

impl<R> RetainedMessageStream<R>
where
    R: AsyncBufRead + Unpin,
{
    pub fn new(reader: R, sector_size: u64) -> Self {
        Self {
            header_read: false,
            batch_length: 0,
            read_bytes: 0,
            state: State::Ready,
            message_length: 0,
            sector_size,
            reader,
        }
    }
}

#[derive(Copy, Clone)]
enum Reading {
    Header,
    Length,
    Message,
}

enum State {
    Ready,
    Pending(Reading, usize, Vec<u8>),
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
        let mut this = self.project();
        let state = std::mem::replace(this.state, State::Ready);

        let mut read_exact = |reading: Reading,
                              buf: &mut [u8],
                              cx: &mut std::task::Context<'_>|
         -> Poll<Result<(), IggyError>> {
            let mut read_offset = 0;
            while read_offset < buf.len() {
                let n = match this.reader.read(&mut buf[read_offset..]).poll_unpin(cx)? {
                    Poll::Ready(val) => val,
                    Poll::Pending => {
                        let len = buf.len();
                        let mut new_buf = vec![0; len];
                        new_buf.copy_from_slice(&buf);
                        *this.state = State::Pending(reading, read_offset, new_buf);
                        return Poll::Pending;
                    }
                };
                read_offset += n;
            }
            Poll::Ready(Ok(()))
        };

        match state {
            State::Ready => {}
            State::Pending(reading, read, mut buf) => {
                match reading {
                    Reading::Header => {
                        if !*this.header_read {
                            if let Err(e) =
                                futures::ready!(read_exact(Reading::Header, &mut buf[read..], cx))
                            {
                                return Some(Err(e.into())).into();
                            }

                            //TODO: maybe we could use more of those fields ??
                            let batch_length = u32::from_le_bytes(buf[8..12].try_into().unwrap());
                            *this.batch_length = batch_length as _;
                            *this.read_bytes = 0;
                            *this.header_read = true;
                        }
                    }
                    Reading::Length => {
                        if let Err(e) =
                            futures::ready!(read_exact(Reading::Length, &mut buf[read..], cx))
                        {
                            return Some(Err(e.into())).into();
                        }
                        let length = u32::from_le_bytes(buf[0..4].try_into().unwrap());
                        *this.message_length = length;

                        let mut payload = BytesMut::with_capacity(length as _);
                        payload.put_bytes(0, length as _);
                        if let Err(e) =
                            futures::ready!(read_exact(Reading::Message, &mut payload, cx))
                        {
                            return Some(Err(e.into())).into();
                        }
                        *this.read_bytes += length as u64 + 4;
                        if this.read_bytes >= this.batch_length {
                            // This is a temp solution, to the padding that Direct I/O requires.
                            // Later on, we could encode that information in our batch header
                            // for example Header { batch_length: usize, padding: usize }
                            // and use the padding to advance the reader further.
                            /*
                            let total_batch_length = *this.batch_length + RETAINED_BATCH_OVERHEAD;
                            let adjusted_size = io::val_align_up(total_batch_length, *this.sector_size);
                            */
                            let total_batch_length = *this.batch_length + RETAINED_BATCH_OVERHEAD;
                            let sectors = total_batch_length.div_ceil(*this.sector_size);
                            let adjusted_size = *this.sector_size * sectors;
                            let diff = adjusted_size - total_batch_length;
                            this.reader.consume_unpin(diff as _);
                            *this.header_read = false;
                        }

                        let message = RetainedMessage::try_from_bytes(payload.freeze()).unwrap();
                        return Poll::Ready(Some(Ok(message)));
                    }
                    Reading::Message => {
                        if let Err(e) =
                            futures::ready!(read_exact(Reading::Message, &mut buf[read..], cx))
                        {
                            return Some(Err(e.into())).into();
                        }
                        *this.read_bytes += *this.message_length as u64 + 4;
                        if this.read_bytes >= this.batch_length {
                            // This is a temp solution, to the padding that Direct I/O requires.
                            // Later on, we could encode that information in our batch header
                            // for example Header { batch_length: usize, padding: usize }
                            // and use the padding to advance the reader further.
                            /*
                            let total_batch_length = *this.batch_length + RETAINED_BATCH_OVERHEAD;
                            let adjusted_size = io::val_align_up(total_batch_length, *this.sector_size);
                            */
                            let total_batch_length = *this.batch_length + RETAINED_BATCH_OVERHEAD;
                            let sectors = total_batch_length.div_ceil(*this.sector_size);
                            let adjusted_size = *this.sector_size * sectors;
                            let diff = adjusted_size - total_batch_length;
                            this.reader.consume_unpin(diff as _);
                            *this.header_read = false;
                        }

                        let message = RetainedMessage::try_from_bytes(buf.into()).unwrap();
                        return Poll::Ready(Some(Ok(message)));
                    }
                }
            }
        }

        if !*this.header_read {
            let mut buf = [0u8; RETAINED_BATCH_OVERHEAD as _];
            if let Err(e) = futures::ready!(read_exact(Reading::Header, &mut buf, cx)) {
                return Some(Err(e.into())).into();
            }

            //TODO: maybe we could use more of those fields ??
            let batch_length = u32::from_le_bytes(buf[8..12].try_into().unwrap());
            *this.batch_length = batch_length as _;
            *this.read_bytes = 0;
            *this.header_read = true;
        }
        assert!(*this.batch_length > 0);

        let mut buf = [0u8; 4];
        if let Err(e) = futures::ready!(read_exact(Reading::Length, &mut buf, cx)) {
            return Some(Err(e.into())).into();
        }
        let length = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        *this.message_length = length;

        let mut payload = BytesMut::with_capacity(length as _);
        payload.put_bytes(0, length as _);
        if let Err(e) = futures::ready!(read_exact(Reading::Message, &mut payload, cx)) {
            return Some(Err(e.into())).into();
        }
        *this.read_bytes += length as u64 + 4;
        if this.read_bytes >= this.batch_length {
            // This is a temp solution, to the padding that Direct I/O requires.
            // Later on, we could encode that information in our batch header
            // for example Header { batch_length: usize, padding: usize }
            // and use the padding to advance the reader further.
            /*
            let total_batch_length = *this.batch_length + RETAINED_BATCH_OVERHEAD;
            let adjusted_size = io::val_align_up(total_batch_length, *this.sector_size);
            */
            let total_batch_length = *this.batch_length + RETAINED_BATCH_OVERHEAD;
            let sectors = total_batch_length.div_ceil(*this.sector_size);
            let adjusted_size = *this.sector_size * sectors;
            let diff = adjusted_size - total_batch_length;
            this.reader.consume_unpin(diff as _);
            *this.header_read = false;
        }

        let message = RetainedMessage::try_from_bytes(payload.freeze()).unwrap();
        Poll::Ready(Some(Ok(message)))
    }
}
