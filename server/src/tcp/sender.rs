use bytes::{BufMut, BytesMut};
use iggy::error::IggyError;
use monoio::io::{AsyncReadRent, AsyncReadRentExt, AsyncWriteRent, AsyncWriteRentExt};
use std::mem::size_of;
use tracing::debug;

const STATUS_OK: &[u8] = &[0; 4];

pub(crate) async fn read<T>(
    stream: &mut T,
    buffer: BytesMut,
) -> (Result<usize, IggyError>, BytesMut)
where
    T: AsyncReadRent + AsyncWriteRent + Unpin,
{
    match stream.read_exact(buffer).await {
        (Ok(0), buffer) => (Err(IggyError::ConnectionClosed), buffer),
        (Ok(read_bytes), buffer) => (Ok(read_bytes), buffer),
        (Err(error), buffer) => {
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                (Err(IggyError::ConnectionClosed), buffer)
            } else {
                (Err(IggyError::from(error)), buffer)
            }
        }
    }
}

pub(crate) async fn send_empty_ok_response<T>(stream: &mut T) -> Result<(), IggyError>
where
    T: AsyncReadRent + AsyncWriteRent + Unpin,
{
    send_ok_response(stream, &[]).await
}

pub(crate) async fn send_ok_response<T>(stream: &mut T, payload: &[u8]) -> Result<(), IggyError>
where
    T: AsyncReadRent + AsyncWriteRent + Unpin,
{
    send_response(stream, STATUS_OK, payload).await
}

pub(crate) async fn send_error_response<T>(
    stream: &mut T,
    error: IggyError,
) -> Result<(), IggyError>
where
    T: AsyncReadRent + AsyncWriteRent + Unpin,
{
    let error_message = error.to_string();
    let length = error_message.len() as u32;

    let mut error_details_buffer = BytesMut::with_capacity(error_message.len() + size_of::<u32>());
    error_details_buffer.put_u32_le(length);
    error_details_buffer.put_slice(error_message.as_bytes());

    send_response(
        stream,
        &error.as_code().to_le_bytes(),
        &error_details_buffer,
    )
    .await
}

pub(crate) async fn send_response<T>(
    stream: &mut T,
    status: &[u8],
    payload: &[u8],
) -> Result<(), IggyError>
where
    T: AsyncReadRent + AsyncWriteRentExt + Unpin,
{
    debug!("Sending response with status: {:?}...", status);
    let length = (payload.len() as u32).to_le_bytes();
    let result = stream.write_all([status, &length, payload].concat()).await;

    if result.0.is_err() {
        return Err(IggyError::from(result.0.unwrap_err()));
    }
    debug!("Sent response with status: {:?}", status);
    Ok(())
}
