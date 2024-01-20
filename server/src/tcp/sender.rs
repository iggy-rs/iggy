use iggy::error::IggyError;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::debug;

const STATUS_OK: &[u8] = &[0; 4];

pub(crate) async fn read<T>(stream: &mut T, buffer: &mut [u8]) -> Result<usize, IggyError>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let read_bytes = stream.read_exact(buffer).await;
    if let Err(error) = read_bytes {
        return Err(IggyError::from(error));
    }

    Ok(read_bytes.unwrap())
}

pub(crate) async fn send_empty_ok_response<T>(stream: &mut T) -> Result<(), IggyError>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    send_ok_response(stream, &[]).await
}

pub(crate) async fn send_ok_response<T>(stream: &mut T, payload: &[u8]) -> Result<(), IggyError>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    send_response(stream, STATUS_OK, payload).await
}

pub(crate) async fn send_error_response<T>(
    stream: &mut T,
    error: IggyError,
) -> Result<(), IggyError>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    send_response(stream, &error.as_code().to_le_bytes(), &[]).await
}

pub(crate) async fn send_response<T>(
    stream: &mut T,
    status: &[u8],
    payload: &[u8],
) -> Result<(), IggyError>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    debug!("Sending response with status: {:?}...", status);
    let length = (payload.len() as u32).to_le_bytes();
    stream
        .write_all(&[status, &length, payload].as_slice().concat())
        .await?;
    debug!("Sent response with status: {:?}", status);
    Ok(())
}
