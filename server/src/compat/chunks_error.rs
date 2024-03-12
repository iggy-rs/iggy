use futures::stream::TryChunksError;
use iggy::error::IggyError;

pub trait IntoTryChunksError<T> {
    fn into_try_chunks_error(self) -> TryChunksError<T, IggyError>;
}

impl<T> IntoTryChunksError<T> for IggyError {
    fn into_try_chunks_error(self) -> TryChunksError<T, IggyError> {
        TryChunksError(Vec::new(), self)
    }
}

impl<T> IntoTryChunksError<T> for std::io::Error {
    fn into_try_chunks_error(self) -> TryChunksError<T, IggyError> {
        TryChunksError(Vec::new(), IggyError::from(self))
    }
}
