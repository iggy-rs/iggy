use async_trait::async_trait;

use crate::{
    bytes_serializable::BytesSerializable, error::IggyError, tcp::client::ConnectionStream,
};

/// The trait for serializing a struct into a stream.
#[async_trait]
pub(crate) trait WriteTo: BytesSerializable {
    /// Serialize the struct into the stream.
    async fn write_to(&self, stream: &mut dyn ConnectionStream) -> Result<(), IggyError>;
}
