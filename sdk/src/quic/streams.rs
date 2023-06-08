use crate::binary;
use crate::client::StreamClient;
use crate::error::Error;
use crate::quic::client::QuicClient;
use crate::stream::Stream;
use async_trait::async_trait;
use shared::streams::create_stream::CreateStream;
use shared::streams::delete_stream::DeleteStream;
use shared::streams::get_streams::GetStreams;

#[async_trait]
impl StreamClient for QuicClient {
    async fn get_streams(&self, command: GetStreams) -> Result<Vec<Stream>, Error> {
        binary::streams::get_streams(self, command).await
    }

    async fn create_stream(&self, command: CreateStream) -> Result<(), Error> {
        binary::streams::create_stream(self, command).await
    }

    async fn delete_stream(&self, command: DeleteStream) -> Result<(), Error> {
        binary::streams::delete_stream(self, command).await
    }
}
