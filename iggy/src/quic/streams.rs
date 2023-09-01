use crate::binary;
use crate::client::StreamClient;
use crate::error::Error;
use crate::models::stream::{Stream, StreamDetails};
use crate::quic::client::QuicClient;
use crate::streams::create_stream::CreateStream;
use crate::streams::delete_stream::DeleteStream;
use crate::streams::get_stream::GetStream;
use crate::streams::get_streams::GetStreams;
use crate::streams::update_stream::UpdateStream;
use async_trait::async_trait;

#[async_trait]
impl StreamClient for QuicClient {
    async fn get_stream(&self, command: &GetStream) -> Result<StreamDetails, Error> {
        binary::streams::get_stream(self, command).await
    }

    async fn get_streams(&self, command: &GetStreams) -> Result<Vec<Stream>, Error> {
        binary::streams::get_streams(self, command).await
    }

    async fn create_stream(&self, command: &CreateStream) -> Result<(), Error> {
        binary::streams::create_stream(self, command).await
    }

    async fn update_stream(&self, command: &UpdateStream) -> Result<(), Error> {
        binary::streams::update_stream(self, command).await
    }

    async fn delete_stream(&self, command: &DeleteStream) -> Result<(), Error> {
        binary::streams::delete_stream(self, command).await
    }
}
