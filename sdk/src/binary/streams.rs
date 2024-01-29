use crate::binary::binary_client::BinaryClient;
use crate::binary::{fail_if_not_authenticated, mapper};
use crate::bytes_serializable::BytesSerializable;
use crate::client::StreamClient;
use crate::command::{
    CREATE_STREAM_CODE, DELETE_STREAM_CODE, GET_STREAMS_CODE, GET_STREAM_CODE, PURGE_STREAM_CODE,
    UPDATE_STREAM_CODE,
};
use crate::error::IggyError;
use crate::models::stream::{Stream, StreamDetails};
use crate::streams::create_stream::CreateStream;
use crate::streams::delete_stream::DeleteStream;
use crate::streams::get_stream::GetStream;
use crate::streams::get_streams::GetStreams;
use crate::streams::purge_stream::PurgeStream;
use crate::streams::update_stream::UpdateStream;

#[async_trait::async_trait]
impl<B: BinaryClient> StreamClient for B {
    async fn get_stream(&self, command: &GetStream) -> Result<StreamDetails, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(GET_STREAM_CODE, command.as_bytes())
            .await?;
        mapper::map_stream(response)
    }

    async fn get_streams(&self, command: &GetStreams) -> Result<Vec<Stream>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(GET_STREAMS_CODE, command.as_bytes())
            .await?;
        mapper::map_streams(response)
    }

    async fn create_stream(&self, command: &CreateStream) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(CREATE_STREAM_CODE, command.as_bytes())
            .await?;
        Ok(())
    }

    async fn delete_stream(&self, command: &DeleteStream) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(DELETE_STREAM_CODE, command.as_bytes())
            .await?;
        Ok(())
    }

    async fn update_stream(&self, command: &UpdateStream) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(UPDATE_STREAM_CODE, command.as_bytes())
            .await?;
        Ok(())
    }

    async fn purge_stream(&self, command: &PurgeStream) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(PURGE_STREAM_CODE, command.as_bytes())
            .await?;
        Ok(())
    }
}
