use crate::binary::binary_client::BinaryClient;
use crate::binary::{fail_if_not_authenticated, mapper};
use crate::bytes_serializable::BytesSerializable;
use crate::client::StreamClient;
use crate::command::{
    CREATE_STREAM_CODE, DELETE_STREAM_CODE, GET_STREAMS_CODE, GET_STREAM_CODE, PURGE_STREAM_CODE,
    UPDATE_STREAM_CODE,
};
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::models::stream::{Stream, StreamDetails};
use crate::streams::create_stream::CreateStream;
use crate::streams::delete_stream::DeleteStream;
use crate::streams::get_stream::GetStream;
use crate::streams::get_streams::GetStreams;
use crate::streams::purge_stream::PurgeStream;
use crate::streams::update_stream::UpdateStream;

#[async_trait::async_trait]
impl<B: BinaryClient> StreamClient for B {
    async fn get_stream(&self, stream_id: &Identifier) -> Result<StreamDetails, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(
                GET_STREAM_CODE,
                GetStream {
                    stream_id: stream_id.clone(),
                }
                .to_bytes(),
            )
            .await?;
        mapper::map_stream(response)
    }

    async fn get_streams(&self) -> Result<Vec<Stream>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(GET_STREAMS_CODE, GetStreams {}.to_bytes())
            .await?;
        mapper::map_streams(response)
    }

    async fn create_stream(&self, name: &str, stream_id: Option<u32>) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(
            CREATE_STREAM_CODE,
            CreateStream {
                name: name.to_string(),
                stream_id,
            }
            .to_bytes(),
        )
        .await?;
        Ok(())
    }

    async fn update_stream(&self, stream_id: &Identifier, name: &str) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(
            UPDATE_STREAM_CODE,
            UpdateStream {
                stream_id: stream_id.clone(),
                name: name.to_string(),
            }
            .to_bytes(),
        )
        .await?;
        Ok(())
    }

    async fn delete_stream(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(
            DELETE_STREAM_CODE,
            DeleteStream {
                stream_id: stream_id.clone(),
            }
            .to_bytes(),
        )
        .await?;
        Ok(())
    }

    async fn purge_stream(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(
            PURGE_STREAM_CODE,
            PurgeStream {
                stream_id: stream_id.clone(),
            }
            .to_bytes(),
        )
        .await?;
        Ok(())
    }
}
