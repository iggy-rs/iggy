use crate::binary::binary_client::BinaryClient;
use crate::binary::{fail_if_not_authenticated, mapper};
use crate::client::StreamClient;
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
    async fn get_stream(&self, stream_id: &Identifier) -> Result<Option<StreamDetails>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(&GetStream {
                stream_id: stream_id.clone(),
            })
            .await?;
        if response.is_empty() {
            return Ok(None);
        }

        mapper::map_stream(response).map(Some)
    }

    async fn get_streams(&self) -> Result<Vec<Stream>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self.send_with_response(&GetStreams {}).await?;
        mapper::map_streams(response)
    }

    async fn create_stream(
        &self,
        name: &str,
        stream_id: Option<u32>,
    ) -> Result<StreamDetails, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(&CreateStream {
                name: name.to_string(),
                stream_id,
            })
            .await?;
        mapper::map_stream(response)
    }

    async fn update_stream(&self, stream_id: &Identifier, name: &str) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&UpdateStream {
            stream_id: stream_id.clone(),
            name: name.to_string(),
        })
        .await?;
        Ok(())
    }

    async fn delete_stream(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&DeleteStream {
            stream_id: stream_id.clone(),
        })
        .await?;
        Ok(())
    }

    async fn purge_stream(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&PurgeStream {
            stream_id: stream_id.clone(),
        })
        .await?;
        Ok(())
    }
}
