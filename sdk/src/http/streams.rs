use crate::client::StreamClient;
use crate::error::IggyError;
use crate::http::client::HttpClient;
use crate::http::HttpTransport;
use crate::identifier::Identifier;
use crate::models::stream::{Stream, StreamDetails};
use crate::streams::create_stream::CreateStream;
use crate::streams::update_stream::UpdateStream;
use async_trait::async_trait;

const PATH: &str = "/streams";

#[async_trait]
impl StreamClient for HttpClient {
    async fn get_stream(&self, stream_id: &Identifier) -> Result<Option<StreamDetails>, IggyError> {
        let response = self.get(&get_details_path(&stream_id.as_cow_str())).await?;
        if response.status() == 404 {
            return Ok(None);
        }

        let stream = response.json().await?;
        Ok(Some(stream))
    }

    async fn get_streams(&self) -> Result<Vec<Stream>, IggyError> {
        let response = self.get(PATH).await?;
        let streams = response.json().await?;
        Ok(streams)
    }

    async fn create_stream(
        &self,
        name: &str,
        stream_id: Option<u32>,
    ) -> Result<StreamDetails, IggyError> {
        let response = self
            .post(
                PATH,
                &CreateStream {
                    name: name.to_string(),
                    stream_id,
                },
            )
            .await?;
        let stream = response.json().await?;
        Ok(stream)
    }

    async fn update_stream(&self, stream_id: &Identifier, name: &str) -> Result<(), IggyError> {
        self.put(
            &get_details_path(&stream_id.as_cow_str()),
            &UpdateStream {
                stream_id: stream_id.clone(),
                name: name.to_string(),
            },
        )
        .await?;
        Ok(())
    }

    async fn delete_stream(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        self.delete(&get_details_path(&stream_id.as_cow_str()))
            .await?;
        Ok(())
    }

    async fn purge_stream(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        self.delete(&format!(
            "{}/purge",
            get_details_path(&stream_id.as_cow_str())
        ))
        .await?;
        Ok(())
    }
}

fn get_details_path(stream_id: &str) -> String {
    format!("{PATH}/{stream_id}")
}
