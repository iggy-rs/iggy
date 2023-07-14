use crate::client::StreamClient;
use crate::error::Error;
use crate::http::client::HttpClient;
use crate::models::stream::{Stream, StreamDetails};
use crate::streams::create_stream::CreateStream;
use crate::streams::delete_stream::DeleteStream;
use crate::streams::get_stream::GetStream;
use crate::streams::get_streams::GetStreams;
use async_trait::async_trait;

const PATH: &str = "/streams";

#[async_trait]
impl StreamClient for HttpClient {
    async fn get_stream(&self, command: &GetStream) -> Result<StreamDetails, Error> {
        let response = self.get(&format!("{}/{}", PATH, command.stream_id)).await?;
        let stream = response.json().await?;
        Ok(stream)
    }

    async fn get_streams(&self, _command: &GetStreams) -> Result<Vec<Stream>, Error> {
        let response = self.get(PATH).await?;
        let streams = response.json().await?;
        Ok(streams)
    }

    async fn create_stream(&self, command: &CreateStream) -> Result<(), Error> {
        self.post(PATH, &command).await?;
        Ok(())
    }

    async fn delete_stream(&self, command: &DeleteStream) -> Result<(), Error> {
        let path = format!("{}/{}", PATH, command.stream_id);
        self.delete(&path).await?;
        Ok(())
    }
}
