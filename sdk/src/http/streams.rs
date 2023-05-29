use crate::error::Error;
use crate::http::client::Client;
use crate::stream::Stream;
use shared::streams::create_stream::CreateStream;
use shared::streams::delete_stream::DeleteStream;

const PATH: &str = "/streams";

impl Client {
    pub async fn get_streams(&self) -> Result<Vec<Stream>, Error> {
        let response = self.get(PATH).await?;
        let streams = response.json().await?;
        Ok(streams)
    }

    pub async fn create_stream(&self, command: &CreateStream) -> Result<(), Error> {
        self.post(PATH, command).await?;
        Ok(())
    }

    pub async fn delete_stream(&self, command: &DeleteStream) -> Result<(), Error> {
        let path = format!("{}/{}", PATH, command.stream_id);
        self.delete(&path).await?;
        Ok(())
    }
}
