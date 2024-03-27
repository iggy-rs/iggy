use crate::client::StreamClient;
use crate::error::IggyError;
use crate::http::client::HttpClient;
use crate::http::HttpTransport;
use crate::identifier::Identifier;
use crate::models::stream::{Stream, StreamDetails};
use crate::next_client::StreamClientNext;
use crate::streams::create_stream::CreateStream;
use crate::streams::delete_stream::DeleteStream;
use crate::streams::get_stream::GetStream;
use crate::streams::get_streams::GetStreams;
use crate::streams::purge_stream::PurgeStream;
use crate::streams::update_stream::UpdateStream;
use async_trait::async_trait;

const PATH: &str = "/streams";

#[async_trait]
impl StreamClient for HttpClient {
    async fn get_stream(&self, command: &GetStream) -> Result<StreamDetails, IggyError> {
        get_stream(self, command).await
    }

    async fn get_streams(&self, command: &GetStreams) -> Result<Vec<Stream>, IggyError> {
        get_streams(self, command).await
    }

    async fn create_stream(&self, command: &CreateStream) -> Result<(), IggyError> {
        create_stream(self, command).await
    }

    async fn update_stream(&self, command: &UpdateStream) -> Result<(), IggyError> {
        update_stream(self, command).await
    }

    async fn delete_stream(&self, command: &DeleteStream) -> Result<(), IggyError> {
        delete_stream(self, command).await
    }

    async fn purge_stream(&self, command: &PurgeStream) -> Result<(), IggyError> {
        purge_stream(self, command).await
    }
}

#[async_trait]
impl StreamClientNext for HttpClient {
    async fn get_stream(&self, stream_id: &Identifier) -> Result<StreamDetails, IggyError> {
        get_stream(
            self,
            &GetStream {
                stream_id: stream_id.clone(),
            },
        )
        .await
    }

    async fn get_streams(&self) -> Result<Vec<Stream>, IggyError> {
        get_streams(self, &GetStreams {}).await
    }

    async fn create_stream(&self, name: &str, stream_id: Option<u32>) -> Result<(), IggyError> {
        create_stream(
            self,
            &CreateStream {
                name: name.to_string(),
                stream_id,
            },
        )
        .await
    }

    async fn update_stream(&self, stream_id: &Identifier, name: &str) -> Result<(), IggyError> {
        update_stream(
            self,
            &UpdateStream {
                stream_id: stream_id.clone(),
                name: name.to_string(),
            },
        )
        .await
    }

    async fn delete_stream(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        delete_stream(
            self,
            &DeleteStream {
                stream_id: stream_id.clone(),
            },
        )
        .await
    }

    async fn purge_stream(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        purge_stream(
            self,
            &PurgeStream {
                stream_id: stream_id.clone(),
            },
        )
        .await
    }
}

async fn get_stream<T: HttpTransport>(
    transport: &T,
    command: &GetStream,
) -> Result<StreamDetails, IggyError> {
    let response = transport
        .get(&get_details_path(&command.stream_id.as_cow_str()))
        .await?;
    let stream = response.json().await?;
    Ok(stream)
}

async fn get_streams<T: HttpTransport>(
    transport: &T,
    _command: &GetStreams,
) -> Result<Vec<Stream>, IggyError> {
    let response = transport.get(PATH).await?;
    let streams = response.json().await?;
    Ok(streams)
}

async fn create_stream<T: HttpTransport>(
    transport: &T,
    command: &CreateStream,
) -> Result<(), IggyError> {
    transport.post(PATH, &command).await?;
    Ok(())
}

async fn update_stream<T: HttpTransport>(
    transport: &T,
    command: &UpdateStream,
) -> Result<(), IggyError> {
    transport
        .put(&get_details_path(&command.stream_id.as_cow_str()), command)
        .await?;
    Ok(())
}

async fn delete_stream<T: HttpTransport>(
    transport: &T,
    command: &DeleteStream,
) -> Result<(), IggyError> {
    let path = format!("{}/{}", PATH, command.stream_id.as_cow_str());
    transport.delete(&path).await?;
    Ok(())
}

async fn purge_stream<T: HttpTransport>(
    transport: &T,
    command: &PurgeStream,
) -> Result<(), IggyError> {
    let path = format!("{}/{}/purge", PATH, command.stream_id.as_cow_str());
    transport.delete(&path).await?;
    Ok(())
}

fn get_details_path(stream_id: &str) -> String {
    format!("{PATH}/{stream_id}")
}
