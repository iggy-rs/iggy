use crate::binary::binary_client::{BinaryClient, BinaryClientNext};
use crate::binary::{fail_if_not_authenticated, mapper, BinaryTransport};
use crate::bytes_serializable::BytesSerializable;
use crate::client::StreamClient;
use crate::command::{
    CREATE_STREAM_CODE, DELETE_STREAM_CODE, GET_STREAMS_CODE, GET_STREAM_CODE, PURGE_STREAM_CODE,
    UPDATE_STREAM_CODE,
};
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::models::stream::{Stream, StreamDetails};
use crate::next_client::StreamClientNext;
use crate::streams::create_stream::CreateStream;
use crate::streams::delete_stream::DeleteStream;
use crate::streams::get_stream::GetStream;
use crate::streams::get_streams::GetStreams;
use crate::streams::purge_stream::PurgeStream;
use crate::streams::update_stream::UpdateStream;

#[async_trait::async_trait]
impl<B: BinaryClient> StreamClient for B {
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

#[async_trait::async_trait]
impl<B: BinaryClientNext> StreamClientNext for B {
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

async fn get_stream<T: BinaryTransport>(
    transport: &T,
    command: &GetStream,
) -> Result<StreamDetails, IggyError> {
    fail_if_not_authenticated(transport).await?;
    let response = transport
        .send_with_response(GET_STREAM_CODE, command.as_bytes())
        .await?;
    mapper::map_stream(response)
}

async fn get_streams<T: BinaryTransport>(
    transport: &T,
    command: &GetStreams,
) -> Result<Vec<Stream>, IggyError> {
    fail_if_not_authenticated(transport).await?;
    let response = transport
        .send_with_response(GET_STREAMS_CODE, command.as_bytes())
        .await?;
    mapper::map_streams(response)
}

async fn create_stream<T: BinaryTransport>(
    transport: &T,
    command: &CreateStream,
) -> Result<(), IggyError> {
    fail_if_not_authenticated(transport).await?;
    transport
        .send_with_response(CREATE_STREAM_CODE, command.as_bytes())
        .await?;
    Ok(())
}

async fn update_stream<T: BinaryTransport>(
    transport: &T,
    command: &UpdateStream,
) -> Result<(), IggyError> {
    fail_if_not_authenticated(transport).await?;
    transport
        .send_with_response(UPDATE_STREAM_CODE, command.as_bytes())
        .await?;
    Ok(())
}

async fn delete_stream<T: BinaryTransport>(
    transport: &T,
    command: &DeleteStream,
) -> Result<(), IggyError> {
    fail_if_not_authenticated(transport).await?;
    transport
        .send_with_response(DELETE_STREAM_CODE, command.as_bytes())
        .await?;
    Ok(())
}

async fn purge_stream<T: BinaryTransport>(
    transport: &T,
    command: &PurgeStream,
) -> Result<(), IggyError> {
    fail_if_not_authenticated(transport).await?;
    transport
        .send_with_response(PURGE_STREAM_CODE, command.as_bytes())
        .await?;
    Ok(())
}
