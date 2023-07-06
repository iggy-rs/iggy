use crate::binary::binary_client::BinaryClient;
use crate::binary::mapper;
use crate::bytes_serializable::BytesSerializable;
use crate::command::{GET_CLIENTS_CODE, GET_CLIENT_CODE, KILL_CODE, PING_CODE};
use crate::error::Error;
use crate::models::client_info::{ClientInfo, ClientInfoDetails};
use crate::system::get_client::GetClient;
use crate::system::get_clients::GetClients;
use crate::system::kill::Kill;
use crate::system::ping::Ping;

pub async fn get_client(
    client: &dyn BinaryClient,
    command: &GetClient,
) -> Result<ClientInfoDetails, Error> {
    let response = client
        .send_with_response(GET_CLIENT_CODE, &command.as_bytes())
        .await?;
    mapper::map_client(&response)
}

pub async fn get_clients(
    client: &dyn BinaryClient,
    command: &GetClients,
) -> Result<Vec<ClientInfo>, Error> {
    let response = client
        .send_with_response(GET_CLIENTS_CODE, &command.as_bytes())
        .await?;
    mapper::map_clients(&response)
}

pub async fn ping(client: &dyn BinaryClient, command: &Ping) -> Result<(), Error> {
    client
        .send_with_response(PING_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

pub async fn kill(client: &dyn BinaryClient, command: &Kill) -> Result<(), Error> {
    client
        .send_with_response(KILL_CODE, &command.as_bytes())
        .await?;
    Ok(())
}
