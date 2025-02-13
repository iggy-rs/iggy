use crate::client::Client;
use crate::clients::client::IggyClient;
use crate::error::IggyError;

/// Builds an `IggyClient` from the given connection string.
///
/// # Arguments
///
/// * `connection_string` - The connection string to use.
///
/// # Errors
///
/// * `IggyError` - If the connection string is invalid or the client cannot be initialized.
///
/// # Details
///
/// This function will create a new `IggyClient` with the given `connection_string`.
/// It will then connect to the server using the provided connection string.
/// If the connection string is invalid or the client cannot be initialized,
/// an `IggyError` will be returned.
///
pub(crate) async fn build_iggy_client(connection_string: &str) -> Result<IggyClient, IggyError> {
    let client = match IggyClient::from_connection_string(connection_string) {
        Ok(client) => client,
        Err(err) => return Err(err),
    };

    match client.connect().await {
        Ok(_) => {}
        Err(err) => return Err(err),
    };

    Ok(client)
}
