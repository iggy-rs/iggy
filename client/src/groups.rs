use sdk::client::Client;
use sdk::client_error::ClientError;
use sdk::groups::create_group::CreateGroup;
use sdk::groups::delete_group::DeleteGroup;
use sdk::groups::get_group::GetGroup;
use sdk::groups::get_groups::GetGroups;
use sdk::groups::join_group::JoinGroup;
use sdk::groups::leave_group::LeaveGroup;
use tracing::info;

pub async fn get_group(command: &GetGroup, client: &dyn Client) -> Result<(), ClientError> {
    let group = client.get_group(command).await?;
    info!("Group: {:#?}", group);
    Ok(())
}

pub async fn get_groups(command: &GetGroups, client: &dyn Client) -> Result<(), ClientError> {
    let groups = client.get_groups(command).await?;
    if groups.is_empty() {
        info!("No groups found");
        return Ok(());
    }

    info!("Group: {:#?}", groups);
    Ok(())
}

pub async fn create_group(command: &CreateGroup, client: &dyn Client) -> Result<(), ClientError> {
    client.create_group(command).await?;
    Ok(())
}

pub async fn delete_group(command: &DeleteGroup, client: &dyn Client) -> Result<(), ClientError> {
    client.delete_group(command).await?;
    Ok(())
}

pub async fn join_group(command: &JoinGroup, client: &dyn Client) -> Result<(), ClientError> {
    client.join_group(command).await?;
    Ok(())
}

pub async fn leave_group(command: &LeaveGroup, client: &dyn Client) -> Result<(), ClientError> {
    client.leave_group(command).await?;
    Ok(())
}
