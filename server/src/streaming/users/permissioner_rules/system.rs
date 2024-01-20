use crate::streaming::users::permissioner::Permissioner;
use iggy::error::IggyError;

impl Permissioner {
    pub fn get_stats(&self, user_id: u32) -> Result<(), IggyError> {
        self.get_server_info(user_id)
    }

    pub fn get_clients(&self, user_id: u32) -> Result<(), IggyError> {
        self.get_server_info(user_id)
    }

    pub fn get_client(&self, user_id: u32) -> Result<(), IggyError> {
        self.get_server_info(user_id)
    }

    fn get_server_info(&self, user_id: u32) -> Result<(), IggyError> {
        if let Some(global_permissions) = self.users_permissions.get(&user_id) {
            if global_permissions.manage_servers || global_permissions.read_servers {
                return Ok(());
            }
        }

        Err(IggyError::Unauthorized)
    }
}
