use crate::streaming::users::permissioner::Permissioner;
use iggy::error::IggyError;

impl Permissioner {
    pub fn get_user(&self, user_id: u32) -> Result<(), IggyError> {
        self.read_users(user_id)
    }

    pub fn get_users(&self, user_id: u32) -> Result<(), IggyError> {
        self.read_users(user_id)
    }

    pub fn create_user(&self, user_id: u32) -> Result<(), IggyError> {
        self.manager_users(user_id)
    }

    pub fn delete_user(&self, user_id: u32) -> Result<(), IggyError> {
        self.manager_users(user_id)
    }

    pub fn update_user(&self, user_id: u32) -> Result<(), IggyError> {
        self.manager_users(user_id)
    }

    pub fn update_permissions(&self, user_id: u32) -> Result<(), IggyError> {
        self.manager_users(user_id)
    }

    pub fn change_password(&self, user_id: u32) -> Result<(), IggyError> {
        self.manager_users(user_id)
    }

    fn manager_users(&self, user_id: u32) -> Result<(), IggyError> {
        if let Some(global_permissions) = self.users_permissions.get(&user_id) {
            if global_permissions.manage_users {
                return Ok(());
            }
        }

        Err(IggyError::Unauthorized)
    }

    fn read_users(&self, user_id: u32) -> Result<(), IggyError> {
        if let Some(global_permissions) = self.users_permissions.get(&user_id) {
            if global_permissions.manage_users || global_permissions.read_users {
                return Ok(());
            }
        }

        Err(IggyError::Unauthorized)
    }
}
