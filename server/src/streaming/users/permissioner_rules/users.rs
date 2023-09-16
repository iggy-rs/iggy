use crate::streaming::users::permissioner::Permissioner;
use iggy::error::Error;

impl Permissioner {
    pub fn create_user(&self, user_id: u32) -> Result<(), Error> {
        self.manager_users(user_id)
    }

    pub fn delete_user(&self, user_id: u32) -> Result<(), Error> {
        self.manager_users(user_id)
    }

    pub fn update_user(&self, user_id: u32) -> Result<(), Error> {
        self.manager_users(user_id)
    }

    pub fn update_permissions(&self, user_id: u32) -> Result<(), Error> {
        self.manager_users(user_id)
    }

    fn manager_users(&self, user_id: u32) -> Result<(), Error> {
        if !self.enabled {
            return Ok(());
        }

        if let Some(global_permissions) = self.users_permissions.get(&user_id) {
            if global_permissions.manage_users {
                return Ok(());
            }
        }

        Err(Error::Unauthorized)
    }
}
