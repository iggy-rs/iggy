use crate::users::permissioner::Permissioner;
use iggy::error::Error;

impl Permissioner {
    pub fn get_stream(&self, user_id: u32, stream_id: u32) -> Result<(), Error> {
        if !self.enabled {
            return Ok(());
        }

        if let Some(global_permissions) = self.users_permissions.get(&user_id) {
            if global_permissions.read_streams || global_permissions.manage_streams {
                return Ok(());
            }
        }

        if self
            .users_streams_permissions
            .contains_key(&(user_id, stream_id))
        {
            return Ok(());
        }

        Err(Error::Unauthorized)
    }

    pub fn get_streams(&self, user_id: u32) -> Result<(), Error> {
        if !self.enabled {
            return Ok(());
        }

        if let Some(global_permissions) = self.users_permissions.get(&user_id) {
            if global_permissions.read_streams || global_permissions.manage_streams {
                return Ok(());
            }
        }

        Err(Error::Unauthorized)
    }

    pub fn create_stream(&self, user_id: u32) -> Result<(), Error> {
        self.manage_streams(user_id)
    }

    pub fn update_stream(&self, user_id: u32) -> Result<(), Error> {
        self.manage_streams(user_id)
    }

    pub fn delete_stream(&self, user_id: u32) -> Result<(), Error> {
        self.manage_streams(user_id)
    }

    fn manage_streams(&self, user_id: u32) -> Result<(), Error> {
        if !self.enabled {
            return Ok(());
        }

        if let Some(global_permissions) = self.users_permissions.get(&user_id) {
            if global_permissions.manage_streams {
                return Ok(());
            }
        }

        Err(Error::Unauthorized)
    }
}
