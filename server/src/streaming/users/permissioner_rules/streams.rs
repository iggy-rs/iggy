use crate::streaming::users::permissioner::Permissioner;
use iggy::error::Error;

impl Permissioner {
    pub fn get_stream(&self, user_id: u32, stream_id: u32) -> Result<(), Error> {
        if !self.enabled {
            return Ok(());
        }

        if let Some(global_permissions) = self.users_permissions.get(&user_id) {
            if global_permissions.manage_streams || global_permissions.read_streams {
                return Ok(());
            }
        }

        if let Some(stream_permissions) = self.users_streams_permissions.get(&(user_id, stream_id))
        {
            if stream_permissions.manage_stream || stream_permissions.read_stream {
                return Ok(());
            }
        }

        Err(Error::Unauthorized)
    }

    pub fn get_streams(&self, user_id: u32) -> Result<(), Error> {
        if !self.enabled {
            return Ok(());
        }

        if let Some(global_permissions) = self.users_permissions.get(&user_id) {
            if global_permissions.manage_streams || global_permissions.read_streams {
                return Ok(());
            }
        }

        Err(Error::Unauthorized)
    }

    pub fn create_stream(&self, user_id: u32) -> Result<(), Error> {
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

    pub fn update_stream(&self, user_id: u32, stream_id: u32) -> Result<(), Error> {
        self.manage_stream(user_id, stream_id)
    }

    pub fn delete_stream(&self, user_id: u32, stream_id: u32) -> Result<(), Error> {
        self.manage_stream(user_id, stream_id)
    }

    fn manage_stream(&self, user_id: u32, stream_id: u32) -> Result<(), Error> {
        if !self.enabled {
            return Ok(());
        }

        if let Some(global_permissions) = self.users_permissions.get(&user_id) {
            if global_permissions.manage_streams {
                return Ok(());
            }
        }

        let stream_permissions = self.users_streams_permissions.get(&(user_id, stream_id));
        if let Some(stream_permissions) = stream_permissions {
            if stream_permissions.manage_stream {
                return Ok(());
            }
        }

        Err(Error::Unauthorized)
    }
}
