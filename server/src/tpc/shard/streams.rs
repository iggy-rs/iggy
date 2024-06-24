use crate::streaming::session::Session;
use crate::streaming::streams::stream::Stream;
use futures::future::try_join_all;
use iggy::error::IggyError;
use iggy::identifier::{IdKind, Identifier};
use iggy::locking::IggySharedMutFn;
use iggy::utils::text;
use iggy::utils::text::IggyStringUtils;
use std::borrow::BorrowMut;
use std::cell::{Ref, RefCell, RefMut};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use tracing::{error, info};

use super::shard::IggyShard;

static CURRENT_STREAM_ID: AtomicU32 = AtomicU32::new(1);

impl IggyShard {
    pub(crate) async fn load_streams(&self) -> Result<(), IggyError> {
        info!("Loading streams from disk...");
        let mut unloaded_streams = Vec::new();
        let dir_entries = std::fs::read_dir(&self.config.system.get_streams_path());
        if let Err(error) = dir_entries {
            error!("Cannot read streams directory: {}", error);
            return Err(IggyError::CannotReadStreams);
        }

        let mut dir_entries = dir_entries.unwrap();
        while let Some(dir_entry) = dir_entries.next() {
            let dir_entry = dir_entry.unwrap();
            let name = dir_entry.file_name().into_string().unwrap();
            let stream_id = name.parse::<u32>();
            if stream_id.is_err() {
                error!("Invalid stream ID file with name: '{}'.", name);
                continue;
            }

            let stream_id = stream_id.unwrap();
            let stream = Stream::empty(stream_id, self.config.system.clone(), self.storage.clone());
            unloaded_streams.push(stream);
        }

        let loaded_streams = RefCell::new(Vec::new());
        let load_stream_tasks = unloaded_streams.into_iter().map(|mut stream| {
            let load_stream_task = async {
                stream.load().await?;
                loaded_streams.borrow_mut().push(stream);
                Result::<(), IggyError>::Ok(())
            };
            load_stream_task
        });
        try_join_all(load_stream_tasks).await?;

        for stream in loaded_streams.take() {
            if self.streams.borrow().contains_key(&stream.stream_id) {
                error!("Stream with ID: '{}' already exists.", &stream.stream_id);
                continue;
            }

            if self.streams_ids.borrow().contains_key(&stream.name) {
                error!("Stream with name: '{}' already exists.", &stream.name);
                continue;
            }

            self.metrics.increment_streams(1);
            self.metrics.increment_topics(stream.get_topics_count());
            self.metrics
                .increment_partitions(stream.get_partitions_count());
            self.metrics.increment_segments(stream.get_segments_count());
            self.metrics.increment_messages(stream.get_messages_count());

            self.streams_ids
                .borrow_mut()
                .insert(stream.name.clone(), stream.stream_id);
            self.streams.borrow_mut().insert(stream.stream_id, stream);
        }

        info!(
            "Loaded {} stream(s) from disk.",
            self.streams.borrow().len()
        );
        Ok(())
    }

    pub fn get_streams(&self) -> Vec<Stream> {
        self.streams.borrow().values().cloned().collect()
    }

    pub fn find_streams(&self, session: &Session) -> Result<Vec<Stream>, IggyError> {
        let user_id = self.ensure_authenticated(session.client_id)?;
        self.permissioner.borrow().get_streams(user_id)?;
        Ok(self.get_streams())
    }

    pub fn find_stream(
        &self,
        client_id: u32,
        identifier: &Identifier,
    ) -> Result<Stream, IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        let stream = self.get_stream(identifier)?;
        self.permissioner
            .borrow()
            .get_stream(user_id, stream.stream_id)?;
        Ok(stream.clone())
    }

    pub fn get_stream(&self, identifier: &Identifier) -> Result<Ref<'_, Stream>, IggyError> {
        match identifier.kind {
            IdKind::Numeric => self.get_stream_by_id(identifier.get_u32_value()?),
            IdKind::String => self.get_stream_by_name(&identifier.get_cow_str_value()?),
        }
    }

    pub fn get_stream_mut(&self, identifier: &Identifier) -> Result<RefMut<'_, Stream>, IggyError> {
        match identifier.kind {
            IdKind::Numeric => self.get_stream_by_id_mut(identifier.get_u32_value()?),
            IdKind::String => self.get_stream_by_name_mut(&identifier.get_cow_str_value()?),
        }
    }

    fn get_stream_by_name(&self, name: &str) -> Result<Ref<'_, Stream>, IggyError> {
        let streams = self.streams_ids.borrow();
        let exists = streams.iter().any(|s| s.0 == name);
        if !exists {
            return Err(IggyError::StreamNameNotFound(name.to_string()));
        }
        let stream_id = streams.get(name).unwrap();
        self.get_stream_by_id(*stream_id)
    }

    fn get_stream_by_id(&self, stream_id: u32) -> Result<Ref<'_, Stream>, IggyError> {
        let streams = self.streams.borrow();
        let exists = streams.iter().any(|s| s.0 == &stream_id);
        if !exists {
            return Err(IggyError::StreamIdNotFound(stream_id));
        }
        Ok(Ref::map(streams, |streams| {
            let stream = streams.get(&stream_id);
            stream.unwrap()
        }))
    }

    fn get_stream_by_name_mut(&self, name: &str) -> Result<RefMut<'_, Stream>, IggyError> {
        let streams_ids = self.streams_ids.borrow_mut();
        let exists = streams_ids.iter().any(|s| s.0 == name);
        if !exists {
            return Err(IggyError::StreamNameAlreadyExists(name.to_owned()));
        }
        let id = streams_ids.get(name);
        self.get_stream_by_id_mut(*id.unwrap())
    }

    fn get_stream_by_id_mut(&self, stream_id: u32) -> Result<RefMut<'_, Stream>, IggyError> {
        let streams = self.streams.borrow_mut();
        let exists = streams.iter().any(|s| s.0 == &stream_id);
        if !exists {
            return Err(IggyError::StreamIdNotFound(stream_id));
        }
        Ok(RefMut::map(streams, |s| {
            let stream = s.get_mut(&stream_id);
            stream.unwrap()
        }))
    }

    pub async fn create_stream(
        &self,
        client_id: u32,
        stream_id: Option<u32>,
        name: String,
        should_persist: bool,
    ) -> Result<(), IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        self.permissioner.borrow().create_stream(user_id)?;
        let name = name.to_lowercase_non_whitespace();
        if self.streams_ids.borrow().contains_key(&name) {
            return Err(IggyError::StreamNameAlreadyExists(name));
        }

        let mut id;
        if stream_id.is_none() {
            id = CURRENT_STREAM_ID.fetch_add(1, Ordering::SeqCst);
            loop {
                if self.streams.borrow().contains_key(&id) {
                    if id == u32::MAX {
                        return Err(IggyError::StreamIdAlreadyExists(id));
                    }
                    id = CURRENT_STREAM_ID.fetch_add(1, Ordering::SeqCst);
                } else {
                    break;
                }
            }
        } else {
            id = stream_id.unwrap();
        }

        if self.streams.borrow().contains_key(&id) {
            return Err(IggyError::StreamIdAlreadyExists(id));
        }

        let stream = Stream::create(id, &name, self.config.system.clone(), self.storage.clone());
        if should_persist {
            stream.persist().await?;
        }
        self.streams_ids.borrow_mut().insert(name.clone(), stream.stream_id);
        self.streams.borrow_mut().insert(stream.stream_id, stream);
        self.metrics.increment_streams(1);
        info!("Created stream with ID: {id}, name: '{name}'.");
        Ok(())
    }

    pub async fn update_stream(
        &self,
        client_id: u32,
        id: &Identifier,
        name: String,
    ) -> Result<(), IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        let stream = self.get_stream(id)?;
        let stream_id = stream.stream_id;
        self.permissioner
            .borrow()
            .update_stream(user_id, stream_id)?;

        let updated_name = name.to_lowercase_non_whitespace();
        if let Some(stream_id_by_name) = self.streams_ids.borrow().get(&updated_name) {
            if *stream_id_by_name != stream_id {
                return Err(IggyError::StreamNameAlreadyExists(updated_name));
            }
        }
        let mut stream = self.get_stream_mut(id)?;
        let stream = stream.borrow_mut();
        let old_name = stream.name.clone();
        stream.name.clone_from(&updated_name);
        stream.persist().await?;

        self.streams_ids.borrow_mut().remove(&old_name);
        self.streams_ids
            .borrow_mut()
            .insert(updated_name.clone(), stream_id);

        info!(
            "Stream with ID '{}' updated. Old name: '{}' changed to: '{}'.",
            id, old_name, updated_name
        );
        Ok(())
    }

    pub async fn delete_stream(&self, client_id: u32, id: &Identifier) -> Result<u32, IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        let stream = self.get_stream(id)?;
        let stream_id = stream.stream_id;
        self.permissioner
            .borrow()
            .delete_stream(user_id, stream_id)?;
        let stream_name = stream.name.clone();
        if stream.delete().await.is_err() {
            return Err(IggyError::CannotDeleteStream(stream_id));
        }

        self.metrics.decrement_streams(1);
        self.metrics.decrement_topics(stream.get_topics_count());
        self.metrics
            .decrement_partitions(stream.get_partitions_count());
        self.metrics.decrement_messages(stream.get_messages_count());
        self.metrics.decrement_segments(stream.get_segments_count());
        self.streams.borrow_mut().remove(&stream_id);
        self.streams_ids.borrow_mut().remove(&stream_name);
        let current_stream_id = CURRENT_STREAM_ID.load(Ordering::SeqCst);
        if current_stream_id > stream_id {
            CURRENT_STREAM_ID.store(stream_id, Ordering::SeqCst);
        }

        self.client_manager
            .borrow_mut()
            .delete_consumer_groups_for_stream(stream_id)
            .await;
        Ok(stream_id)
    }

    pub async fn purge_stream(
        &self,
        client_id: u32,
        stream_id: &Identifier,
    ) -> Result<(), IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        let stream = self.get_stream(stream_id)?;
        self.permissioner
            .borrow()
            .purge_stream(user_id, stream.stream_id)?;
        stream.purge().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::server::PersonalAccessTokenConfig;
    use crate::configs::system::SystemConfig;
    use crate::streaming::storage::tests::get_test_system_storage;
    use crate::streaming::users::user::User;
    use iggy::users::defaults::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME};
    use std::{
        net::{Ipv4Addr, SocketAddr},
        sync::Arc,
    };

    /*
    #[monoio::test]
    async fn should_get_stream_by_id_and_name() {
        let stream_id = 1;
        let stream_name = "test";
        let config = Arc::new(SystemConfig::default());
        let storage = get_test_system_storage();
        let mut system =
            System::create(config, storage, None, PersonalAccessTokenConfig::default());
        let root = User::root(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD);
        let session = Session::new(
            1,
            root.id,
            SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 1234),
        );
        system.permissioner.init_permissions_for_user(root);
        system
            .create_stream(&session, Some(stream_id), stream_name)
            .await
            .unwrap();

        let stream = system.get_stream(&Identifier::numeric(stream_id).unwrap());
        assert!(stream.is_ok());
        let stream = stream.unwrap();
        assert_eq!(stream.stream_id, stream_id);
        assert_eq!(stream.name, stream_name);

        let stream = system.get_stream(&Identifier::named(stream_name).unwrap());
        assert!(stream.is_ok());
        let stream = stream.unwrap();
        assert_eq!(stream.stream_id, stream_id);
        assert_eq!(stream.name, stream_name);
    }
     */
}
