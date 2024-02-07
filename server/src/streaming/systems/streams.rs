use crate::streaming::session::Session;
use crate::streaming::streams::stream::Stream;
use crate::streaming::systems::system::System;
use futures::future::try_join_all;
use iggy::error::IggyError;
use iggy::identifier::{IdKind, Identifier};
use iggy::locking::IggySharedMutFn;
use iggy::utils::text;
use std::cell::RefCell;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::fs::read_dir;
use tracing::{error, info};

static CURRENT_STREAM_ID: AtomicU32 = AtomicU32::new(1);

impl System {
    pub(crate) async fn load_streams(&mut self) -> Result<(), IggyError> {
        info!("Loading streams from disk...");
        let mut unloaded_streams = Vec::new();
        let dir_entries = read_dir(&self.config.get_streams_path()).await;
        if let Err(error) = dir_entries {
            error!("Cannot read streams directory: {}", error);
            return Err(IggyError::CannotReadStreams);
        }

        let mut dir_entries = dir_entries.unwrap();
        while let Some(dir_entry) = dir_entries.next_entry().await.unwrap_or(None) {
            let name = dir_entry.file_name().into_string().unwrap();
            let stream_id = name.parse::<u32>();
            if stream_id.is_err() {
                error!("Invalid stream ID file with name: '{}'.", name);
                continue;
            }

            let stream_id = stream_id.unwrap();
            let stream = Stream::empty(stream_id, self.config.clone(), self.storage.clone());
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
            if self.streams.contains_key(&stream.stream_id) {
                error!("Stream with ID: '{}' already exists.", &stream.stream_id);
                continue;
            }

            if self.streams_ids.contains_key(&stream.name) {
                error!("Stream with name: '{}' already exists.", &stream.name);
                continue;
            }

            self.metrics.increment_streams(1);
            self.metrics.increment_topics(stream.get_topics_count());
            self.metrics
                .increment_partitions(stream.get_partitions_count());
            self.metrics
                .increment_segments(stream.get_segments_count().await);
            self.metrics.increment_messages(stream.get_messages_count());

            self.streams_ids
                .insert(stream.name.clone(), stream.stream_id);
            self.streams.insert(stream.stream_id, stream);
        }

        info!("Loaded {} stream(s) from disk.", self.streams.len());
        Ok(())
    }

    pub fn get_streams(&self) -> Vec<&Stream> {
        self.streams.values().collect()
    }

    pub fn find_streams(&self, session: &Session) -> Result<Vec<&Stream>, IggyError> {
        self.ensure_authenticated(session)?;
        self.permissioner.get_streams(session.get_user_id())?;
        Ok(self.get_streams())
    }

    pub fn find_stream(
        &self,
        session: &Session,
        identifier: &Identifier,
    ) -> Result<&Stream, IggyError> {
        self.ensure_authenticated(session)?;
        let stream = self.get_stream(identifier)?;
        self.permissioner
            .get_stream(session.get_user_id(), stream.stream_id)?;
        Ok(stream)
    }

    pub fn get_stream(&self, identifier: &Identifier) -> Result<&Stream, IggyError> {
        match identifier.kind {
            IdKind::Numeric => self.get_stream_by_id(identifier.get_u32_value()?),
            IdKind::String => self.get_stream_by_name(&identifier.get_cow_str_value()?),
        }
    }

    pub fn get_stream_mut(&mut self, identifier: &Identifier) -> Result<&mut Stream, IggyError> {
        match identifier.kind {
            IdKind::Numeric => self.get_stream_by_id_mut(identifier.get_u32_value()?),
            IdKind::String => self.get_stream_by_name_mut(&identifier.get_cow_str_value()?),
        }
    }

    fn get_stream_by_name(&self, name: &str) -> Result<&Stream, IggyError> {
        let stream_id = self.streams_ids.get(name);
        if stream_id.is_none() {
            return Err(IggyError::StreamNameNotFound(name.to_string()));
        }

        self.get_stream_by_id(*stream_id.unwrap())
    }

    fn get_stream_by_id(&self, stream_id: u32) -> Result<&Stream, IggyError> {
        let stream = self.streams.get(&stream_id);
        if stream.is_none() {
            return Err(IggyError::StreamIdNotFound(stream_id));
        }

        Ok(stream.unwrap())
    }

    fn get_stream_by_name_mut(&mut self, name: &str) -> Result<&mut Stream, IggyError> {
        let stream_id;
        {
            let id = self.streams_ids.get_mut(name);
            if id.is_none() {
                return Err(IggyError::StreamNameNotFound(name.to_string()));
            }

            stream_id = *id.unwrap();
        }

        self.get_stream_by_id_mut(stream_id)
    }

    fn get_stream_by_id_mut(&mut self, stream_id: u32) -> Result<&mut Stream, IggyError> {
        let stream = self.streams.get_mut(&stream_id);
        if stream.is_none() {
            return Err(IggyError::StreamIdNotFound(stream_id));
        }

        Ok(stream.unwrap())
    }

    pub async fn create_stream(
        &mut self,
        session: &Session,
        stream_id: Option<u32>,
        name: &str,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        self.permissioner.create_stream(session.get_user_id())?;
        let name = text::to_lowercase_non_whitespace(name);
        if self.streams_ids.contains_key(&name) {
            return Err(IggyError::StreamNameAlreadyExists(name.to_string()));
        }

        let mut id;
        if stream_id.is_none() {
            id = CURRENT_STREAM_ID.fetch_add(1, Ordering::SeqCst);
            loop {
                if self.streams.contains_key(&id) {
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

        if self.streams.contains_key(&id) {
            return Err(IggyError::StreamIdAlreadyExists(id));
        }

        let stream = Stream::create(id, &name, self.config.clone(), self.storage.clone());
        stream.persist().await?;
        info!("Created stream with ID: {id}, name: '{name}'.");
        self.streams_ids.insert(name, stream.stream_id);
        self.streams.insert(stream.stream_id, stream);
        self.metrics.increment_streams(1);
        Ok(())
    }

    pub async fn update_stream(
        &mut self,
        session: &Session,
        id: &Identifier,
        name: &str,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        let stream_id;
        {
            let stream = self.get_stream(id)?;
            stream_id = stream.stream_id;
        }

        self.permissioner
            .update_stream(session.get_user_id(), stream_id)?;
        let updated_name = text::to_lowercase_non_whitespace(name);

        {
            if let Some(stream_id_by_name) = self.streams_ids.get(&updated_name) {
                if *stream_id_by_name != stream_id {
                    return Err(IggyError::StreamNameAlreadyExists(updated_name.clone()));
                }
            }
        }

        let old_name;
        {
            let stream = self.get_stream_mut(id)?;
            old_name = stream.name.clone();
            stream.name = updated_name.clone();
            stream.persist().await?;
        }

        {
            self.streams_ids.remove(&old_name);
            self.streams_ids.insert(updated_name.clone(), stream_id);
        }

        info!(
            "Stream with ID '{}' updated. Old name: '{}' changed to: '{}'.",
            id, old_name, updated_name
        );
        Ok(())
    }

    pub async fn delete_stream(
        &mut self,
        session: &Session,
        id: &Identifier,
    ) -> Result<u32, IggyError> {
        self.ensure_authenticated(session)?;
        let stream = self.get_stream(id)?;
        let stream_id = stream.stream_id;
        self.permissioner
            .delete_stream(session.get_user_id(), stream_id)?;
        let stream_name = stream.name.clone();
        if stream.delete().await.is_err() {
            return Err(IggyError::CannotDeleteStream(stream_id));
        }

        self.metrics.decrement_streams(1);
        self.metrics.decrement_topics(stream.get_topics_count());
        self.metrics
            .decrement_partitions(stream.get_partitions_count());
        self.metrics.decrement_messages(stream.get_messages_count());
        self.metrics
            .decrement_segments(stream.get_segments_count().await);

        self.streams.remove(&stream_id);
        self.streams_ids.remove(&stream_name);
        let current_stream_id = CURRENT_STREAM_ID.load(Ordering::SeqCst);
        if current_stream_id > stream_id {
            CURRENT_STREAM_ID.store(stream_id, Ordering::SeqCst);
        }

        let client_manager = self.client_manager.read().await;
        client_manager
            .delete_consumer_groups_for_stream(stream_id)
            .await;
        Ok(stream_id)
    }

    pub async fn purge_stream(
        &self,
        session: &Session,
        stream_id: &Identifier,
    ) -> Result<(), IggyError> {
        let stream = self.get_stream(stream_id)?;
        self.permissioner
            .purge_stream(session.get_user_id(), stream.stream_id)?;
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
    use std::{
        net::{Ipv4Addr, SocketAddr},
        sync::Arc,
    };

    #[tokio::test]
    async fn should_get_stream_by_id_and_name() {
        let stream_id = 1;
        let stream_name = "test";
        let config = Arc::new(SystemConfig::default());
        let storage = get_test_system_storage();
        let mut system =
            System::create(config, storage, None, PersonalAccessTokenConfig::default());
        let root = User::root();
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
}
