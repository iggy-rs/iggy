use crate::streaming::session::Session;
use crate::streaming::streams::stream::Stream;
use crate::streaming::systems::system::System;
use futures::future::join_all;
use iggy::error::Error;
use iggy::identifier::{IdKind, Identifier};
use iggy::utils::text;
use std::sync::Arc;
use tokio::fs::read_dir;
use tokio::sync::Mutex;
use tracing::{error, info};

impl System {
    pub(crate) async fn load_streams(&mut self) -> Result<(), Error> {
        info!("Loading streams from disk...");
        let mut unloaded_streams = Vec::new();
        let dir_entries = read_dir(&self.config.get_streams_path()).await;
        if let Err(error) = dir_entries {
            error!("Cannot read streams directory: {}", error);
            return Err(Error::CannotReadStreams);
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

        let loaded_streams = Arc::new(Mutex::new(Vec::new()));
        let mut load_streams = Vec::new();
        for mut stream in unloaded_streams {
            let loaded_streams = loaded_streams.clone();
            let load_stream = tokio::spawn(async move {
                match stream.load().await {
                    Ok(_) => {
                        loaded_streams.lock().await.push(stream);
                    }
                    Err(error) => {
                        error!(
                            "Failed to load stream with ID: {}. Error: {}",
                            stream.stream_id, error
                        );
                    }
                }
            });
            load_streams.push(load_stream);
        }

        join_all(load_streams).await;
        for stream in loaded_streams.lock().await.drain(..) {
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
            self.metrics
                .increment_messages(stream.get_messages_count().await);

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

    pub fn find_streams(&self, session: &Session) -> Result<Vec<&Stream>, Error> {
        self.ensure_authenticated(session)?;
        self.permissioner.get_streams(session.user_id)?;
        Ok(self.get_streams())
    }

    pub fn find_stream(
        &self,
        session: &Session,
        identifier: &Identifier,
    ) -> Result<&Stream, Error> {
        self.ensure_authenticated(session)?;
        let stream = self.get_stream(identifier)?;
        self.permissioner
            .get_stream(session.user_id, stream.stream_id)?;
        Ok(stream)
    }

    pub fn get_stream(&self, identifier: &Identifier) -> Result<&Stream, Error> {
        match identifier.kind {
            IdKind::Numeric => self.get_stream_by_id(identifier.get_u32_value().unwrap()),
            IdKind::String => self.get_stream_by_name(&identifier.get_string_value().unwrap()),
        }
    }

    pub fn get_stream_mut(&mut self, identifier: &Identifier) -> Result<&mut Stream, Error> {
        match identifier.kind {
            IdKind::Numeric => self.get_stream_by_id_mut(identifier.get_u32_value().unwrap()),
            IdKind::String => self.get_stream_by_name_mut(&identifier.get_string_value().unwrap()),
        }
    }

    fn get_stream_by_name(&self, name: &str) -> Result<&Stream, Error> {
        let stream_id = self.streams_ids.get(name);
        if stream_id.is_none() {
            return Err(Error::StreamNameNotFound(name.to_string()));
        }

        self.get_stream_by_id(*stream_id.unwrap())
    }

    fn get_stream_by_id(&self, stream_id: u32) -> Result<&Stream, Error> {
        let stream = self.streams.get(&stream_id);
        if stream.is_none() {
            return Err(Error::StreamIdNotFound(stream_id));
        }

        Ok(stream.unwrap())
    }

    fn get_stream_by_name_mut(&mut self, name: &str) -> Result<&mut Stream, Error> {
        let stream_id;
        {
            let id = self.streams_ids.get_mut(name);
            if id.is_none() {
                return Err(Error::StreamNameNotFound(name.to_string()));
            }

            stream_id = *id.unwrap();
        }

        self.get_stream_by_id_mut(stream_id)
    }

    fn get_stream_by_id_mut(&mut self, stream_id: u32) -> Result<&mut Stream, Error> {
        let stream = self.streams.get_mut(&stream_id);
        if stream.is_none() {
            return Err(Error::StreamIdNotFound(stream_id));
        }

        Ok(stream.unwrap())
    }

    pub async fn create_stream(
        &mut self,
        session: &Session,
        stream_id: u32,
        name: &str,
    ) -> Result<(), Error> {
        self.ensure_authenticated(session)?;
        self.permissioner.create_stream(session.user_id)?;
        if self.streams.contains_key(&stream_id) {
            return Err(Error::StreamIdAlreadyExists(stream_id));
        }

        let name = text::to_lowercase_non_whitespace(name);
        if self.streams_ids.contains_key(&name) {
            return Err(Error::StreamNameAlreadyExists(name.to_string()));
        }

        let stream = Stream::create(stream_id, &name, self.config.clone(), self.storage.clone());
        stream.persist().await?;
        info!("Created stream with ID: {}, name: '{}'.", stream_id, name);
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
    ) -> Result<(), Error> {
        self.ensure_authenticated(session)?;
        let stream_id;
        {
            let stream = self.get_stream(id)?;
            stream_id = stream.stream_id;
        }

        self.permissioner
            .update_stream(session.user_id, stream_id)?;
        let updated_name = text::to_lowercase_non_whitespace(name);

        {
            if let Some(stream_id_by_name) = self.streams_ids.get(&updated_name) {
                if *stream_id_by_name != stream_id {
                    return Err(Error::StreamNameAlreadyExists(updated_name.clone()));
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
    ) -> Result<u32, Error> {
        self.ensure_authenticated(session)?;
        let stream = self.get_stream(id)?;
        let stream_id = stream.stream_id;
        self.permissioner
            .delete_stream(session.user_id, stream_id)?;
        let stream_name = stream.name.clone();
        if stream.delete().await.is_err() {
            return Err(Error::CannotDeleteStream(stream_id));
        }

        self.metrics.decrement_streams(1);
        self.metrics.decrement_topics(stream.get_topics_count());
        self.metrics
            .decrement_partitions(stream.get_partitions_count());
        self.metrics
            .decrement_messages(stream.get_messages_count().await);
        self.metrics
            .decrement_segments(stream.get_segments_count().await);

        self.streams.remove(&stream_id);
        self.streams_ids.remove(&stream_name);
        let client_manager = self.client_manager.read().await;
        client_manager
            .delete_consumer_groups_for_stream(stream_id)
            .await;
        Ok(stream_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::server::PersonalAccessTokenConfig;
    use crate::configs::system::SystemConfig;
    use crate::streaming::storage::tests::get_test_system_storage;
    use crate::streaming::users::user::User;

    #[tokio::test]
    async fn should_get_stream_by_id_and_name() {
        let stream_id = 1;
        let stream_name = "test";
        let config = Arc::new(SystemConfig::default());
        let storage = get_test_system_storage();
        let mut system =
            System::create(config, storage, None, PersonalAccessTokenConfig::default());
        let root = User::root();
        let session = Session::new(1, root.id, "127.0.0.1".to_string());
        system.permissioner.init_permissions_for_user(root);
        system
            .create_stream(&session, stream_id, stream_name)
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
