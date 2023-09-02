use crate::streams::stream::Stream;
use crate::systems::system::System;
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
        let dir_entries = read_dir(&self.streams_path).await;
        if dir_entries.is_err() {
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
                if stream.load().await.is_err() {
                    error!("Failed to load stream with ID: {}.", stream.id);
                    return;
                }

                loaded_streams.lock().await.push(stream);
            });
            load_streams.push(load_stream);
        }

        join_all(load_streams).await;
        for stream in loaded_streams.lock().await.drain(..) {
            if self.streams.contains_key(&stream.id) {
                error!("Stream with ID: '{}' already exists.", &stream.id);
                continue;
            }

            if self.streams_ids.contains_key(&stream.name) {
                error!("Stream with name: '{}' already exists.", &stream.name);
                continue;
            }

            self.streams_ids.insert(stream.name.clone(), stream.id);
            self.streams.insert(stream.id, stream);
        }

        info!("Loaded {} stream(s) from disk.", self.streams.len());
        Ok(())
    }

    pub fn get_streams(&self) -> Vec<&Stream> {
        self.streams.values().collect()
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

    fn get_stream_by_id(&self, id: u32) -> Result<&Stream, Error> {
        let stream = self.streams.get(&id);
        if stream.is_none() {
            return Err(Error::StreamIdNotFound(id));
        }

        Ok(stream.unwrap())
    }

    fn get_stream_by_name(&self, name: &str) -> Result<&Stream, Error> {
        let stream_id = self.streams_ids.get(name);
        if stream_id.is_none() {
            return Err(Error::StreamNameNotFound(name.to_string()));
        }

        self.get_stream_by_id(*stream_id.unwrap())
    }

    fn get_stream_by_id_mut(&mut self, id: u32) -> Result<&mut Stream, Error> {
        let stream = self.streams.get_mut(&id);
        if stream.is_none() {
            return Err(Error::StreamIdNotFound(id));
        }

        Ok(stream.unwrap())
    }

    fn get_stream_by_name_mut(&mut self, name: &str) -> Result<&mut Stream, Error> {
        let stream_id = self.streams_ids.get_mut(name);
        if stream_id.is_none() {
            return Err(Error::StreamNameNotFound(name.to_string()));
        }

        Ok(self.streams.get_mut(stream_id.unwrap()).unwrap())
    }

    pub async fn create_stream(&mut self, id: u32, name: &str) -> Result<(), Error> {
        if self.streams.contains_key(&id) {
            return Err(Error::StreamIdAlreadyExists(id));
        }

        let name = text::to_lowercase_non_whitespace(name);
        if self.streams_ids.contains_key(&name) {
            return Err(Error::StreamNameAlreadyExists(name.to_string()));
        }

        let stream = Stream::create(id, &name, self.config.clone(), self.storage.clone());
        stream.persist().await?;
        info!("Created stream with ID: {}, name: '{}'.", id, name);
        self.streams_ids.insert(name, stream.id);
        self.streams.insert(stream.id, stream);
        Ok(())
    }

    pub async fn update_stream(&mut self, id: &Identifier, name: &str) -> Result<(), Error> {
        let stream_id;
        {
            let stream = self.get_stream(id)?;
            stream_id = stream.id;
        }

        let updated_name = text::to_lowercase_non_whitespace(name);

        {
            if let Some(stream_id_by_name) = self.streams_ids.get(&updated_name) {
                if *stream_id_by_name != stream_id {
                    return Err(Error::StreamNameAlreadyExists(updated_name.clone()));
                }
            }
        }

        {
            self.streams_ids.remove(&updated_name.clone());
            self.streams_ids.insert(updated_name.clone(), stream_id);
            let stream = self.get_stream_mut(id)?;
            stream.name = updated_name;
            stream.persist().await?;
            info!("Updated stream: {} with ID: {}", stream.name, id);
        }

        Ok(())
    }

    pub async fn delete_stream(&mut self, id: &Identifier) -> Result<u32, Error> {
        let stream = self.get_stream(id)?;
        let stream_id = stream.id;
        let stream_name = stream.name.clone();
        if stream.delete().await.is_err() {
            return Err(Error::CannotDeleteStream(stream_id));
        }
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
    use crate::config::SystemConfig;
    use crate::storage::tests::get_test_system_storage;

    #[tokio::test]
    async fn should_get_stream_by_id_and_name() {
        let stream_id = 1;
        let stream_name = "test";
        let config = Arc::new(SystemConfig::default());
        let storage = get_test_system_storage();
        let mut system = System::create(config, storage);
        system.create_stream(stream_id, stream_name).await.unwrap();

        let stream = system.get_stream(&Identifier::numeric(stream_id).unwrap());
        assert!(stream.is_ok());
        let stream = stream.unwrap();
        assert_eq!(stream.id, stream_id);
        assert_eq!(stream.name, stream_name);

        let stream = system.get_stream(&Identifier::named(stream_name).unwrap());
        assert!(stream.is_ok());
        let stream = stream.unwrap();
        assert_eq!(stream.id, stream_id);
        assert_eq!(stream.name, stream_name);
    }
}
