use super::shard::IggyShard;
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
use tracing::{error, info};

static CURRENT_STREAM_ID: AtomicU32 = AtomicU32::new(1);

impl IggyShard {
    pub(crate) async fn load_streams(&mut self) -> Result<(), IggyError> {
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
            self.metrics.increment_segments(stream.get_segments_count());
            self.metrics.increment_messages(stream.get_messages_count());

            self.streams_ids
                .insert(stream.name.clone(), stream.stream_id);
            self.streams.insert(stream.stream_id, stream);
        }

        info!("Loaded {} stream(s) from disk.", self.streams.len());
        Ok(())
    }
}
