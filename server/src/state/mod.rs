use crate::state::command::EntryCommand;
use crate::state::entry::StateEntry;
use async_trait::async_trait;
use iggy::error::IggyError;
use std::fmt::Debug;

pub mod command;
pub mod entry;
pub mod file;
pub mod models;
pub mod system;

#[async_trait]
pub trait State: Send + Sync + Debug {
    async fn init(&self) -> Result<Vec<StateEntry>, IggyError>;
    async fn load_entries(&self) -> Result<Vec<StateEntry>, IggyError>;
    async fn apply(&self, user_id: u32, command: EntryCommand) -> Result<(), IggyError>;
}
