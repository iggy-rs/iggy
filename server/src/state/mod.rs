use crate::state::command::EntryCommand;
use crate::state::entry::StateEntry;
use async_trait::async_trait;
use iggy::error::IggyError;
#[cfg(test)]
use mockall::automock;
use std::fmt::Debug;

pub mod command;
pub mod entry;
pub mod file;
pub mod models;
pub mod system;

pub const COMPONENT: &str = "STATE";

#[derive(Debug)]
pub enum StateKind {
    File(file::FileState),
    #[cfg(test)]
    Mock(MockState),
}

#[async_trait]
#[cfg_attr(test, automock)]
pub trait State: Send + Sync + Debug {
    async fn init(&self) -> Result<Vec<StateEntry>, IggyError>;
    async fn load_entries(&self) -> Result<Vec<StateEntry>, IggyError>;
    async fn apply(&self, user_id: u32, command: EntryCommand) -> Result<(), IggyError>;
}

impl StateKind {
    pub async fn init(&self) -> Result<Vec<StateEntry>, IggyError> {
        match self {
            Self::File(s) => s.init().await,
            #[cfg(test)]
            Self::Mock(s) => s.init().await,
        }
    }

    pub async fn load_entries(&self) -> Result<Vec<StateEntry>, IggyError> {
        match self {
            Self::File(s) => s.load_entries().await,
            #[cfg(test)]
            Self::Mock(s) => s.load_entries().await,
        }
    }

    pub async fn apply(&self, user_id: u32, command: EntryCommand) -> Result<(), IggyError> {
        match self {
            Self::File(s) => s.apply(user_id, command).await,
            #[cfg(test)]
            Self::Mock(s) => s.apply(user_id, command).await,
        }
    }
}
