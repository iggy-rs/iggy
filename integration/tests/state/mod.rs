use server::state::file::FileState;
use server::streaming::persistence::persister::FilePersister;
use server::versioning::SemanticVersion;
use std::str::FromStr;
use std::sync::Arc;
use tokio::fs::create_dir;
use uuid::Uuid;

mod file;
mod system;

pub struct StateSetup {
    directory_path: String,
    state: FileState,
    version: u32,
}

impl StateSetup {
    pub async fn init() -> StateSetup {
        let directory_path = format!("state_{}", Uuid::new_v4().to_u128_le());
        let log_path = format!("{}/log", directory_path);
        create_dir(&directory_path).await.unwrap();

        let version = SemanticVersion::from_str("1.2.3").unwrap();
        let persister = FilePersister {};
        let state = FileState::new(&log_path, &version, Arc::new(persister), None);

        Self {
            directory_path,
            state,
            version: version.get_numeric_version().unwrap(),
        }
    }

    pub fn state(&self) -> &FileState {
        &self.state
    }

    pub fn version(&self) -> u32 {
        self.version
    }
}

impl Drop for StateSetup {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.directory_path).unwrap();
    }
}
