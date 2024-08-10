use iggy::utils::crypto::{Aes256GcmEncryptor, Encryptor};
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
        StateSetup::create(None).await
    }

    pub async fn init_with_encryptor() -> StateSetup {
        StateSetup::create(Some(&[1; 32])).await
    }

    pub async fn create(encryption_key: Option<&[u8]>) -> StateSetup {
        let directory_path = format!("state_{}", Uuid::now_v7().to_u128_le());
        let log_path = format!("{}/log", directory_path);
        create_dir(&directory_path).await.unwrap();

        let version = SemanticVersion::from_str("1.2.3").unwrap();
        let persister = FilePersister {};
        let encryptor: Option<Arc<dyn Encryptor>> = match encryption_key {
            Some(key) => Some(Arc::new(Aes256GcmEncryptor::new(key).unwrap())),
            None => None,
        };
        let state = FileState::new(&log_path, &version, Arc::new(persister), encryptor);

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
