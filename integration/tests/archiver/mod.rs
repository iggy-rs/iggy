use server::archiver::disk::DiskArchiver;
use server::configs::server::DiskArchiverConfig;
use tokio::fs::create_dir;
use uuid::Uuid;

mod disk;

pub struct ArchiverSetup {
    base_path: String,
    archive_path: String,
    archiver: DiskArchiver,
}

impl ArchiverSetup {
    pub async fn init() -> ArchiverSetup {
        let base_path = format!("test_local_data_{}", Uuid::new_v4().to_u128_le());
        let archive_path = format!("{}/archive", base_path);
        let config = DiskArchiverConfig {
            path: archive_path.clone(),
        };
        let archiver = DiskArchiver::new(config);
        create_dir(&base_path).await.unwrap();

        Self {
            base_path,
            archive_path,
            archiver,
        }
    }

    pub fn archiver(&self) -> &DiskArchiver {
        &self.archiver
    }
}

impl Drop for ArchiverSetup {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.base_path).unwrap();
    }
}
