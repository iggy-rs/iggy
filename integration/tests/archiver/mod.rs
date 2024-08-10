use server::archiver::disk::DiskArchiver;
use server::configs::server::DiskArchiverConfig;
use tokio::fs::create_dir;
use uuid::Uuid;

mod disk;
mod s3;

pub struct DiskArchiverSetup {
    base_path: String,
    archive_path: String,
    archiver: DiskArchiver,
}

impl DiskArchiverSetup {
    pub async fn init() -> DiskArchiverSetup {
        let base_path = format!("test_local_data_{}", Uuid::now_v7().to_u128_le());
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

impl Drop for DiskArchiverSetup {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.base_path).unwrap();
    }
}
