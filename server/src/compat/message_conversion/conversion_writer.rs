use crate::streaming::utils::file;
use iggy::error::IggyError;
use tracing::trace;

//TODO (numinex) - Make this writer transactional
pub struct ConversionWriter<'w> {
    pub log_path: &'w str,
    pub index_path: &'w str,
    pub time_index_path: &'w str,

    pub alt_log_path: String,
    pub alt_index_path: String,
    pub alt_time_index_path: String,

    compat_backup_path: &'w str,
}

impl<'w> ConversionWriter<'w> {
    pub fn init(
        log_path: &'w str,
        index_path: &'w str,
        time_index_path: &'w str,
        compat_backup_path: &'w str,
    ) -> ConversionWriter<'w> {
        ConversionWriter {
            log_path,
            index_path,
            time_index_path,
            alt_log_path: format!("{}_temp.{}", log_path.split('.').next().unwrap(), "log"),
            alt_index_path: format!("{}_temp.{}", index_path.split('.').next().unwrap(), "index"),
            alt_time_index_path: format!(
                "{}_temp.{}",
                time_index_path.split('.').next().unwrap(),
                "timeindex"
            ),
            compat_backup_path,
        }
    }

    pub async fn create_alt_directories(&self) -> Result<(), IggyError> {
        tokio::fs::File::create(&self.alt_log_path).await?;
        tokio::fs::File::create(&self.alt_index_path).await?;
        tokio::fs::File::create(&self.alt_time_index_path).await?;

        trace!(
            "Created temporary files for conversion, log: {}, index: {}, time_index: {}",
            &self.alt_log_path,
            &self.alt_index_path,
            &self.alt_time_index_path
        );
        Ok(())
    }

    pub async fn create_old_segment_backup(&self) -> Result<(), IggyError> {
        let log_backup_path = &self
            .log_path
            .split_once('/')
            .map(|(_, path)| format!("{}/{}", &self.compat_backup_path, path))
            .unwrap();
        let index_backup_path = &self
            .index_path
            .split_once('/')
            .map(|(_, path)| format!("{}/{}", self.compat_backup_path, path))
            .unwrap();
        let time_index_backup_path = &self
            .time_index_path
            .split_once('/')
            .map(|(_, path)| format!("{}/{}", self.compat_backup_path, path))
            .unwrap();

        let log_path_last_idx = log_backup_path.rfind('/').unwrap();
        let index_path_last_idx = index_backup_path.rfind('/').unwrap();
        let time_index_path_last_idx = time_index_backup_path.rfind('/').unwrap();
        if tokio::fs::metadata(&log_backup_path[..log_path_last_idx])
            .await
            .is_err()
        {
            tokio::fs::create_dir_all(&log_backup_path[..log_path_last_idx]).await?;
        }
        if tokio::fs::metadata(&index_backup_path[..index_path_last_idx])
            .await
            .is_err()
        {
            tokio::fs::create_dir_all(&index_backup_path[..index_path_last_idx]).await?;
        }
        if tokio::fs::metadata(&time_index_backup_path[..time_index_path_last_idx])
            .await
            .is_err()
        {
            tokio::fs::create_dir_all(&time_index_backup_path[..time_index_path_last_idx]).await?;
        }
        file::rename(self.log_path, log_backup_path).await?;
        file::rename(self.index_path, index_backup_path).await?;
        file::rename(self.time_index_path, time_index_backup_path).await?;

        trace!(
            "Created backup of converted segment, log: {}, index: {}, time_index: {}",
            &log_backup_path,
            &index_backup_path,
            &time_index_backup_path
        );
        Ok(())
    }

    pub async fn replace_with_converted(&self) -> Result<(), IggyError> {
        file::rename(&self.alt_log_path, self.log_path).await?;
        file::rename(&self.alt_index_path, self.index_path).await?;
        file::rename(&self.alt_time_index_path, self.time_index_path).await?;

        trace!("Replaced old segment with newly converted files");
        Ok(())
    }
}
