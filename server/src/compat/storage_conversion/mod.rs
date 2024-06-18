mod persistency;

use crate::configs::system::SystemConfig;
use iggy::error::IggyError;
use std::sync::Arc;
use tracing::info;

pub async fn init(config: Arc<SystemConfig>) -> Result<(), IggyError> {
    let path = config.get_database_path();
    if path.is_none() {
        info!("No database path configured, skipping storage migration");
        return Ok(());
    }

    let path = path.unwrap();
    info!("Starting storage migration, database found at: {path}");
    Ok(())
}
