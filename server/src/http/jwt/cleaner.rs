use crate::http::state::AppState;
use iggy::utils::timestamp::TimeStamp;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

pub fn start_expired_tokens_cleaner(app_state: Arc<AppState>) {
    tokio::spawn(async move {
        let mut interval_timer = tokio::time::interval(Duration::from_secs(300));
        loop {
            interval_timer.tick().await;
            info!("Deleting expired tokens...");
            let now = TimeStamp::now().to_secs();
            if app_state
                .jwt_manager
                .delete_expired_revoked_tokens(now)
                .await
                .is_err()
            {
                error!("Failed to delete expired revoked access tokens.");
            }
            if app_state
                .jwt_manager
                .delete_expired_refresh_tokens(now)
                .await
                .is_err()
            {
                error!("Failed to delete expired refresh tokens.");
            }
        }
    });
}
