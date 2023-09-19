use crate::http::jwt::JwtManager;
use crate::streaming::systems::system::System;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct AppState {
    pub jwt_manager: JwtManager,
    pub system: Arc<RwLock<System>>,
}
