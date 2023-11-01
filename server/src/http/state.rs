use crate::http::jwt::jwt_manager::JwtManager;
use crate::streaming::systems::system::SharedSystem;

pub struct AppState {
    pub jwt_manager: JwtManager,
    pub system: SharedSystem,
}
