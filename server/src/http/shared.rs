use crate::http::jwt::jwt_manager::JwtManager;
use crate::streaming::systems::system::SharedSystem;
use std::net::SocketAddr;
use ulid::Ulid;

pub struct AppState {
    pub jwt_manager: JwtManager,
    pub system: SharedSystem,
}

#[derive(Debug, Copy, Clone)]
pub struct RequestDetails {
    pub request_id: Ulid,
    pub ip_address: SocketAddr,
}
