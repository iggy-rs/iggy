pub mod connection_handler;
pub mod sender;
pub mod tcp_listener;
pub mod tcp_sender;
pub mod tcp_server;
mod tcp_socket;
pub mod tcp_tls_listener;
pub mod tcp_tls_sender;

pub const COMPONENT: &str = "TCP";
