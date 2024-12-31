#[cfg(any(feature = "mimalloc", target_env = "musl"))]
use mimalloc::MiMalloc;

#[cfg(any(feature = "mimalloc", target_env = "musl"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

pub mod archiver;
pub mod args;
pub mod binary;
pub mod channels;
mod command;
pub(crate) mod compat;
pub mod configs;
pub mod http;
pub mod log;
pub mod quic;
pub mod server_error;
pub mod state;
pub mod streaming;
pub mod tcp;
pub mod versioning;

const IGGY_ROOT_USERNAME_ENV: &str = "IGGY_ROOT_USERNAME";
const IGGY_ROOT_PASSWORD_ENV: &str = "IGGY_ROOT_PASSWORD";

pub(crate) fn map_toggle_str<'a>(enabled: bool) -> &'a str {
    match enabled {
        true => "enabled",
        false => "disabled",
    }
}
