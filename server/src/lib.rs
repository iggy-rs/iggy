#[cfg(all(target_env = "msvc", feature = "jemalloc"))]
compile_error!("'jemalloc' feature cannot be used when MSVC is being used");

#[cfg(all(not(target_env = "msvc"), feature = "jemalloc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(all(not(target_env = "msvc"), feature = "jemalloc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

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
