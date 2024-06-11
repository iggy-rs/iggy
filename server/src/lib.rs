#[cfg(all(target_env = "msvc", feature = "jemalloc"))]
compile_error!("'jemalloc' feature cannot be used when MSVC is being used");

#[cfg(all(not(target_env = "msvc"), feature = "jemalloc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(all(not(target_env = "msvc"), feature = "jemalloc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

pub mod args;
pub mod binary;
pub mod channels;
pub(crate) mod compat;
pub mod configs;
pub(crate) mod io_utils;

// TODO: monoio: enable HTTP API
// pub mod http;
pub mod log;
pub mod server_error;
pub mod streaming;
pub mod tcp;
pub mod tpc;