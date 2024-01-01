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
pub mod configs;
pub mod http;
pub mod logging;
pub mod quic;
pub mod server_error;
pub mod streaming;
pub mod tcp;
