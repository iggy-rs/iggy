use nonzero_lit::u32;
use std::num::NonZeroU32;

pub const DEFAULT_HTTP_SERVER_ADDRESS: &str = "127.0.0.1:3000";
pub const DEFAULT_HTTP_START_STREAM_ID: NonZeroU32 = u32!(1000000);

pub const DEFAULT_TCP_SERVER_ADDRESS: &str = "127.0.0.1:8090";
pub const DEFAULT_TCP_START_STREAM_ID: NonZeroU32 = u32!(3000000);

pub const DEFAULT_QUIC_CLIENT_ADDRESS: &str = "127.0.0.1:0";
pub const DEFAULT_QUIC_SERVER_ADDRESS: &str = "127.0.0.1:8080";
pub const DEFAULT_QUIC_SERVER_NAME: &str = "localhost";
pub const DEFAULT_QUIC_VALIDATE_CERTIFICATE: bool = false;
pub const DEFAULT_QUIC_START_STREAM_ID: NonZeroU32 = u32!(2000000);

pub const DEFAULT_DISABLE_PARALLEL_PRODUCER_STREAMS: bool = false;
pub const DEFAULT_DISABLE_PARALLEL_CONSUMER_STREAMS: bool = false;

pub const DEFAULT_MESSAGES_PER_BATCH: NonZeroU32 = u32!(1000);
pub const DEFAULT_MESSAGE_BATCHES: NonZeroU32 = u32!(1000);
pub const DEFAULT_MESSAGE_SIZE: NonZeroU32 = u32!(1000);

pub const DEFAULT_NUMBER_OF_STREAMS: NonZeroU32 = u32!(10);
pub const DEFAULT_NUMBER_OF_STREAMS_CONSUMER_GROUP: NonZeroU32 = u32!(1);
pub const DEFAULT_NUMBER_OF_PARTITIONS: NonZeroU32 = u32!(1);

pub const DEFAULT_NUMBER_OF_CONSUMERS: NonZeroU32 = u32!(10);
pub const DEFAULT_NUMBER_OF_CONSUMER_GROUPS: NonZeroU32 = u32!(1);
pub const DEFAULT_NUMBER_OF_PRODUCERS: NonZeroU32 = u32!(10);

pub const DEFAULT_PERFORM_CLEANUP: bool = false;
pub const DEFAULT_SERVER_SYSTEM_PATH: &str = "local_data";
pub const DEFAULT_SERVER_STDOUT_VISIBILITY: bool = false;

pub const DEFAULT_WARMUP_TIME: &str = "1 s";
pub const DEFAULT_SKIP_SERVER_START: bool = false;
