use crate::utils::bitset::OptimalBitset;

pub(crate) mod client_table;
pub(crate) mod consensus;
pub(crate) mod header;
pub(crate) mod message;
pub(crate) mod replica;
pub(crate) mod status;

// Types
pub(crate) type OpNumber = u64;
pub(crate) type CommitNumber = u64;
pub(crate) type ViewNumber = u32;
pub(crate) type Version = usize;
pub(crate) type ProtocolVersion = u16;
pub(crate) type ReplicaCount = u8;

#[derive(Default, Debug, PartialEq, Eq)]
pub(crate) enum Operation {
    /// Authorization
    Auth,
    /// Metadata such as CRUD on streams / topics / partitions
    #[default]
    Metadata,
    /// Messages(User data) persisted by our storage.
    Messages,
}

pub(crate) struct QuorumCounter<T> {
    value: T,
    acks: OptimalBitset,
    quorum: bool,
}

impl<T> QuorumCounter<T> {
    pub fn new(value: T, acks: OptimalBitset) -> Self {
        Self {
            value,
            acks,
            quorum: false
        }
    }
}

impl<T> Default for QuorumCounter<T>
where
    T: Default,
{
    fn default() -> Self {
        Self {
            value: Default::default(),
            acks: Default::default(),
            quorum: Default::default(),
        }
    }
}
