pub mod byte_size;
pub mod checksum;
pub mod crypto;
pub mod duration;
pub mod expiry;
pub mod personal_access_token_expiry;
pub mod sizeable;
pub mod text;
pub mod timestamp;
pub mod topic_size;

pub fn val_align_up(value: u64, alignment: u64) -> u64 {
    (value + alignment - 1) & !(alignment - 1)
}
