pub mod buf;
pub mod log;
pub mod stream;

pub fn val_align_up(value: u64, alignment: u64) -> u64 {
    (value + alignment - 1) & !(alignment - 1)
}

pub fn val_align_down(value: u64, alignment: u64) -> u64 {
    value & !(alignment - 1)
}
