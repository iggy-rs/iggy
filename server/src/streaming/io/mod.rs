pub mod buf;

pub fn val_align_up(value: u64, alignment: u64) -> u64 {
    (value + alignment - 1) & !(alignment - 1)
}

pub fn val_align_down(value: u64, alignment: u64) -> u64 {
    value & !(alignment - 1)
}

pub trait IoUnit {
    fn pos(&self) -> u64;
    fn size(&self) -> usize;
}

impl IoUnit for IggyIoUnit {
    fn pos(&self) -> u64 {
        self.0
    }

    fn size(&self) -> usize {
        self.1
    }
}
pub type IggyIoUnit = (u64, usize);
