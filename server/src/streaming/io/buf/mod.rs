pub mod dma_buf;

pub trait IoBuf: AsRef<[u8]> + AsMut<[u8]> {
    fn with_capacity(size: usize) -> Self;
    fn as_ptr(&self) -> *const u8;
    fn as_ptr_mut(&mut self) -> *mut u8;
    fn as_bytes(&self) -> &[u8];
    fn as_bytes_mut(&mut self) -> &mut [u8];
}
