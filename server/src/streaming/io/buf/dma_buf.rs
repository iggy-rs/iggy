use std::{
    alloc::{self, Layout},
    ptr,
};

use super::IoBuf;

#[derive(Debug)]
pub struct DmaBuf {
    data: ptr::NonNull<u8>,
    layout: Layout,
    size: usize,
}

impl DmaBuf {
    pub fn len(&self) -> usize {
        self.size
    }
}

// SAFETY: fuck safety.
unsafe impl Send for DmaBuf {}
unsafe impl Sync for DmaBuf {}

impl Drop for DmaBuf {
    fn drop(&mut self) {
        unsafe {
            alloc::dealloc(self.data.as_ptr(), self.layout);
        }
    }
}

impl IoBuf for DmaBuf {
    fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    fn as_ptr_mut(&mut self) -> *mut u8 {
        self.data.as_ptr()
    }

    fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.size) }
    }

    fn as_bytes_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.as_ptr_mut(), self.size) }
    }

    fn with_capacity(size: usize) -> Self {
        assert!(size > 0);
        assert!(size % 512 == 0);
        let layout =
            Layout::from_size_align(size, 4096).expect("Falied to create layout for DmaBuf");
        let data_ptr = unsafe { alloc::alloc(layout) };
        let data = ptr::NonNull::new(data_ptr).expect("DmaBuf data_ptr is not null");

        Self { data, layout, size }
    }
}

impl AsRef<[u8]> for DmaBuf {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl AsMut<[u8]> for DmaBuf {
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_bytes_mut()
    }
}
