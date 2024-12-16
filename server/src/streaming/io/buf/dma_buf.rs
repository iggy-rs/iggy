use std::{
    alloc::{self, Layout},
    ptr,
    sync::Arc,
};

use super::IoBuf;

#[derive(Debug)]
pub struct AreczekDmaBuf {
    inner: Arc<DmaBuf>,
}

impl Clone for AreczekDmaBuf {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

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

impl IoBuf for AreczekDmaBuf {
    fn new(size: usize) -> Self {
        let buf = DmaBuf::new(size);
        let inner = Arc::new(buf);
        Self { inner }
    }

    fn as_ptr(&self) -> *const u8 {
        self.inner.as_ptr()
    }

    fn as_ptr_mut(&mut self) -> *mut u8 {
        let this = Arc::get_mut(&mut self.inner).unwrap();
        this.as_ptr_mut()
    }

    fn as_bytes(&self) -> &[u8] {
        self.inner.as_bytes()
    }

    fn as_bytes_mut(&mut self) -> &mut [u8] {
        let this = Arc::get_mut(&mut self.inner).unwrap();
        this.as_bytes_mut()
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn into_areczek(self) -> AreczekDmaBuf {
        unimplemented!()
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

    fn len(&self) -> usize {
        self.size
    }

    fn new(size: usize) -> Self {
        assert!(size > 0);
        assert!(size % 512 == 0);
        let layout =
            Layout::from_size_align(size, 4096).expect("Falied to create layout for DmaBuf");
        let data_ptr = unsafe { alloc::alloc(layout) };
        let data = ptr::NonNull::new(data_ptr).expect("DmaBuf data_ptr is not null");

        Self { data, layout, size }
    }

    fn into_areczek(self) -> AreczekDmaBuf {
        let inner = Arc::new(self);
        AreczekDmaBuf { inner }
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

impl AsRef<[u8]> for AreczekDmaBuf {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_bytes()
    }
}

impl AsMut<[u8]> for AreczekDmaBuf {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut []
    }
}
