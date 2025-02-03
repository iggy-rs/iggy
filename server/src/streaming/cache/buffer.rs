use crate::streaming::local_sizeable::LocalSizeable;

use super::memory_tracker::CacheMemoryTracker;
use atone::Vc;
use iggy::utils::byte_size::IggyByteSize;
use std::fmt::Debug;
use std::ops::Index;
use std::sync::Arc;

#[derive(Debug)]
pub struct SmartCache<T: LocalSizeable + Debug> {
    current_size: IggyByteSize,
    buffer: Vc<T>,
    memory_tracker: Arc<CacheMemoryTracker>,
}

impl<T> SmartCache<T>
where
    T: LocalSizeable + Debug,
{
    pub fn new() -> Self {
        let current_size = IggyByteSize::default();
        let buffer = Vc::new();
        let memory_tracker = CacheMemoryTracker::get_instance().unwrap();

        Self {
            current_size,
            buffer,
            memory_tracker,
        }
    }

    pub fn buffer(&self) -> &Vc<T> {
        &self.buffer
    }

    // Used only for cache validation tests
    // Not anymore XD
    /*
    #[cfg(test)]
    pub fn to_vec(&self) -> Vec<T> {
        let mut vec = Vec::with_capacity(self.buffer.len());
        vec.extend(self.buffer.iter().cloned());
        vec
    }
    */

    /// Pushes an element to the buffer, and if adding the element would exceed the memory limit,
    /// removes the oldest elements until there's enough space for the new element.
    /// It's preferred to use `extend` instead of this method.
    pub fn push_safe(&mut self, element: T) {
        let element_size = element.get_size_bytes();

        while !self.memory_tracker.will_fit_into_cache(element_size) {
            if let Some(oldest_element) = self.buffer.pop_front() {
                let oldest_size = oldest_element.get_size_bytes();
                self.memory_tracker
                    .decrement_used_memory(oldest_size.as_bytes_u64());
                self.current_size -= oldest_size;
            }
        }

        self.memory_tracker
            .increment_used_memory(element_size.as_bytes_u64());
        self.current_size += element_size;
        self.buffer.push_back(element);
    }

    /// Removes the oldest elements until there's enough space for the new element.
    pub fn evict_by_size(&mut self, size_to_remove: u64) {
        let mut removed_size = IggyByteSize::default();

        while let Some(element) = self.buffer.pop_front() {
            if removed_size >= size_to_remove {
                break;
            }
            let elem_size = element.get_size_bytes();
            self.memory_tracker
                .decrement_used_memory(elem_size.as_bytes_u64());
            self.current_size -= elem_size;
            removed_size += elem_size;
        }
    }

    pub fn purge(&mut self) {
        self.buffer.clear();
        self.memory_tracker
            .decrement_used_memory(self.current_size.as_bytes_u64());
        self.current_size = IggyByteSize::default();
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn current_size(&self) -> IggyByteSize {
        self.current_size
    }

    /// Extends the buffer with the given elements, and always adding the elements,
    /// even if it exceeds the memory limit.
    pub fn extend(&mut self, elements: impl IntoIterator<Item = T>) {
        let elements = elements.into_iter().inspect(|element| {
            let element_size = element.get_size_bytes();
            self.memory_tracker
                .increment_used_memory(element_size.as_bytes_u64());
            self.current_size += element_size;
        });
        self.buffer.extend(elements);
    }

    pub fn push(&mut self, element: T) {
        let element_size = element.get_size_bytes();
        self.memory_tracker
            .increment_used_memory(element_size.as_bytes_u64());
        self.current_size += element_size;
        self.buffer.push(element);
    }

    /// Always appends the element into the buffer, even if it exceeds the memory limit.
    pub fn append(&mut self, element: T) {
        let element_size = element.get_size_bytes();
        self.memory_tracker
             .increment_used_memory(element_size.as_bytes_u64());
        self.current_size += element_size;
        self.buffer.push(element);
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.buffer.iter()
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }
}

impl<T> Index<usize> for SmartCache<T>
where
    T: LocalSizeable + Debug,
{
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.buffer[index]
    }
}

impl<T: LocalSizeable + Debug> Default for SmartCache<T> {
    fn default() -> Self {
        Self::new()
    }
}
