use std::ops::{Deref, DerefMut};

/// This module provides a trait and implementations for a shared mutable reference configurable via feature flags.
#[cfg(feature = "tokio_lock")]
//#[cfg(not(any(feature = "fast_async_lock")))]
mod tokio_lock;

// this can be used in the future to provide different locking mechanisms
//#[cfg(feature = "fast_async_lock")]
//mod parking_lot_lock;

#[cfg(feature = "tokio_lock")]
//#[cfg(not(any(feature = "fast_async_lock")))]
pub type IggySharedMut<T> = tokio_lock::IggyTokioRwLock<T>;

// this can be used in the future to provide different locking mechanisms
//#[cfg(feature = "fast_async_lock")]
//pub type IggySharedMut<T> = parking_lot_lock::IggyParkingLotRwLock<T>;

#[allow(async_fn_in_trait)]
pub trait IggySharedMutFn<T>: Send + Sync {
    type ReadGuard<'a>: Deref<Target = T> + Send
    where
        T: 'a,
        Self: 'a;
    type WriteGuard<'a>: DerefMut<Target = T> + Send
    where
        T: 'a,
        Self: 'a;

    fn new(data: T) -> Self
    where
        Self: Sized;

    async fn read<'a>(&'a self) -> Self::ReadGuard<'a>
    where
        T: 'a;

    async fn write<'a>(&'a self) -> Self::WriteGuard<'a>
    where
        T: 'a;
}
