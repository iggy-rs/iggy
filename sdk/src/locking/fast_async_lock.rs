use crate::locking::IggySharedMutFn;
use fast_async_mutex::rwlock::{RwLock as FastAsyncRwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::Arc;

#[cfg(feature = "fast_async_lock")]
#[derive(Debug)]
pub struct IggyFastAsyncRwLock<T>(Arc<FastAsyncRwLock<T>>);

impl<T> IggySharedMutFn<T> for IggyFastAsyncRwLock<T>
where
    T: Send + Sync,
{
    type ReadGuard<'a> = RwLockReadGuard<'a, T> where T: 'a;
    type WriteGuard<'a> = RwLockWriteGuard<'a, T> where T: 'a;
    fn new(data: T) -> Self {
        IggyFastAsyncRwLock(Arc::new(FastAsyncRwLock::new(data)))
    }

    async fn read<'a>(&'a self) -> Self::ReadGuard<'a>
    where
        T: 'a,
    {
        self.0.read().await
    }

    async fn write<'a>(&'a self) -> Self::WriteGuard<'a>
    where
        T: 'a,
    {
        self.0.write().await
    }
}

impl<T> Clone for IggyFastAsyncRwLock<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}
