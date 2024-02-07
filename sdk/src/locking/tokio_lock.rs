use crate::locking::IggySharedMutFn;
use std::sync::Arc;
use tokio::sync::{RwLock as TokioRwLock, RwLockReadGuard, RwLockWriteGuard};

#[cfg(feature = "tokio_lock")]
#[derive(Debug)]
pub struct IggyTokioRwLock<T>(Arc<TokioRwLock<T>>);

impl<T> IggySharedMutFn<T> for IggyTokioRwLock<T>
where
    T: Send + Sync,
{
    type ReadGuard<'a> = RwLockReadGuard<'a, T> where T: 'a;
    type WriteGuard<'a> = RwLockWriteGuard<'a, T> where T: 'a;

    fn new(data: T) -> Self {
        IggyTokioRwLock(Arc::new(TokioRwLock::new(data)))
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

impl<T> Clone for IggyTokioRwLock<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}
