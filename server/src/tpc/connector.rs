use futures::{task::AtomicWaker, Stream};
use sharded_queue::ShardedQueue;
use std::{
    sync::{atomic::AtomicUsize, Arc},
    task::Poll,
};

pub struct ShardConnector<T> {
    pub id: u16,
    pub sender: Sender<T>,
    pub receiver: Receiver<T>,
}

impl<T> ShardConnector<T> {
    pub fn new(id: u16) -> Self {
        let channel = Arc::new(ShardedChannel::new(1));
        let (sender, receiver) = channel.unbounded();
        Self {
            id,
            receiver,
            sender,
        }
    }

    pub fn send(&self, source_id: u16, data: T) {
        self.sender.send(data);
    }
}

#[derive(Clone)]
pub struct Receiver<T> {
    channel: Arc<ShardedChannel<T>>,
}

#[derive(Clone)]
pub struct Sender<T> {
    channel: Arc<ShardedChannel<T>>,
}

impl<T> Sender<T> {
    pub fn send(&self, data: T) {
        self.channel
            .task_queue
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.channel.queue.push_back(data);
        self.channel.waker.wake();
    }
}

pub struct ShardedChannel<T> {
    queue: ShardedQueue<T>,
    task_queue: AtomicUsize,
    waker: AtomicWaker,
}

impl<T> ShardedChannel<T> {
    pub fn new(max_concurrent_thread_count: usize) -> Self {
        let waker = AtomicWaker::new();

        Self {
            queue: ShardedQueue::new(max_concurrent_thread_count),
            task_queue: AtomicUsize::new(0),
            waker,
        }
    }
}

pub trait ShardedChannelsSplit<T> {
    fn unbounded(&self) -> (Sender<T>, Receiver<T>);

    fn sender(&self) -> Sender<T>;
}

impl<T> ShardedChannelsSplit<T> for Arc<ShardedChannel<T>> {
    fn unbounded(&self) -> (Sender<T>, Receiver<T>) {
        let tx = self.sender();
        let rx = Receiver {
            channel: Arc::clone(self),
        };

        (tx, rx)
    }

    fn sender(&self) -> Sender<T> {
        Sender {
            channel: Arc::clone(self),
        }
    }
}

impl<T> Stream for Receiver<T> {
    type Item = T;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let old = self
            .channel
            .task_queue
            .load(std::sync::atomic::Ordering::Relaxed);
        if old == 0 {
            self.channel.waker.register(cx.waker());
            return Poll::Pending;
        }

        assert!(old > 0);
        self.channel
            .task_queue
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        let item = self.channel.queue.pop_front_or_spin_wait_item();
        Poll::Ready(Some(item))
    }
}
