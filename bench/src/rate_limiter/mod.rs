use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time::sleep;

/// Thread-safe rate limiter using linger-based algorithm
pub struct RateLimiter {
    bytes_per_second: u64,
    last_operation: Arc<Mutex<Instant>>,
}

impl RateLimiter {
    pub fn new(bytes_per_second: u64) -> Self {
        Self {
            bytes_per_second,
            last_operation: Arc::new(Mutex::new(Instant::now())),
        }
    }

    /// Wait for the required time based on the desired throughput rate
    pub async fn wait_and_consume(&self, bytes: u64) {
        let now = Instant::now();
        let mut last_op = self.last_operation.lock().await;

        // Calculate time per byte in seconds
        let time_per_byte = 1.0 / self.bytes_per_second as f64;

        // Calculate target duration for these bytes
        let target_duration = Duration::from_secs_f64(bytes as f64 * time_per_byte);

        // Calculate how long it's been since last operation
        let elapsed = now.duration_since(*last_op);

        // If we need to wait longer, sleep for the remaining time
        if elapsed < target_duration {
            let sleep_duration = target_duration - elapsed;
            // Update last_op before sleeping to account for operation time
            *last_op = now + sleep_duration;
            drop(last_op); // Release the lock before sleeping
            sleep(sleep_duration).await;
        } else {
            // If we don't need to wait, just update the timestamp
            *last_op = now;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rate_limiter() {
        let limiter = RateLimiter::new(1000); // 1000 bytes per second
        let start = Instant::now();

        // Try to send 100 bytes 5 times
        for _ in 0..5 {
            limiter.wait_and_consume(100).await;
        }

        // Should take approximately 0.5 seconds (500ms) to send 500 bytes at 1000 bytes/sec
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(450)); // Allow some wiggle room
        assert!(elapsed <= Duration::from_millis(550));
    }
}
