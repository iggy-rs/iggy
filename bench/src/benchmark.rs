use crate::args::Args;
use crate::test_result::TestResult;
use async_trait::async_trait;
use tokio::task::JoinHandle;

#[async_trait]
pub(crate) trait Benchmark {
    async fn poll_messages(&self, args: &Args) -> Vec<JoinHandle<TestResult>>;
    async fn send_messages(&self, args: &Args) -> Vec<JoinHandle<TestResult>>;
}
