use crate::utils::test_server::TestServer;
use serial_test::parallel;

use super::{run_bench_and_wait_for_finish, Protocol};

#[test]
#[parallel]
fn quic_bench() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = format!("http://{}", test_server.get_http_api_addr().unwrap());
    run_bench_and_wait_for_finish(&server_addr, Protocol::Http);
}
