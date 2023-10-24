use super::run_bench_and_wait_for_finish;
use integration::test_server::{TestServer, Transport};
use serial_test::parallel;

#[test]
#[parallel]
fn http_bench() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_http_api_addr().unwrap();
    run_bench_and_wait_for_finish(&server_addr, Transport::Http);
}
