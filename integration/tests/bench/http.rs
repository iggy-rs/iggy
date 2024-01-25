use super::run_bench_and_wait_for_finish;
use integration::test_server::{IpAddrKind, TestServer, Transport};
use serial_test::serial;

#[test]
#[serial]
fn http_ipv4_bench() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_http_api_addr().unwrap();
    run_bench_and_wait_for_finish(&server_addr, Transport::Http);
}

#[cfg_attr(feature = "ci-qemu", ignore)]
#[test]
#[serial]
fn http_ipv6_bench() {
    let mut test_server = TestServer::new(None, true, None, IpAddrKind::V6);
    test_server.start();
    let server_addr = test_server.get_http_api_addr().unwrap();
    run_bench_and_wait_for_finish(&server_addr, Transport::Http);
}
