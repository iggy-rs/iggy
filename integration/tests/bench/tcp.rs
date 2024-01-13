use super::run_bench_and_wait_for_finish;
use integration::test_server::{IpAddrKind, TestServer, Transport};
use serial_test::parallel;

#[test]
#[parallel]
fn tcp_ipv4_bench() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    run_bench_and_wait_for_finish(&server_addr, Transport::Tcp);
}

#[cfg_attr(feature = "ci-qemu", ignore)]
#[test]
#[parallel]
fn tcp_ipv6_bench() {
    let mut test_server = TestServer::new(None, true, None, IpAddrKind::V6);
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    run_bench_and_wait_for_finish(&server_addr, Transport::Tcp);
}
