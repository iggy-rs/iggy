use super::run_bench_and_wait_for_finish;
use iggy::utils::byte_size::IggyByteSize;
use integration::test_server::{IpAddrKind, TestServer, Transport};
use serial_test::parallel;
use std::str::FromStr;

#[test]
#[parallel]
fn quic_ipv4_bench() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_quic_udp_addr().unwrap();
    run_bench_and_wait_for_finish(
        &server_addr,
        Transport::Quic,
        "send",
        IggyByteSize::from_str("10MB").unwrap(),
    );
    run_bench_and_wait_for_finish(
        &server_addr,
        Transport::Quic,
        "poll",
        IggyByteSize::from_str("10MB").unwrap(),
    );
}

#[cfg_attr(feature = "ci-qemu", ignore)]
#[test]
#[parallel]
fn quic_ipv6_bench() {
    let mut test_server = TestServer::new(None, true, None, IpAddrKind::V6);
    test_server.start();
    let server_addr = test_server.get_quic_udp_addr().unwrap();
    run_bench_and_wait_for_finish(
        &server_addr,
        Transport::Quic,
        "send",
        IggyByteSize::from_str("10MB").unwrap(),
    );
    run_bench_and_wait_for_finish(
        &server_addr,
        Transport::Quic,
        "poll",
        IggyByteSize::from_str("10MB").unwrap(),
    );
}
