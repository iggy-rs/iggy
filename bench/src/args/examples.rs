const EXAMPLES: &str = r#"EXAMPLES:

1) Pinned Mode Benchmarking:

    Run benchmarks with pinned producers and consumers. This mode pins specific producers
    and consumers to specific streams and partitions:

    $ cargo r --bin iggy-bench pinned-producer --streams 10 --producers 10 tcp
    $ cargo r --bin iggy-bench pinned-consumer --streams 10 --consumers 10 tcp
    $ cargo r --bin iggy-bench pinned-producer-and-consumer --streams 10 --producers 10 --consumers 10 tcp

2) Balanced Mode Benchmarking:

    Run benchmarks with balanced distribution of producers and consumers. This mode
    automatically balances the load across streams and consumer groups:

    $ cargo r --bin iggy-bench balanced-producer --partitions 10 --producers 5 tcp
    $ cargo r --bin iggy-bench balanced-consumer-group --consumers 5 tcp
    $ cargo r --bin iggy-bench balanced-producer-and-consumer-group --partitions 2 --producers 5 --consumers 5 tcp

3) End-to-End Benchmarking:

    Run end-to-end benchmarks that measure performance for a producer that is also a consumer:

    $ cargo r --bin iggy-bench end-to-end-producing-consumer --producers 5 --consumers 5 tcp

4) Advanced Configuration:

    You can customize various parameters for any benchmark mode:

    Global options (before the benchmark command):
    --messages-per-batch (-p): Number of messages per batch [default: 1000]
    --message-batches (-b): Total number of batches [default: 1000]
    --message-size (-m): Message size in bytes [default: 1000]
    --rate-limit (-r): Optional throughput limit per producer (e.g., "50KB/s", "10MB/s")
    --warmup-time (-w): Warmup duration [default: 0s]
    --sampling-time (-t): Metrics sampling interval [default: 10ms]
    --moving-average-window (-W): Window size for moving average [default: 20]
    --cleanup: Remove server data after benchmark
    --verbose (-v): Show server output

    Benchmark-specific options (after the benchmark command):
    --streams (-s): Number of streams
    --partitions (-a): Number of partitions
    --producers (-c): Number of producers
    --consumers (-c): Number of consumers
    --max-topic-size (-t): Max topic size (e.g., "1GiB")

    Example with detailed configuration:
    $ cargo r --bin iggy-bench \
        --message-size 1000 \
        --messages-per-batch 100 \
        --message-batches 1000 \
        --rate-limit "100MB/s" \
        --warmup-time "10s" \
        --sampling-time "1s" \
        balanced-producer \
        --streams 5 \
        --partitions 2 \
        --producers 5 \
        --max-topic-size "1GiB" \
        tcp

5) Remote Server Benchmarking:

    To benchmark a remote server, specify the server address in the transport subcommand:

    $ cargo r --bin iggy-bench pinned-producer \
        --streams 5 --producers 5 \
        tcp --server-address 192.168.1.100:8090

6) Help and Documentation:

    For more details on available options:

    # General help
    $ cargo r --bin iggy-bench --help

    # Help for specific benchmark mode
    $ cargo r --bin iggy-bench pinned-producer --help
    $ cargo r --bin iggy-bench balanced-consumer-group --help
    $ cargo r --bin iggy-bench end-to-end-producing-consumer --help

    # Help for transport options
    $ cargo r --bin iggy-bench pinned-producer tcp --help
    $ cargo r --bin iggy-bench balanced-producer http --help
    $ cargo r --bin iggy-bench end-to-end-producing-consumer quic --help

"#;

pub fn print_examples() {
    print!("{}", EXAMPLES)
}
