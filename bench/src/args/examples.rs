const EXAMPLES: &str = r#"EXAMPLES:

1) Default Benchmarking with TCP:

    Start server, perform given benchmark with 10 producers / 10 consumers and
    10 streams, sending 1000 message of 10000 byte each per batch, 1000 batches:

    $ cargo r --bin iggy-bench -- send tcp
    $ cargo r --bin iggy-bench -- poll tcp
    $ cargo r --bin iggy-bench -- send-and-poll tcp

    This will save around 10 GB of data on SSD.
    Keep in mind that after each benchmark, the server will be NOT be stopped.
    Server will have to be stopped manually afterwards.

2) Custom Benchmarking with cleanup and visible server stdout:

    Start server and cleanup after benchmark is done, server stdout will be visible
    perform TCP benchmark with 5 producers and 5 streams, sending 2000 messages of
    100 bytes each per batch, for 1000 batches - 20 GB of data on SSD will be saved:

    $ cargo r --bin iggy-bench -r -- -c -v send --message-size 2000 --messages-per-batch 1000 --message-batches 1000 --producers 5 --streams 5 tcp


3) Benchmarking with remote server:

    It's possible to benchmark remote server. In this case, omit the `cleanup` and `verbose` flags,
    and specify server IP after the transport:

    $ cargo r --bin iggy-bench -- send --message-size 2000 --messages-per-batch 1000 --message-batches 1000 --producers 5 --streams 5 tcp --server-address 142.250.203.142:8090

4) Other options:

    If more options are needed, please refer to the help menu:

    $ cargo r --bin iggy-bench -r -- --help

    Each subcommand has it's own help:

    $ cargo r --bin iggy-bench -r -- send --help
    $ cargo r --bin iggy-bench -r -- poll --help
    $ cargo r --bin iggy-bench -r -- send-and-poll --help

    $ cargo r --bin iggy-bench -r -- send tcp --help
    $ cargo r --bin iggy-bench -r -- poll tcp --help
    $ cargo r --bin iggy-bench -r -- send-and-poll tcp --help

"#;

pub fn print_examples() {
    print!("{}", EXAMPLES)
}
