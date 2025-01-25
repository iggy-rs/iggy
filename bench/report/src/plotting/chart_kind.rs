use derive_more::derive::Display;

#[derive(Debug, Display)]
pub enum ChartKind {
    #[display("Throughput")]
    Throughput,
    #[display("Latency")]
    Latency,
}
