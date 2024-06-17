use tokio::fs::File;
use tokio::io::BufWriter;

const BUF_WRITER_CAPACITY_BYTES: usize = 512 * 1000;

pub struct RetainedBatchWriter {
    pub log_writer: BufWriter<File>,
    pub index_writer: BufWriter<File>,
    pub time_index_writer: BufWriter<File>,
}

impl RetainedBatchWriter {
    pub fn init(log_file: File, index_file: File, time_index_file: File) -> Self {
        RetainedBatchWriter {
            log_writer: BufWriter::with_capacity(BUF_WRITER_CAPACITY_BYTES, log_file),
            index_writer: BufWriter::with_capacity(BUF_WRITER_CAPACITY_BYTES, index_file),
            time_index_writer: BufWriter::with_capacity(BUF_WRITER_CAPACITY_BYTES, time_index_file),
        }
    }
}
