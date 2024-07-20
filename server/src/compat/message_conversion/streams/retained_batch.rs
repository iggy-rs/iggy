use crate::io_utils::reader::{IggyFile, IggyWriter};
use monoio::fs::File;

const BUF_WRITER_CAPACITY_BYTES: usize = 512 * 1000;

pub struct RetainedBatchWriter {
    pub log_writer: IggyWriter<IggyFile>,
    pub index_writer: IggyWriter<IggyFile>,
    pub time_index_writer: IggyWriter<IggyFile>,
}

impl RetainedBatchWriter {
    pub fn init(log_file: File, index_file: File, time_index_file: File) -> Self {
        RetainedBatchWriter {
            log_writer: IggyWriter::with_capacity(BUF_WRITER_CAPACITY_BYTES, log_file),
            index_writer: IggyWriter::with_capacity(BUF_WRITER_CAPACITY_BYTES, index_file),
            time_index_writer: IggyWriter::with_capacity(
                BUF_WRITER_CAPACITY_BYTES,
                time_index_file,
            ),
        }
    }
}
