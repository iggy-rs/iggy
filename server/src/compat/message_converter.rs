use crate::compat::schema_sampler::BinarySchemaSampler;
use crate::compat::samplers::message_sampler::MessageSampler;
use crate::compat::samplers::retained_batch_sampler::RetainedMessageBatchSampler;

pub struct MessageFormatConverter {
    pub samplers: Vec<Box<dyn BinarySchemaSampler>>,
}

impl MessageFormatConverter {
    pub fn init(
        segment_start_offset: u64,
        log_path: String,
        index_path: String,
    ) -> MessageFormatConverter {
        // Always append new schemas to beginning of vec
        MessageFormatConverter {
            samplers: vec![
                Box::new(RetainedMessageBatchSampler::new(
                    segment_start_offset,
                    log_path.clone(),
                    index_path.clone(),
                )),
                Box::new(MessageSampler::new(
                    segment_start_offset,
                    log_path,
                    index_path,
                )),
            ],
        }
    }
}
