use crate::compat::format_sampler::BinaryFormatSampler;
use crate::compat::samplers::message_sampler::MessageSampler;

pub struct MessageFormatConverter {
    pub format_samplers: Vec<Box<dyn BinaryFormatSampler>>,
}

impl MessageFormatConverter {
    pub fn init(message_sampler: MessageSampler) -> MessageFormatConverter {
        MessageFormatConverter {
            format_samplers: vec![Box::new(message_sampler)],
        }
    }
}
