use std::fmt::{Display, Formatter};

#[derive(Clone, Copy)]
pub enum BinarySchema {
    RetainedMessageSchema,
    RetainedMessageBatchSchema,
}

impl Display for BinarySchema {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BinarySchema::RetainedMessageSchema => write!(f, "retained_message_schema"),
            BinarySchema::RetainedMessageBatchSchema => write!(f, "retained_message_batch_schema"),
        }
    }
}
