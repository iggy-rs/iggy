use bytes::Bytes;
use iggy::error::IggyError;

pub struct RetainedMessageBatch {
    pub base_offset: u64,
    pub last_offset_delta: u32,
    pub max_timestamp: u64,
    pub length: u32,
    pub bytes: Bytes,
}

impl TryFrom<Bytes> for RetainedMessageBatch {
    type Error = IggyError;
    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        let base_offset = u64::from_le_bytes(
            value
                .get(0..8)
                .ok_or_else(|| {
                    IggyError::CannotReadMessageBatchFormatConversion(
                        "Failed to read batch base offset".to_owned(),
                    )
                })?
                .try_into()?,
        );
        let length = u32::from_le_bytes(
            value
                .get(8..12)
                .ok_or_else(|| {
                    IggyError::CannotReadMessageBatchFormatConversion(
                        "Failed to read batch length".to_owned(),
                    )
                })?
                .try_into()?,
        );
        let last_offset_delta = u32::from_le_bytes(
            value
                .get(12..16)
                .ok_or_else(|| {
                    IggyError::CannotReadMessageBatchFormatConversion(
                        "Failed to read batch last_offset_delta".to_owned(),
                    )
                })?
                .try_into()?,
        );
        let max_timestamp = u64::from_le_bytes(
            value
                .get(16..24)
                .ok_or_else(|| {
                    IggyError::CannotReadMessageBatchFormatConversion(
                        "Failed to read batch max_timestamp".to_owned(),
                    )
                })?
                .try_into()?,
        );
        let bytes = Bytes::from(
            value
                .get(24..length as usize)
                .ok_or_else(|| {
                    IggyError::CannotReadMessageBatchFormatConversion(
                        "Failed to read batch payload".to_owned(),
                    )
                })?
                .to_owned(),
        );
        Ok(RetainedMessageBatch {
            base_offset,
            last_offset_delta,
            max_timestamp,
            length,
            bytes,
        })
    }
}
