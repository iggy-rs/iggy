use crate::batching::batches_converter::BatchesConverter;
use crate::batching::BATCH_METADATA_BYTES_LEN;
use crate::bytes_serializable::BytesSerializable;
use crate::compression::compression_algorithm::CompressionAlgorithm;
use crate::compression::compressor::{Compressor, GzCompressor, Lz4Compressor, ZstdCompressor};
use crate::error::IggyError;
use crate::models::messages::{Message, MessageState};
use crate::sizeable::Sizeable;
use bytes::{Buf, BufMut, Bytes};
use std::collections::HashMap;
use std::sync::Arc;

use super::batcher::{BatchItemizer, Batcher};

/*
 Attributes Byte Structure:
 | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 |
 ---------------------------------
 | CA | CA | CA | U | U | U | U | U |

 Legend:
 CA - Compression Algorithm (Bits 0 to 3)
 U  - Unused (Bits 3 to 7)
*/
const COMPRESSION_ALGORITHM_SHIFT: u8 = 5;
const COMPRESSION_ALGORITHM_MASK: u8 = 0b11100000;

#[derive(Debug, Clone)]
pub struct MessageBatch {
    pub base_offset: u64,
    pub length: u32,
    pub last_offset_delta: u32,
    attributes: u8,
    pub messages: Bytes,
}

impl MessageBatch {
    pub fn new(
        base_offset: u64,
        length: u32,
        last_offset_delta: u32,
        attributes: u8,
        messages: Bytes,
    ) -> Self {
        Self {
            base_offset,
            length,
            last_offset_delta,
            attributes,
            messages,
        }
    }

    pub fn get_compression_algorithm(&self) -> Result<CompressionAlgorithm, IggyError> {
        let compression_algorithm =
            MessageBatchAttributes::get_compression_algorithm_code(&self.attributes);
        CompressionAlgorithm::from_code(compression_algorithm)
    }

    pub fn is_contained_or_overlapping_within_offset_range(
        &self,
        start_offset: u64,
        end_offset: u64,
    ) -> bool {
        (self.base_offset <= end_offset && self.get_last_offset() >= end_offset)
            || (self.base_offset <= start_offset && self.get_last_offset() <= end_offset)
            || (self.base_offset <= end_offset && self.get_last_offset() >= start_offset)
    }

    pub fn extend(&self, bytes: &mut Vec<u8>) {
        bytes.put_u64_le(self.base_offset);
        bytes.put_u32_le(self.length);
        bytes.put_u32_le(self.last_offset_delta);
        bytes.put_u8(self.attributes);
        bytes.extend(&self.messages);
    }

    pub fn get_last_offset(&self) -> u64 {
        self.base_offset + self.last_offset_delta as u64
    }
}

pub struct MessageBatchAttributes {
    pub compression_algorithm: CompressionAlgorithm,
}

impl MessageBatchAttributes {
    pub fn new(compression_algorithm: CompressionAlgorithm) -> Self {
        Self {
            compression_algorithm,
        }
    }

    fn change_compression_algorithm(
        attributes: u8,
        compression_algorithm: CompressionAlgorithm,
    ) -> u8 {
        let mut attributes = attributes;
        attributes &= !COMPRESSION_ALGORITHM_MASK;

        let compression_bits = (compression_algorithm.as_code() << COMPRESSION_ALGORITHM_SHIFT)
            & COMPRESSION_ALGORITHM_MASK;
        attributes |= compression_bits;
        attributes
    }

    fn get_compression_algorithm_code(attributes: &u8) -> u8 {
        (attributes & COMPRESSION_ALGORITHM_MASK) >> COMPRESSION_ALGORITHM_SHIFT
    }
}

impl From<MessageBatchAttributes> for u8 {
    fn from(attributes: MessageBatchAttributes) -> Self {
        let mut attributes_byte: u8 = 0;
        let compression_bits = (attributes.compression_algorithm.as_code()
            << COMPRESSION_ALGORITHM_SHIFT)
            & COMPRESSION_ALGORITHM_MASK;

        attributes_byte |= compression_bits;
        attributes_byte
    }
}

impl Sizeable for Arc<MessageBatch> {
    fn get_size_bytes(&self) -> u32 {
        BATCH_METADATA_BYTES_LEN + self.messages.len() as u32
    }
}

impl Sizeable for MessageBatch {
    fn get_size_bytes(&self) -> u32 {
        BATCH_METADATA_BYTES_LEN + self.messages.len() as u32
    }
}

impl<T, U> BatchesConverter<Message, U, T> for T
where
    T: IntoIterator<Item = U>,
    U: BatchItemizer<Message>,
{
    fn convert_and_filter_by_offset_range(
        self,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<Vec<Message>, IggyError> {
        self.into_iter()
            .try_fold(Vec::new(), |mut messages, batch| {
                messages.extend(
                    batch
                        .into_messages()?
                        .into_iter()
                        .filter(|msg| msg.offset >= start_offset && msg.offset <= end_offset)
                        .collect::<Vec<_>>(),
                );
                Ok(messages)
            })
    }
}

impl BatchItemizer<Message> for Arc<MessageBatch> {
    fn into_messages(self) -> Result<impl IntoIterator<Item = Message>, IggyError> {
        let compression_algorithm = self.get_compression_algorithm()?;
        let messages_count = self.last_offset_delta + 1;
        let mut messages = Vec::with_capacity(messages_count as usize);
        let buffer = &self.messages;

        let mut decompressed = match compression_algorithm {
            // This clone should be cheap as it's Bytes
            CompressionAlgorithm::None => buffer.clone(),
            _ => {
                let compressor: Box<dyn Compressor> = match compression_algorithm {
                    CompressionAlgorithm::Gzip => Box::<GzCompressor>::default(),
                    CompressionAlgorithm::Lz4 => Box::<Lz4Compressor>::default(),
                    CompressionAlgorithm::Zstd => Box::<ZstdCompressor>::default(),
                    _ => unreachable!("Unsupported compression algorithm"),
                };

                let compression_ratio = 1.4;
                let buffer_size = (buffer.len() as f64 * compression_ratio) as usize;
                let mut decompression_buffer = Vec::with_capacity(buffer_size);

                // In the future, we can look into moving this closer to I/O layer,
                // flate2 encoders can take advantage of BufRead
                // https://docs.rs/flate2/latest/flate2/bufread/struct.GzEncoder.html
                compressor.decompress(buffer.as_ref(), &mut decompression_buffer)?;
                Bytes::from(decompression_buffer)
            }
        };

        while decompressed.remaining() > 0 {
            let offset = decompressed.get_u64_le();
            let state_code = decompressed.get_u8();
            let state = MessageState::from_code(state_code)?;
            let timestamp = decompressed.get_u64_le();
            let id = decompressed.get_u128_le();
            let checksum = decompressed.get_u32_le();

            let headers_len = decompressed.get_u32_le();
            let headers = if headers_len > 0 {
                let headers_payload = decompressed.copy_to_bytes(headers_len as usize);
                Some(HashMap::from_bytes(headers_payload)?)
            } else {
                None
            };

            let length = decompressed.get_u32_le();
            let payload = decompressed.copy_to_bytes(length as usize);
            messages.push(Message {
                offset,
                state,
                timestamp,
                id,
                checksum,
                headers,
                length,
                payload,
            });
        }

        Ok(messages)
    }
}

impl Batcher<Message, Arc<MessageBatch>> for Vec<Message> {
    fn into_batch(
        self,
        base_offset: u64,
        last_offset_delta: u32,
        attributes: u8,
    ) -> Result<Arc<MessageBatch>, IggyError> {
        let ca_code = MessageBatchAttributes::get_compression_algorithm_code(&attributes);
        let compression_algorithm = CompressionAlgorithm::from_code(ca_code)?;
        let mut attributes = attributes;

        let payload: Vec<_> = self
            .into_iter()
            .flat_map(|message| message.as_bytes())
            .collect();

        let payload =
            match compression_algorithm {
                CompressionAlgorithm::None => payload,
                _ => {
                    if payload.len() > compression_algorithm.min_data_size() {
                        // Let's use this simple heuristic for now,
                        // Later on, once we have proper compression metrics
                        // We can employ statistical analysis
                        let compression_ratio = 1.4;
                        let buffer_size = (payload.len() as f64 / compression_ratio) as usize;
                        let mut compression_buffer = Vec::with_capacity(buffer_size);

                        match compression_algorithm {
                            CompressionAlgorithm::Gzip => GzCompressor::default()
                                .compress(&payload, &mut compression_buffer)?,
                            CompressionAlgorithm::Lz4 => Lz4Compressor::default()
                                .compress(&payload, &mut compression_buffer)?,
                            CompressionAlgorithm::Zstd => ZstdCompressor::default()
                                .compress(&payload, &mut compression_buffer)?,
                            _ => unreachable!("Unsupported compression algorithm"),
                        };
                        compression_buffer
                    } else {
                        // Need to change attributes, to include the fact that no compression is applied
                        attributes = MessageBatchAttributes::change_compression_algorithm(
                            attributes,
                            CompressionAlgorithm::None,
                        );
                        payload
                    }
                }
            };

        let len = BATCH_METADATA_BYTES_LEN + payload.len() as u32;
        Ok(Arc::new(MessageBatch::new(
            base_offset,
            len,
            last_offset_delta,
            attributes,
            Bytes::from(payload),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn should_create_attributes_with_gzip_compression_algorithm() {
        let attributes = MessageBatchAttributes::new(CompressionAlgorithm::Gzip).into();
        let messages_batch = MessageBatch::new(1337, 69, 420, attributes, Bytes::new());
        let compression_algorithm = messages_batch.get_compression_algorithm().unwrap();

        assert_eq!(compression_algorithm, CompressionAlgorithm::Gzip);
    }
    #[test]
    fn should_create_attributes_with_no_compression_algorithm() {
        let attributes = MessageBatchAttributes::new(CompressionAlgorithm::None).into();
        let messages_batch = MessageBatch::new(1337, 69, 420, attributes, Bytes::new());
        let compression_algorithm = messages_batch.get_compression_algorithm().unwrap();

        assert_eq!(compression_algorithm, CompressionAlgorithm::None);
    }
}
