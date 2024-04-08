use iggy::compression::compression_algorithm::CompressionAlgorithm;
use iggy::error::IggyError;
/*
 Attributes Byte Structure:
 | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 14 | 15 |
 ---------------------------------
 | CA | CA | | U | U | U | U | U |

 Legend:
 CA - Compression Algorithm (Bits 0 to 2)
 U  - Unused (Bits 2 to 15)
*/

const COMPRESSION_ALGORITHM_SHIFT: u16 = 14;
const COMPRESSION_ALGORITHM_MASK: u16 = 0b1100000000000000;

pub struct RetainedMessageBatchAttributes {
    pub value: u16,
}

impl RetainedMessageBatchAttributes {
    pub fn new(value: u16) -> Self {
        Self { value }
    }
    
    pub fn init() -> Self {
        Self { value: 0 }
    }

    pub fn with_compression_algorithm(
        mut self,
        compression_algorithm: CompressionAlgorithm,
    ) -> Self {
        let compression_bits = (compression_algorithm.as_code() as u16) << COMPRESSION_ALGORITHM_SHIFT
            & COMPRESSION_ALGORITHM_MASK;
        self.value |= compression_bits;
        self
    }

    pub fn change_compression_algorithm(&mut self, compression_algorithm: CompressionAlgorithm) {
        self.value &= !COMPRESSION_ALGORITHM_MASK;
        let compression_bits = (compression_algorithm.as_code() as u16) << COMPRESSION_ALGORITHM_SHIFT
            & COMPRESSION_ALGORITHM_MASK;
        self.value |= compression_bits;
    }

    pub fn get_compression_algorithm(&self) -> Result<CompressionAlgorithm, IggyError> {
        let compression_code =
            (self.value & COMPRESSION_ALGORITHM_MASK) >> COMPRESSION_ALGORITHM_SHIFT;
        CompressionAlgorithm::from_code(compression_code as u8)
    }
}

impl From<RetainedMessageBatchAttributes> for u16 {
    fn from(attributes: RetainedMessageBatchAttributes) -> Self {
        attributes.value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const COMPRESSION_ALGORITHM_NONE: u8 = 1;
    const COMPRESSION_ALGORITHM_GZIP: u8 = 2;

    #[test]
    fn should_create_attributes_with_none_compression_algorithm() {
        let attributes = RetainedMessageBatchAttributes::init()
            .with_compression_algorithm(CompressionAlgorithm::None);
        let ca_from_attributes = attributes.get_compression_algorithm().unwrap();
        assert_eq!(ca_from_attributes.as_code(), COMPRESSION_ALGORITHM_NONE);
    }
    #[test]
    fn should_create_attributes_with_none_compression_and_successfully_change_to_gzip() {
        let mut attributes = RetainedMessageBatchAttributes::init()
            .with_compression_algorithm(CompressionAlgorithm::None);
        let ca_from_attributes = attributes.get_compression_algorithm().unwrap();
        assert_eq!(ca_from_attributes.as_code(), COMPRESSION_ALGORITHM_NONE);

        attributes.change_compression_algorithm(CompressionAlgorithm::Gzip);
        let ca_from_attributes = attributes.get_compression_algorithm().unwrap();
        assert_eq!(ca_from_attributes.as_code(), COMPRESSION_ALGORITHM_GZIP);
    }
}
