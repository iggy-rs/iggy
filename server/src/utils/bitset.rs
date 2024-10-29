use fixedbitset::FixedBitSet;

pub enum OptimalBitset {
    Small8(u8),
    Small16(u16),
    Small32(u32),
    Small64(u64),
    Large(FixedBitSet),
}

impl Default for OptimalBitset {
    fn default() -> Self {
        Self::Small32(0)
    }
}

impl OptimalBitset {
    pub fn new(size: usize) -> Self {
        match size {
            0..=8 => OptimalBitset::Small8(0),
            9..=16 => OptimalBitset::Small16(0),
            17..=32 => OptimalBitset::Small32(0),
            33..=64 => OptimalBitset::Small64(0),
            65.. => OptimalBitset::Large(FixedBitSet::with_capacity(size)),
        }
    }

    pub fn set(&mut self, bit: u8) {
        match self {
            OptimalBitset::Small8(bits) => *bits |= 1 << bit,
            OptimalBitset::Small16(bits) => *bits |= 1 << bit,
            OptimalBitset::Small32(bits) => *bits |= 1 << bit,
            OptimalBitset::Small64(bits) => *bits |= 1 << bit,
            OptimalBitset::Large(bits) => {
                bits.insert(bit as _);
            }
        }
    }

    pub fn is_set(&self, bit: u8) -> bool {
        match self {
            OptimalBitset::Small8(bits) => *bits & (1 << bit) != 0,
            OptimalBitset::Small16(bits) => *bits & (1 << bit) != 0,
            OptimalBitset::Small32(bits) => *bits & (1 << bit) != 0,
            OptimalBitset::Small64(bits) => *bits & (1 << bit) != 0,
            OptimalBitset::Large(bits) => bits.contains(bit as _),
        }
    }

    pub fn count(&self) -> usize {
        match self {
            OptimalBitset::Small8(bits) => bits.count_ones() as usize,
            OptimalBitset::Small16(bits) => bits.count_ones() as usize,
            OptimalBitset::Small32(bits) => bits.count_ones() as usize,
            OptimalBitset::Small64(bits) => bits.count_ones() as usize,
            OptimalBitset::Large(bits) => bits.count_ones(..),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_small8() {
        let mut bitset = OptimalBitset::new(8);
        bitset.set(3);
        assert!(bitset.is_set(3));
        assert!(!bitset.is_set(2));
        assert!(!bitset.is_set(4));
    }

    #[test]
    fn test_small16() {
        let mut bitset = OptimalBitset::new(16);
        bitset.set(10);
        assert!(bitset.is_set(10));
        assert!(!bitset.is_set(9));
        assert!(!bitset.is_set(11));
    }

    #[test]
    fn test_small32() {
        let mut bitset = OptimalBitset::new(32);
        bitset.set(20);
        assert!(bitset.is_set(20));
        assert!(!bitset.is_set(19));
        assert!(!bitset.is_set(21));
    }

    #[test]
    fn test_small64() {
        let mut bitset = OptimalBitset::new(64);
        bitset.set(40);
        assert!(bitset.is_set(40));
        assert!(!bitset.is_set(39));
        assert!(!bitset.is_set(41));
    }

    #[test]
    fn test_large() {
        let mut bitset = OptimalBitset::new(256);
        bitset.set(128);
        bitset.set(200);
        assert!(bitset.is_set(128));
        assert!(bitset.is_set(200));
        assert!(!bitset.is_set(127));
        assert!(!bitset.is_set(201));
    }
}
