use rand::Rng;
use std::convert::TryInto;
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};

// This code comes mostly from https://docs.rs/fastuuid/latest/fastuuid/
// Few tweaks were made to get u128 directly

// Generator is a uuid generator that generates unique and guessable 192-bit UUIDs, starting from a random sequence.
#[derive(Debug)]
pub struct UuidGenerator {
    // The constant (random) 192-bit seed.
    // the first 8 bytes are stored in the counter and used for generating new UUIDs
    seed: [u8; 24],
    counter: AtomicUsize,
}

impl UuidGenerator {
    #[allow(dead_code)]
    pub fn new() -> UuidGenerator {
        let seed = rand::thread_rng().gen::<[u8; 24]>();
        UuidGenerator {
            seed,
            counter: AtomicUsize::new(
                u64::from_le_bytes(seed[0..8].try_into().unwrap())
                    .try_into()
                    .unwrap(),
            ),
        }
    }

    // Next returns the next UUID from the generator.
    // Only the first 8 bytes differ from the previous one.
    // It can be used concurrently.
    pub fn next(&self) -> [u8; 24] {
        let current = self.counter.fetch_add(1, Ordering::SeqCst);
        let mut uuid: [u8; 24] = Default::default();
        uuid[..8].copy_from_slice(&current.to_le_bytes());
        uuid[8..].copy_from_slice(&self.seed[8..]);
        uuid
    }

    pub fn hex128(&self) -> u128 {
        let uuid = self.next();
        let (high, low): (u64, u64) = (
            u64::from_le_bytes(uuid[0..8].try_into().unwrap()),
            u64::from_le_bytes(uuid[8..16].try_into().unwrap()),
        );
        ((high as u128) << 64) | (low as u128)
    }

    // hex128_as_str returns hex128(Generator::next()) as &str (without heap allocation of the result)
    pub fn hex128_as_str<'a>(&self, buffer: &'a mut [u8; 36]) -> Result<&'a str, Box<dyn Error>> {
        match std::str::from_utf8(UuidGenerator::hex128_from_bytes(&self.next(), buffer)) {
            Ok(res) => Ok(res),
            Err(err) => Err(Box::new(err)),
        }
    }

    // hex128_as_string returns hex128(Generator::next()) as boxed String value
    pub fn hex128_as_string(&self) -> Result<String, Box<dyn Error>> {
        let mut buffer: [u8; 36] = [0; 36];
        match std::str::from_utf8(UuidGenerator::hex128_from_bytes(&self.next(), &mut buffer)) {
            Ok(res) => Ok(res.to_owned()),
            Err(err) => Err(Box::new(err)),
        }
    }

    // Hex128 returns an RFC4122 V4 representation of the
    // first 128 bits of the given UUID, with hyphens.
    //
    // Example: 11febf98-c108-4383-bb1e-739ffcd44341
    //
    // Before encoding, it swaps bytes 6 and 9
    // so that all the varying bits of Generator.next()
    // are reflected in the resulting UUID.
    //
    // Note: If you want unpredictable UUIDs, you might want to consider
    // hashing the uuid (using SHA256, for example) before passing it
    // to Hex128.
    fn hex128_from_bytes<'a>(uuid: &[u8; 24], buffer: &'a mut [u8; 36]) -> &'a [u8] {
        let mut temp_uuid: [u8; 24] = [0; 24];
        temp_uuid.copy_from_slice(uuid);
        temp_uuid.swap(6, 9);

        // V4
        temp_uuid[6] = (temp_uuid[6] & 0x0f) | 0x40;
        // RFC4122
        temp_uuid[8] = temp_uuid[8] & 0x3f | 0x80;

        faster_hex::hex_encode(&temp_uuid[0..16], &mut buffer[0..32]).unwrap();
        buffer.copy_within(20..32, 24); // needs rust stable 1.37.0!!
        buffer.copy_within(16..20, 19);
        buffer.copy_within(12..16, 14);
        buffer.copy_within(8..12, 9);
        buffer[8] = b'-';
        buffer[13] = b'-';
        buffer[18] = b'-';
        buffer[23] = b'-';
        &buffer[..]
    }

    //  Returns true if provided string is a valid 128-bit UUID
    pub fn is_valid_hex128(uuid: &str) -> bool {
        let uuid_bytes = uuid.as_bytes();
        if uuid.len() != 36
            || uuid_bytes.len() != 36
            || uuid_bytes[8] != b'-'
            || uuid_bytes[13] != b'-'
            || uuid_bytes[18] != b'-'
            || uuid_bytes[23] != b'-'
        {
            return false;
        }

        UuidGenerator::valid_hex(&uuid[..8])
            && UuidGenerator::valid_hex(&uuid[9..13])
            && UuidGenerator::valid_hex(&uuid[14..18])
            && UuidGenerator::valid_hex(&uuid[19..23])
            && UuidGenerator::valid_hex(&uuid[24..])
    }

    fn valid_hex(hex: &str) -> bool {
        hex.chars().all(|c| c.is_ascii_digit())
    }
}

impl Default for UuidGenerator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};
    use std::thread;

    use super::UuidGenerator;

    #[test]
    fn next() {
        let generator = UuidGenerator::new();
        let mut first = generator.next();

        for _ in 0..10 {
            let second = generator.next();
            assert_eq!(
                first.len(),
                second.len(),
                "Arrays don't have the same length"
            );

            first = second;
        }
    }

    #[test]
    fn hex128() {
        let generator = UuidGenerator::new();
        let mut buffer: [u8; 36] = [0; 36];

        assert!(
            UuidGenerator::is_valid_hex128(generator.hex128_as_str(&mut buffer).unwrap()),
            "should be valid hex"
        );
    }

    #[test]
    fn uniqueness() {
        let mut uuids: HashMap<String, bool> = HashMap::new();
        let generator = UuidGenerator::new();
        let mut buffer: [u8; 36] = [0; 36];

        for _ in 0..100000 {
            let next = generator.hex128_as_str(&mut buffer).unwrap();
            assert!(!uuids.contains_key(&next.to_string()), "duplicate found");
            uuids.insert(next.to_string(), true);
        }
    }

    #[test]
    fn uniqueness_concurrent() {
        let generator = Arc::new(UuidGenerator::new());
        let data = Arc::new(RwLock::new(HashMap::new()));
        let threads: Vec<_> = (0..100)
            .map(|_| {
                let data = Arc::clone(&data);
                let generator = generator.clone();
                thread::spawn(move || {
                    let mut map = data.write().unwrap();
                    map.insert(generator.hex128_as_string().unwrap(), true);
                })
            })
            .collect();

        for t in threads {
            t.join().expect("Thread panicked");
        }

        let map = data.read().unwrap();
        assert_eq!(map.len(), 100, "generated non-unique uuids");
    }

    #[test]
    fn valid_hex() {
        // valid v4 uuid
        assert!(
            UuidGenerator::is_valid_hex128("11febf98-c108-4383-bb1e-739ffcd44341"),
            "should be valid hex"
        );

        // invalid uuid
        assert!(
            !UuidGenerator::is_valid_hex128("11febf98-c108-4383-bb1e-739ffcd4434"),
            "should be invalid hex"
        );
        assert!(
            !UuidGenerator::is_valid_hex128("11febf98-c108-4383-bb1e-739ffcd443412"),
            "should be invalid hex"
        );
        assert!(
            !UuidGenerator::is_valid_hex128("11febf98c1-08-4383-bb1e-739ffcd44341"),
            "should be invalid hex"
        );
        assert!(
            !UuidGenerator::is_valid_hex128("11febf98-c1084-383-bb1e-739ffcd44341"),
            "should be invalid hex"
        );
        assert!(
            !UuidGenerator::is_valid_hex128("11febf98-c108-4383bb-1e-739ffcd44341"),
            "should be invalid hex"
        );
        assert!(
            !UuidGenerator::is_valid_hex128("11febf98-c108-4383-bb1e7-39ffcd44341"),
            "should be invalid hex"
        );
    }
}
