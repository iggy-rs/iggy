const MSB: u8 = 0b1000_0000;
const DROP_MSB: u8 = 0b0111_1111;

#[inline]
pub fn encode_var(value: u64, dst: &mut [u8]) -> usize {
    let mut n = value;
    let mut i = 0;

    while n >= 0x80 {
        let dst_i = unsafe { dst.get_unchecked_mut(i) };
        *dst_i = MSB | (n as u8);
        i += 1;
        n >>= 7;
    }

    let dst_i = unsafe { dst.get_unchecked_mut(i) };
    *dst_i = n as u8;
    i + 1
}

#[inline]
pub fn varint_size(value: u64) -> usize {
    let mut n = value;
    let mut i = 0;

    while n >= 0x80 {
        i += 1;
        n >>= 7;
    }

    i + 1
}

#[inline]
pub fn decode_var(src: &[u8], position: &mut usize) -> u64 {
    let mut result: u64 = 0;
    let mut shift = 0;
    let mut idx = 0;

    #[allow(unused_assignments)]
    let mut success = false;
    loop {
        let b = *unsafe { src.get_unchecked(idx)};
        let msb_dropped = b & DROP_MSB;
        result |= (msb_dropped as u64) << shift;
        shift += 7;
        idx += 1;
        *position += 1;

        if b & MSB == 0 || shift > (9 * 7) {
            success = b & MSB == 0;
            break;
        }
    }

    if success {
        result
    } else {
        panic!("Overflow while decoding varint: {}", result);
    }
}
