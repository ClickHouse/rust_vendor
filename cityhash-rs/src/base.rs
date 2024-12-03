use core::convert::TryInto;

pub const K0: u64 = 0xc3a5c85c97cb3127;
pub const K1: u64 = 0xb492b66fbe98f273;
pub const K2: u64 = 0x9ae16a3b2f90404f;
pub const K_MUL: u64 = 0x9ddfea08eb382d69;
pub const K3: u64 = 0xc949d7c7509e6557;

#[inline]
pub fn fetch_64(input: &[u8]) -> u64 {
    u64::from_le_bytes(input[..8].try_into().unwrap())
}

#[inline]
pub fn fetch_32(input: &[u8]) -> u64 {
    u32::from_le_bytes(input[..4].try_into().unwrap()) as u64
}

#[inline]
pub fn raw_weak_hash_32_with_seeds(
    w: u64,
    x: u64,
    y: u64,
    z: u64,
    mut a: u64,
    mut b: u64,
) -> (u64, u64) {
    a = a.wrapping_add(w);
    b = b.wrapping_add(a).wrapping_add(z).rotate_right(21);
    let c = a;
    a = a.wrapping_add(x);
    a = a.wrapping_add(y);
    b = b.wrapping_add(a.rotate_right(44));
    (a.wrapping_add(z), b.wrapping_add(c))
}

#[inline]
pub fn weak_hash_32_with_seeds(input: &[u8], a: u64, b: u64) -> (u64, u64) {
    raw_weak_hash_32_with_seeds(
        fetch_64(input),
        fetch_64(&input[8..]),
        fetch_64(&input[16..]),
        fetch_64(&input[24..]),
        a,
        b,
    )
}

pub fn hash_16(u: u64, v: u64) -> u64 {
    let mut a = (u ^ v).wrapping_mul(K_MUL);
    a ^= a >> 47;
    let mut b = (v ^ a).wrapping_mul(K_MUL);
    b ^= b >> 47;
    b = b.wrapping_mul(K_MUL);
    b
}

pub fn hash_16_mul(u: u64, v: u64, mul: u64) -> u64 {
    let mut a = (u ^ v).wrapping_mul(mul);
    a ^= a >> 47;
    let mut b = (v ^ a).wrapping_mul(mul);
    b ^= b >> 47;
    b = b.wrapping_mul(mul);
    b
}

pub fn hash_110_0_to_16(input: &[u8]) -> u64 {
    let len = input.len() as u64;
    if len >= 8 {
        let mul = K2.wrapping_add(len.wrapping_mul(2));
        let a = fetch_64(input).wrapping_add(K2);
        let b = fetch_64(&input[input.len() - 8..]);
        let c = b.rotate_right(37).wrapping_mul(mul).wrapping_add(a);
        let d = a.rotate_right(25).wrapping_add(b).wrapping_mul(mul);
        hash_16_mul(c, d, mul)
    } else if len >= 4 {
        let mul = K2.wrapping_add(len.wrapping_mul(2));
        let a = fetch_32(input);
        hash_16_mul(
            len.wrapping_add(a << 3),
            fetch_32(&input[input.len() - 4..]),
            mul,
        )
    } else if len > 0 {
        let a = input[0];
        let b = input[input.len() >> 1];
        let c = input[input.len() - 1];
        let y = a as u32 + ((b as u32) << 8);
        let z = (len as u32).wrapping_add((c as u32) << 2);
        shift_mix((y as u64).wrapping_mul(K2) ^ (z as u64).wrapping_mul(K0)).wrapping_mul(K2)
    } else {
        K2
    }
}

pub fn hash_103_0_to_16(input: &[u8]) -> u64 {
    let len = input.len() as u64;
    if len > 8 {
        let a = fetch_64(input);
        let b = fetch_64(&input[input.len() - 8..]);
        hash_16(
            a,
            b.wrapping_add(input.len() as u64)
                .rotate_right(input.len() as u32),
        ) ^ b
    } else if len >= 4 {
        let a = fetch_32(input);
        hash_16(
            len.wrapping_add(a << 3),
            fetch_32(&input[input.len() - 4..]),
        )
    } else if len > 0 {
        let a = input[0];
        let b = input[input.len() >> 1];
        let c = input[input.len() - 1];
        let y = a as u32 + ((b as u32) << 8);
        let z = (len as u32).wrapping_add((c as u32) << 2);
        shift_mix((y as u64).wrapping_mul(K2) ^ (z as u64).wrapping_mul(K3)).wrapping_mul(K2)
    } else {
        K2
    }
}

#[inline]
pub fn shift_mix(val: u64) -> u64 {
    val ^ (val >> 47)
}
