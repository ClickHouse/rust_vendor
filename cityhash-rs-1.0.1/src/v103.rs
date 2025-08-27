use crate::base::*;
use crate::v102::city_murmur;

pub(crate) fn cityhash_103_128_with_seed(mut input: &[u8], seed: (u64, u64)) -> u128 {
    if input.len() < 128 {
        return city_murmur(input, seed);
    }
    let mut x = seed.0;
    let mut y = seed.1;
    let mut z = (input.len() as u64).wrapping_mul(K1);

    let mut v = (0u64, 0u64);
    v.0 = (y ^ K1)
        .rotate_right(49)
        .wrapping_mul(K1)
        .wrapping_add(fetch_64(input));
    v.1 =
        v.0.rotate_right(42)
            .wrapping_mul(K1)
            .wrapping_add(fetch_64(&input[8..]));
    let mut w = (0u64, 0u64);
    w.0 = y
        .wrapping_add(z)
        .rotate_right(35)
        .wrapping_mul(K1)
        .wrapping_add(x);
    w.1 = x
        .wrapping_add(fetch_64(&input[88..]))
        .rotate_right(53)
        .wrapping_mul(K1);

    let original_input = input;
    loop {
        x = x
            .wrapping_add(y)
            .wrapping_add(v.0)
            .wrapping_add(fetch_64(&input[8..]))
            .rotate_right(37)
            .wrapping_mul(K1);
        y = y
            .wrapping_add(v.1)
            .wrapping_add(fetch_64(&input[48..]))
            .rotate_right(42)
            .wrapping_mul(K1);
        x ^= w.1;
        y = y.wrapping_add(v.0.wrapping_add(fetch_64(&input[40..])));
        z = z.wrapping_add(w.0).rotate_right(33).wrapping_mul(K1);
        v = weak_hash_32_with_seeds(input, v.1.wrapping_mul(K1), x.wrapping_add(w.0));
        w = weak_hash_32_with_seeds(
            &input[32..],
            z.wrapping_add(w.1),
            y.wrapping_add(fetch_64(&input[16..])),
        );
        core::mem::swap(&mut z, &mut x);
        input = &input[64..];

        x = x
            .wrapping_add(y)
            .wrapping_add(v.0)
            .wrapping_add(fetch_64(&input[8..]))
            .rotate_right(37)
            .wrapping_mul(K1);
        y = y
            .wrapping_add(v.1)
            .wrapping_add(fetch_64(&input[48..]))
            .rotate_right(42)
            .wrapping_mul(K1);
        x ^= w.1;
        y = y.wrapping_add(v.0.wrapping_add(fetch_64(&input[40..])));
        z = z.wrapping_add(w.0).rotate_right(33).wrapping_mul(K1);
        v = weak_hash_32_with_seeds(input, v.1.wrapping_mul(K1), x.wrapping_add(w.0));
        w = weak_hash_32_with_seeds(
            &input[32..],
            z.wrapping_add(w.1),
            y.wrapping_add(fetch_64(&input[16..])),
        );
        core::mem::swap(&mut z, &mut x);
        input = &input[64..];

        if input.len() < 128 {
            break;
        }
    }

    x = x.wrapping_add(v.0.wrapping_add(z).rotate_right(49).wrapping_mul(K0));
    z = z.wrapping_add(w.0.rotate_right(37).wrapping_mul(K0));

    let mut tail_done: usize = 0;
    while tail_done < input.len() {
        tail_done += 32;
        y = x
            .wrapping_add(y)
            .rotate_right(42)
            .wrapping_mul(K0)
            .wrapping_add(v.1);
        w.0 = w.0.wrapping_add(fetch_64(
            &original_input[original_input.len() - tail_done + 16..],
        ));
        x = x.wrapping_mul(K0).wrapping_add(w.0);
        z = z.wrapping_add(w.1.wrapping_add(fetch_64(
            &original_input[original_input.len() - tail_done..],
        )));
        w.1 = w.1.wrapping_add(v.0);
        v = weak_hash_32_with_seeds(
            &original_input[original_input.len() - tail_done..],
            v.0.wrapping_add(z),
            v.1,
        );
    }

    x = hash_16(x, v.0);
    y = hash_16(y.wrapping_add(z), w.0);
    let part1 = hash_16(x.wrapping_add(v.1), w.1)
        .wrapping_add(y)
        .to_be_bytes();
    let part2 = hash_16(x.wrapping_add(w.1), y.wrapping_add(v.1)).to_be_bytes();

    let mut out_buf: [u8; 16] = [0u8; 16];
    (&mut out_buf[..8]).copy_from_slice(&part1[..]);
    (&mut out_buf[8..]).copy_from_slice(&part2[..]);
    u128::from_be_bytes(out_buf)
}

/// Implementation of cityhash v1.0.3
pub fn cityhash_103_128(input: &[u8]) -> u128 {
    if input.len() >= 16 {
        let seed_part1: u64 = fetch_64(input) ^ K3;
        let seed_part2: u64 = fetch_64(&input[8..]);
        cityhash_103_128_with_seed(&input[16..], (seed_part1, seed_part2))
    } else if input.len() >= 8 {
        let seed_part1: u64 = fetch_64(input) ^ (input.len() as u64).wrapping_mul(K0);
        let seed_part2: u64 = fetch_64(&input[input.len() - 8..]) ^ K1;
        cityhash_103_128_with_seed(&[], (seed_part1, seed_part2))
    } else {
        cityhash_103_128_with_seed(input, (K0, K1))
    }
}
