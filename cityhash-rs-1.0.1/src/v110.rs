use crate::base::*;

fn city_murmur(mut input: &[u8], (mut a, mut b): (u64, u64)) -> u128 {
    let mut c: u64;
    let mut d: u64;
    if input.len() <= 16 {
        a = shift_mix(a.wrapping_mul(K1)).wrapping_mul(K1);
        c = b.wrapping_mul(K1).wrapping_add(hash_110_0_to_16(input));
        d = shift_mix(a.wrapping_add(if input.len() >= 8 { fetch_64(input) } else { c }));
    } else {
        c = hash_16(fetch_64(&input[input.len() - 8..]).wrapping_add(K1), a);
        d = hash_16(
            b.wrapping_add(input.len() as u64),
            c.wrapping_add(fetch_64(&input[input.len() - 16..])),
        );
        a = a.wrapping_add(d);
        loop {
            a ^= shift_mix(fetch_64(input).wrapping_mul(K1)).wrapping_mul(K1);
            a = a.wrapping_mul(K1);
            b ^= a;
            c ^= shift_mix(fetch_64(&input[8..]).wrapping_mul(K1)).wrapping_mul(K1);
            c = c.wrapping_mul(K1);
            d ^= c;
            input = &input[16..];
            if input.len() <= 16 {
                break;
            }
        }
    }

    a = hash_16(a, c);
    b = hash_16(d, b);
    d = hash_16(b, a);
    a ^= b;
    let mut out_buf: [u8; 16] = [0u8; 16];
    (&mut out_buf[..8]).copy_from_slice(&a.to_be_bytes()[..]);
    (&mut out_buf[8..]).copy_from_slice(&d.to_be_bytes()[..]);
    u128::from_be_bytes(out_buf)
}

pub(crate) fn cityhash_110_128_with_seed(mut input: &[u8], seed: (u64, u64)) -> u128 {
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
    y = y.wrapping_mul(K0).wrapping_add(w.1.rotate_right(37));
    z = z.wrapping_mul(K0).wrapping_add(w.0.rotate_right(27));
    w.0 = w.0.wrapping_mul(9);
    v.0 = v.0.wrapping_mul(K0);

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
        v.0 = v.0.wrapping_mul(K0);
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

/// Implementation of cityhash v1.1.0
pub fn cityhash_110_128(input: &[u8]) -> u128 {
    if input.len() >= 16 {
        let seed_part1: u64 = fetch_64(input);
        let seed_part2: u64 = fetch_64(&input[8..]).wrapping_add(K0);
        cityhash_110_128_with_seed(&input[16..], (seed_part1, seed_part2))
    } else {
        cityhash_110_128_with_seed(input, (K0, K1))
    }
}
