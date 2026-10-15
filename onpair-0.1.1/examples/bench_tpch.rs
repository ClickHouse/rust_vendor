// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
//! Standalone OnPair compression/decompression benchmark over TPC-H columns.
#![allow(
    clippy::cast_possible_truncation,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::unwrap_in_result,
    clippy::unwrap_used
)]
//
// End-to-end microbenchmark for the OnPair compressor on TPC-H `l_comment` (or
// any UTF-8 string column from a parquet file). Reports compress/decompress
// throughput plus the achieved compression ratio for bit widths 12 and 16.
//
// Data source:
//   * env `ONPAIR_BENCH_PARQUET` (+ optional `ONPAIR_BENCH_COLUMN`) — read a
//     UTF-8 column from a parquet file (e.g. TPC-H lineitem `l_comment`).
//   * else a synthetic TPC-H-comment-shaped English corpus.
//
// Optional env:
//   * `ONPAIR_BENCH_MAX_BYTES` — cap the corpus at N bytes (default 1 GiB).
//   * `ONPAIR_BENCH_ITERS`     — timed iterations per phase (default 3).
//   * `ONPAIR_BENCH_THRESHOLD` — dynamic-threshold sample fraction (default 0.2).
//
// Run: cargo run --release --example bench_tpch

use std::env;
use std::fs::File;
use std::path::PathBuf;
use std::time::Instant;

use std::mem::MaybeUninit;

use arrow_array::Array;
use arrow_array::cast::AsArray;
use onpair::Config;
use onpair::DECODE_PADDING;
use onpair::MaxDictBits;
use onpair::Threshold;
use onpair::compress;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

const BITS: &[u8] = &[12, 16];

fn main() {
    let max_bytes = env::var("ONPAIR_BENCH_MAX_BYTES")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1 << 30);
    let iters = env::var("ONPAIR_BENCH_ITERS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(3);
    let threshold = env::var("ONPAIR_BENCH_THRESHOLD")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .map(|t| Threshold::new(t).expect("ONPAIR_BENCH_THRESHOLD must be in (0.0, 1.0]"))
        .unwrap_or_else(|| Threshold::new(0.2).expect("0.2 is in range"));

    let (source, bytes, offsets) = load_corpus(max_bytes);
    let n = offsets.len() - 1;
    let total = bytes.len();
    let mib = total as f64 / (1024.0 * 1024.0);
    println!("corpus: {source}\n  rows = {n}, bytes = {mib:.2} MiB");

    for &bits in BITS {
        println!("\n=== bits = {bits} ===");
        let cfg = Config {
            max_dict_bits: MaxDictBits::new(bits).expect("BITS entries are in 9..=16"),
            threshold,
            seed: Some(42),
        };

        let mut compress_secs = f64::MAX;
        let mut col = compress(&bytes, &offsets, cfg).expect("compress");
        for _ in 0..iters {
            let t = Instant::now();
            col = compress(&bytes, &offsets, cfg).expect("compress");
            compress_secs = compress_secs.min(t.elapsed().as_secs_f64());
        }

        let mut decompress_secs = f64::MAX;
        // Caller-owned output buffer, sized from the column's decoded length.
        let mut buf = vec![MaybeUninit::uninit(); col.view().decoded_len() + DECODE_PADDING];
        let mut decoded_len = 0usize;
        for _ in 0..iters {
            let t = Instant::now();
            // SAFETY: freshly-compressed (trusted) column; `buf` is sized to
            // `decoded_len() + DECODE_PADDING`.
            decoded_len = unsafe { col.view().decompress_into(&mut buf) };
            decompress_secs = decompress_secs.min(t.elapsed().as_secs_f64());
        }
        // SAFETY: the decoder initialized exactly `decoded_len` leading bytes.
        let decoded = unsafe { std::slice::from_raw_parts(buf.as_ptr().cast::<u8>(), decoded_len) };

        let dict_bytes = col.dict.bytes().len();
        let dict_offsets = col.dict.offsets().len() * 4;
        let codes = col.codes.len() * 2;
        let row_offsets = std::mem::size_of_val(col.row_offsets.as_slice());
        let compressed = dict_bytes + dict_offsets + codes + row_offsets;
        let comp_mib = compressed as f64 / (1024.0 * 1024.0);

        println!(
            "  compress:   {compress_secs:.3}s  {:.1} MiB/s",
            mib / compress_secs
        );
        println!(
            "  decompress: {decompress_secs:.3}s  {:.1} MiB/s",
            mib / decompress_secs
        );
        println!(
            "  dict tokens = {}, dict bytes = {dict_bytes}, codes = {} ({} bytes)",
            col.dict.offsets().len() - 1,
            col.codes.len(),
            codes
        );
        println!(
            "  compressed = {comp_mib:.2} MiB, ratio = {:.3}x",
            mib / comp_mib
        );

        let roundtrip_ok = decoded == bytes.as_slice();
        println!(
            "  roundtrip: {} (decoded={:.2} MiB)",
            if roundtrip_ok { "PASS" } else { "FAIL" },
            decoded.len() as f64 / (1024.0 * 1024.0)
        );
        assert!(roundtrip_ok, "roundtrip mismatch at bits={bits}");
    }
}

fn load_corpus(max_bytes: usize) -> (String, Vec<u8>, Vec<u64>) {
    if let Ok(path) = env::var("ONPAIR_BENCH_PARQUET")
        && let Some((bytes, offsets)) = read_parquet(&PathBuf::from(&path), max_bytes)
    {
        return (format!("{path} (parquet)"), bytes, offsets);
    }
    let (bytes, offsets) = synthetic(max_bytes);
    (
        "synthetic TPC-H-comment-shaped corpus".to_string(),
        bytes,
        offsets,
    )
}

fn read_parquet(path: &PathBuf, max_bytes: usize) -> Option<(Vec<u8>, Vec<u64>)> {
    let file = File::open(path).ok()?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).ok()?;
    let schema = builder.schema().clone();
    let col_name = env::var("ONPAIR_BENCH_COLUMN").ok();
    let picked = match col_name.as_deref() {
        Some(name) => schema.fields().iter().position(|f| f.name() == name)?,
        None => schema.fields().iter().position(|f| {
            use arrow_schema::DataType::*;
            matches!(f.data_type(), Utf8 | LargeUtf8 | Utf8View)
        })?,
    };
    eprintln!(
        "[bench] column #{picked} `{}`",
        schema.fields().get(picked).unwrap().name()
    );

    let mut bytes = Vec::new();
    let mut offsets = vec![0u64];
    let reader = builder.build().ok()?;
    'outer: for batch in reader.flatten() {
        let arr = batch.column(picked);
        use arrow_schema::DataType::*;
        macro_rules! push_iter {
            ($it:expr) => {
                for s in $it {
                    let b = s.unwrap_or("").as_bytes();
                    bytes.extend_from_slice(b);
                    offsets.push(bytes.len() as u64);
                    if bytes.len() >= max_bytes {
                        break 'outer;
                    }
                }
            };
        }
        match arr.data_type() {
            Utf8 => push_iter!(arr.as_string::<i32>().iter()),
            LargeUtf8 => push_iter!(arr.as_string::<i64>().iter()),
            Utf8View => push_iter!(arr.as_string_view().iter()),
            _ => return None,
        }
    }
    Some((bytes, offsets))
}

/// Build a corpus shaped like TPC-H `l_comment`: short phrases of common
/// English words separated by spaces, ~27 bytes each, with heavy word reuse.
fn synthetic(max_bytes: usize) -> (Vec<u8>, Vec<u64>) {
    const WORDS: &[&str] = &[
        "the",
        "quickly",
        "final",
        "regular",
        "ironic",
        "express",
        "packages",
        "accounts",
        "deposits",
        "foxes",
        "requests",
        "blithely",
        "carefully",
        "furiously",
        "slyly",
        "pending",
        "unusual",
        "even",
        "bold",
        "silent",
        "theodolites",
        "instructions",
        "asymptotes",
        "across",
        "above",
        "after",
        "among",
        "around",
        "thinly",
        "sometimes",
        "boldly",
        "fluffily",
    ];
    let mut bytes = Vec::with_capacity(max_bytes.min(1 << 28));
    let mut offsets = vec![0u64];
    let mut x = 0x9E3779B97F4A7C15u64;
    while bytes.len() < max_bytes {
        let nwords = 3 + (x >> 60) as usize % 7;
        let start = bytes.len();
        for w in 0..nwords {
            x = x.wrapping_add(0x9E3779B97F4A7C15);
            x ^= x >> 31;
            if w > 0 {
                bytes.push(b' ');
            }
            bytes.extend_from_slice(WORDS[(x as usize) % WORDS.len()].as_bytes());
        }
        if bytes.len() == start {
            bytes.push(b' ');
        }
        offsets.push(bytes.len() as u64);
    }
    (bytes, offsets)
}
