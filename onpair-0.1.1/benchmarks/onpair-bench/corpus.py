"""Dataset fetcher for onpair-bench.

The module is named ``corpus`` rather than ``datasets`` so it doesn't
shadow the HuggingFace ``datasets`` package, which the fetchers below
import on demand.

Each dataset lands under ``corpora/datasets/<name>/`` (gitignored via the
existing ``corpora/`` ignore). A ``.done`` marker indicates a successful
fetch so we never re-download; clean removes the marker + payload.

Registry covers:

* ``tpch-sf0.1`` / ``tpch-sf1`` — generated locally via the duckdb tpch
  extension; every table is exported as a parquet file.
* ``clickbench`` — single ``hits.parquet`` fetched over HTTP from the
  ClickHouse hits_compatible bucket.
* The four datasets from the OnPair paper (arXiv:2508.02280 §4.3),
  sourced via the HuggingFace ``datasets`` library and written as a
  single ``.txt`` (one row per LF) so ``run.py`` picks them up verbatim:

  - ``amazon-books-titles``  — McAuley-Lab/Amazon-Reviews-2023 / raw_meta_Books / title
  - ``amazon-books-reviews`` — McAuley-Lab/Amazon-Reviews-2023 / raw_review_Books / text (first 500 MiB)
  - ``news-headlines``       — rajistics/million-headlines / headline_text
  - ``sentiment140-tweets``  — stanfordnlp/sentiment140 / text

``ensure(names)`` returns the dataset dirs so ``run.py`` can plug them
into corpus discovery. ``duckdb`` and ``datasets`` are imported on
demand and only required for the datasets that need them.

CLI:

    python corpus.py list
    python corpus.py fetch tpch-sf1 clickbench amazon-books-titles
    python corpus.py fetch-all
    python corpus.py clean tpch-sf1
    python corpus.py clean-all
    python corpus.py path tpch-sf1
"""

from __future__ import annotations

import argparse
import shutil
import sys
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable

HERE = Path(__file__).resolve().parent
DATASETS_DIR = HERE / "corpora" / "datasets"
DONE = ".done"


# --- helpers ----------------------------------------------------------------


def _download(url: str, dst: Path, *, chunk: int = 1 << 20) -> None:
    """Stream-download to a ``.part`` file and rename on success."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_name(dst.name + ".part")
    print(f"  downloading {url}", file=sys.stderr)
    # Some CDNs (Cloudflare R2 public buckets) 403 the default `Python-urllib`
    # User-Agent. Use a generic browser-ish UA — same content either way.
    req = urllib.request.Request(url, headers={"User-Agent": "onpair-bench/0.1 (+https://github.com/spiraldb/onpair)"})
    with urllib.request.urlopen(req) as resp, tmp.open("wb") as fh:
        total = int(resp.headers.get("Content-Length") or 0)
        seen = 0
        last_pct = -1
        while True:
            buf = resp.read(chunk)
            if not buf:
                break
            fh.write(buf)
            seen += len(buf)
            if total:
                pct = int(seen * 100 / total)
                if pct != last_pct:
                    print(
                        f"\r    {pct:>3d}% ({seen / 1e6:.1f} / {total / 1e6:.1f} MB)",
                        end="",
                        file=sys.stderr,
                    )
                    last_pct = pct
        if total:
            print("", file=sys.stderr)
    tmp.rename(dst)


def _write_lines(
    path: Path,
    rows: Iterable[str | bytes | None],
    *,
    max_bytes: int | None = None,
) -> tuple[int, int]:
    """LF-delimit and write rows. Embedded LFs are replaced with spaces
    because the bench input format can't represent them. Returns
    ``(rows_written, bytes_written)``."""
    written = 0
    bytes_written = 0
    with path.open("wb") as fh:
        for r in rows:
            if r is None:
                continue
            b = r.encode("utf-8") if isinstance(r, str) else bytes(r)
            if not b:
                continue
            if b"\n" in b:
                b = b.replace(b"\n", b" ")
            fh.write(b)
            fh.write(b"\n")
            written += 1
            bytes_written += len(b) + 1
            if max_bytes is not None and bytes_written >= max_bytes:
                break
    return written, bytes_written


def _require(mod_name: str, install_hint: str):
    try:
        return __import__(mod_name)
    except ImportError as exc:
        raise SystemExit(
            f"this dataset needs `{mod_name}`. install with: {install_hint}"
        ) from exc


# --- fetchers ---------------------------------------------------------------


def _fetch_tpch(target: Path, scale: float) -> None:
    duckdb = _require("duckdb", "pip install duckdb")
    con = duckdb.connect()
    con.execute("INSTALL tpch; LOAD tpch;")
    print(f"  generating tpch sf={scale}", file=sys.stderr)
    con.execute(f"CALL dbgen(sf = {scale})")
    for tbl in (
        "lineitem",
        "orders",
        "customer",
        "part",
        "partsupp",
        "supplier",
        "nation",
        "region",
    ):
        out = target / f"{tbl}.parquet"
        print(f"  writing {out.name}", file=sys.stderr)
        con.execute(f"COPY {tbl} TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    con.close()


_CLICKBENCH_SHARDS_URL = (
    "https://pub-3ba949c0f0354ac18db1f0f14f0a2c52.r2.dev"
    "/clickbench/parquet_many/hits_{idx}.parquet"
)
_CLICKBENCH_FULL_URL = (
    "https://pub-3ba949c0f0354ac18db1f0f14f0a2c52.r2.dev"
    "/clickbench/parquet_single/hits.parquet"
)


def _fetch_clickbench_sample(target: Path, *, num_shards: int = 6) -> None:
    # ClickHouse's own bucket (datasets.clickhouse.com) returns 403 as of 2026.
    # vortex hosts a sharded mirror on Cloudflare R2; pull the first
    # `num_shards` shards (~120 MB each) — plenty of rows for compression
    # stats at ~20× less I/O than the full hits.parquet.
    for idx in range(num_shards):
        _download(
            _CLICKBENCH_SHARDS_URL.format(idx=idx),
            target / f"hits_{idx}.parquet",
        )


def _fetch_clickbench_full(target: Path) -> None:
    # Full ~14.5 GB single hits.parquet — same string columns as the sample,
    # ~99 M rows instead of ~10 M. Opt-in via `--dataset clickbench-full`.
    _download(_CLICKBENCH_FULL_URL, target / "hits.parquet")


def _hf_column_to_txt(
    target: Path,
    *,
    repo: str,
    config: str | None,
    split: str,
    column: str,
    out_name: str,
    max_bytes: int | None = None,
) -> None:
    """Stream a single column from an HF dataset to LF-delimited bytes.

    Streaming avoids the ``download_and_prepare`` pipeline (which materialises
    the full dataset as Arrow under ``~/.cache/huggingface/datasets`` — many
    GB even for a single text column). We just read rows lazily and write
    the column out, no on-disk cache beyond ``out_name``."""
    datasets = _require("datasets", "pip install datasets")
    desc = repo + (f":{config}" if config else "") + f"[{split}].{column}"
    print(f"  streaming hf:{desc}", file=sys.stderr)
    ds = datasets.load_dataset(
        repo,
        name=config,
        split=split,
        streaming=True,
        trust_remote_code=True,
    )
    out = target / out_name
    print(f"  writing {out.name}", file=sys.stderr)
    n, nb = _write_lines(
        out,
        (row.get(column) for row in ds),
        max_bytes=max_bytes,
    )
    print(f"  wrote {n:,} rows ({nb / 1e6:.1f} MB)", file=sys.stderr)


# --- registry ---------------------------------------------------------------


@dataclass(frozen=True)
class Dataset:
    name: str
    description: str
    fetch: Callable[[Path], None]
    # When False, `--all-datasets` skips this entry; it still fetches via
    # explicit `--dataset NAME`. Used for outsized variants (e.g. the
    # 14.5 GB clickbench-full) that aren't a sensible default.
    include_in_all: bool = True


REGISTRY: dict[str, Dataset] = {
    "tpch-sf0.1": Dataset(
        "tpch-sf0.1",
        "TPC-H SF=0.1, all 8 tables as parquet (duckdb-generated).",
        lambda d: _fetch_tpch(d, 0.1),
    ),
    "tpch-sf1": Dataset(
        "tpch-sf1",
        "TPC-H SF=1, all 8 tables as parquet (duckdb-generated).",
        lambda d: _fetch_tpch(d, 1.0),
    ),
    "clickbench": Dataset(
        "clickbench",
        "ClickBench hits shards 0..5 (~720 MB; sample of the full hits.parquet).",
        _fetch_clickbench_sample,
    ),
    "clickbench-full": Dataset(
        "clickbench-full",
        "ClickBench full hits.parquet (~14.5 GB; --dataset opt-in, skipped by --all-datasets).",
        _fetch_clickbench_full,
        include_in_all=False,
    ),
    "amazon-books-titles": Dataset(
        "amazon-books-titles",
        "Amazon Reviews 2023, Books metadata `title` (OnPair paper).",
        lambda d: _hf_column_to_txt(
            d,
            repo="McAuley-Lab/Amazon-Reviews-2023",
            config="raw_meta_Books",
            split="full",
            column="title",
            out_name="amazon-books-titles.txt",
        ),
    ),
    "amazon-books-reviews": Dataset(
        "amazon-books-reviews",
        "Amazon Reviews 2023, Books reviews `text`, first 500 MiB (OnPair paper).",
        lambda d: _hf_column_to_txt(
            d,
            repo="McAuley-Lab/Amazon-Reviews-2023",
            config="raw_review_Books",
            split="full",
            column="text",
            out_name="amazon-books-reviews.txt",
            max_bytes=500 * 1024 * 1024,
        ),
    ),
    "news-headlines": Dataset(
        "news-headlines",
        "ABC News headlines, 1.2M rows (OnPair paper, via rajistics/million-headlines).",
        lambda d: _hf_column_to_txt(
            d,
            repo="rajistics/million-headlines",
            config=None,
            split="train",
            column="headline_text",
            out_name="news-headlines.txt",
        ),
    ),
    "sentiment140-tweets": Dataset(
        "sentiment140-tweets",
        "Sentiment140 1.6M tweets, `text` column (OnPair paper).",
        lambda d: _hf_column_to_txt(
            d,
            repo="stanfordnlp/sentiment140",
            config=None,
            split="train",
            column="text",
            out_name="sentiment140-tweets.txt",
        ),
    ),
}


# --- public api -------------------------------------------------------------


def _is_done(target: Path) -> bool:
    return (target / DONE).is_file()


def ensure(names: Iterable[str]) -> list[Path]:
    """Fetch any of ``names`` not already complete. Returns dataset dirs that
    are usable (``.done``-marked). Per-dataset failures warn and continue —
    one broken upstream shouldn't block the rest of an ``--all-datasets``
    sweep. Unknown names still hard-fail (typo defence)."""
    dirs: list[Path] = []
    for name in names:
        ds = REGISTRY.get(name)
        if ds is None:
            raise SystemExit(
                f"unknown dataset {name!r}; known: {', '.join(REGISTRY)}"
            )
        target = DATASETS_DIR / name
        if _is_done(target):
            dirs.append(target)
            continue
        print(f"fetching {name} → {target}", file=sys.stderr)
        # Wipe any partial state from a previous failed attempt.
        if target.exists():
            shutil.rmtree(target)
        target.mkdir(parents=True, exist_ok=True)
        try:
            ds.fetch(target)
        except Exception as exc:  # noqa: BLE001 — surface any fetch failure
            # Walk the cause chain to print the original error (HTTPError,
            # OSError, etc) — the outer wrapper is often DatasetGenerationError
            # which by itself doesn't say what went wrong.
            cause = exc
            chain = [repr(cause)]
            while cause.__cause__ is not None and cause.__cause__ is not cause:
                cause = cause.__cause__
                chain.append(repr(cause))
            print(f"warn: {name} fetch failed; skipping", file=sys.stderr)
            for line in chain:
                print(f"  · {line}", file=sys.stderr)
            shutil.rmtree(target, ignore_errors=True)
            continue
        (target / DONE).touch()
        dirs.append(target)
    return dirs


def clean(names: Iterable[str]) -> None:
    for name in names:
        target = DATASETS_DIR / name
        if target.exists():
            print(f"removing {target}", file=sys.stderr)
            shutil.rmtree(target)


# --- CLI --------------------------------------------------------------------


def _cmd_list() -> int:
    width = max(len(n) for n in REGISTRY)
    for ds in REGISTRY.values():
        target = DATASETS_DIR / ds.name
        mark = "x" if _is_done(target) else " "
        print(f"  [{mark}] {ds.name.ljust(width)}  {ds.description}")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("list", help="list registered datasets and their state")

    f = sub.add_parser("fetch", help="fetch one or more datasets")
    f.add_argument("names", nargs="+")

    sub.add_parser("fetch-all", help="fetch every registered dataset")

    c = sub.add_parser("clean", help="remove one or more datasets")
    c.add_argument("names", nargs="+")

    sub.add_parser("clean-all", help="remove every registered dataset")

    p = sub.add_parser("path", help="print the absolute path for a dataset dir")
    p.add_argument("name")

    args = ap.parse_args(argv)

    if args.cmd == "list":
        return _cmd_list()
    if args.cmd == "fetch":
        ensure(args.names)
        return 0
    if args.cmd == "fetch-all":
        ensure(list(REGISTRY))
        return 0
    if args.cmd == "clean":
        clean(args.names)
        return 0
    if args.cmd == "clean-all":
        clean(list(REGISTRY))
        return 0
    if args.cmd == "path":
        print(DATASETS_DIR / args.name)
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
