# pq

Command-line Parquet file viewer written in Rust.

## Status

- ✅ Schema command - done
- ✅ Head command - done
- ✅ Columns command - done
- ✅ Count command - done

## Features

- Show schema (column names and data types)
- Show first N rows (like `head`)
- Show only column names
- Count total records

## Installation

```bash
cargo install pq-rs
pq schema data.parquet
pq head data.parquet 5 optional(--csv filename)
pq columns data.parquet
pq count data.parquet 
pq export data.parquet --output filename.csv