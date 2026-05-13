# pq

Command-line Parquet file viewer written in Rust.

## Status

- ✅ Schema command - done
- ✅ Head command - done
- ✅ Columns command - done
- ✅ Count command - done
- ✅ Filter command done
- ✅ Filter command done
- Create Parquet file from postgres data is downloading....


## Features

- Show schema (column names and data types)
- Show first N rows (like `head`)
- Show only column names
- Count total records
- export csv file 
- filter and out to csv file

## Installation

```bash
cargo install pq-rs
pq schema data.parquet
pq head data.parquet 5 optional(--csv filename)
pq columns data.parquet
pq count data.parquet 
pq export data.parquet --output filename.csv
pq filter data.parquet "--age>=18 and --city=tehran" --result.csv