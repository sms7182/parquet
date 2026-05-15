# pq

Command-line Parquet file viewer written in Rust.

> **Note on context:** In Iran, the internet has been cut off since February 28th. To keep my sanity and endure life within these walls, I turned to coding. I spent this time learning Rust and recreating the game of my childhood. The internet remains blocked, but I have managed to push this repository.

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
