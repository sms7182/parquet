# Changelog

## [0.5.0] - 2026-05-13

### Added
- **Filter command with expression support**
  - Complex filtering using `--field operator value` syntax
  - Support for multiple conditions with `and` / `or` operators
  - Column name-based filtering (automatic schema detection)
  
- **Supported operators**:
  - Comparison: `>`, `<`, `>=`, `<=`, `=`
  - Logical: `and`, `or`
  - Example: `pq filter data.parquet --age>7 and --city=tehran`

- **CSV output for filtered results**
  - Automatic CSV generation from filtered rows
  - Column headers from Parquet schema
  - Proper CSV escaping for strings with commas or quotes
  - Support for multiple data types (string, integer, boolean, double)

- **Streaming filter processing**
  - Row-by-row processing without loading entire file
  - Constant memory usage (~5-10 MB)
  - Real-time output during filtering
  - Progress indication for large files

### Changed
- Filter evaluation now uses `RowAccessor` trait for type-safe column access
- Improved error messages for missing columns or invalid operators
- Graceful handling of type mismatches in conditions

### Commands
```bash

pq filter data.parquet --age>25 and --city=tehran --output result.csv


pq filter data.parquet --age<18 or --age>65 --output minors_and_seniors.csv


pq filter data.parquet --age>7 and --city=tehran or --status=active --filtered.csv


pq filter data.parquet --age>7 --output -