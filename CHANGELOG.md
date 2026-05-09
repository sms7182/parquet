# Changelog

## [0.3.0] - 2026-05-09

### Added
- **Streaming CSV export** for large Parquet files
  - Memory-efficient processing (constant ~5MB usage)
  - Support for files larger than available RAM
  - Progress indicator for long exports
  
- **New commands**:
  - `pq export <file.parquet> --output <file.csv>`
  - `pq  <file.parquet> head 5 --csv <file.csv>` (alternative syntax)

- **Row-group streaming** implementation
  - Process parquet row groups sequentially
  - Buffered CSV writing (128KB buffer)
  - Automatic flush after each row group

### Changed
- Internal refactoring to support streaming architecture
- Error handling now uses `Result` instead of `panic!`

### Performance
- Export speed: ~50-100 MB/s
- Memory usage: Constant 1-5 MB regardless of file size