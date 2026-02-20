# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.1.0] - 2026-02-20

### Fixed
- Fix LongToWide null-fill bloat: `FillModeNull` expanded hourly data into per-second null-filled rows (604K rows / 59MB for a 7-day query). Pass `nil` instead to only include timestamps present in source data.
- Fix `$__timeGroup` precision: DuckDB's `date_trunc` retains nanosecond residuals on `TIMESTAMP_NS` columns, causing `GROUP BY` to produce per-second rows. Replaced with epoch-based integer math (`epoch_ns // interval`).
- Fix `$__timeFilter` hardcoded to `time` column: now dynamically extracts the column name from the macro argument.
- Fix error messages: surface Arc errors directly in UI instead of generic "query failed" messages. Add user-friendly messages for timeouts, connection refused, and EOF errors while preserving the original error chain.

### Added
- Query splitting: break large time ranges into parallel chunks executed concurrently. Configurable via query editor dropdown (Auto, Off, 1h, 6h, 12h, 1d, 3d, 7d). Auto mode picks chunk size based on the time range.
- Smart split-skipping: automatically bypasses splitting for LIMIT queries, aggregations without `$__timeGroup`, queries without `$__timeFilter`, UNION queries, and window functions.
- Per-query database override: specify a different database per query panel, overriding the datasource default.
- Auto-migrate `rawSql` from Postgres/MySQL/MSSQL/ClickHouse datasources when switching to Arc.
- Auto-add `ORDER BY time ASC` for time series queries without one.
- Configurable max concurrency for query splitting (default 4) via datasource settings.
- 40 unit tests covering query splitting, macros, frame merging, and aggregation detection.

## [1.0.0] - 2025-10-22

### Added
- Initial release of Arc Grafana datasource plugin
- Apache Arrow protocol support for high-performance data transfer
- Backend plugin (Go) for secure credential storage
- Frontend UI components (TypeScript/React):
  - ConfigEditor for datasource settings
  - QueryEditor with SQL text area
  - VariableQueryEditor for template variables
- Grafana macro support:
  - `$__timeFilter(column)` - Automatic time range filtering
  - `$__timeFrom()` / `$__timeTo()` - Time boundaries
  - `$__interval` - Auto-calculated interval
  - `$__timeGroup(column, interval)` - Time bucketing
- Multi-database query support
- Health check endpoint
- Comprehensive documentation (README, ARCHITECTURE)
- Build system with webpack and mage
- Support for all Arrow data types (INT64, FLOAT64, STRING, TIMESTAMP, BOOL)

### Performance
- 7.36x faster queries compared to JSON for large datasets (100K+ rows)
- 43% smaller network payloads
- Zero-copy Arrow deserialization
- Tested with Arc's 2.43M records/sec write performance

### Security
- Encrypted API key storage using Grafana secrets
- Backend-only credential access
- HTTPS support

[Unreleased]: https://github.com/basekick-labs/grafana-arc-datasource/compare/v1.1.0...HEAD
[1.1.0]: https://github.com/basekick-labs/grafana-arc-datasource/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/basekick-labs/grafana-arc-datasource/releases/tag/v1.0.0
