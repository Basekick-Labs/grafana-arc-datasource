# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.3.1] - 2026-09-02

### Changed
- Migrated the repository to Grafana's official create-plugin scaffolding, as required by the plugin catalog's source and build-tooling checks: plugin.json moved to src/, standard webpack/jest/eslint configuration under .config/, SDK-standard Magefile that emits the Go build manifest, and the grafana/plugin-actions release pipeline.
- Updated grafana-plugin-sdk-go to v0.296.4 and the frontend Grafana packages to 13.2.1, clearing all high and critical vulnerabilities the catalog's dependency scanner flagged in the previous lockfile.
- Minimum supported Grafana version is now 12.3 (floor of the current create-plugin toolchain).

### Added
- Playwright e2e smoke tests (config editor, query editor) running against Grafana in Docker on CI.

## [1.3.0] - 2026-09-02

### Added
- MessagePack protocol support: new datasource Protocol selector (Arrow, MessagePack, JSON) wired to Arc's `/api/v1/query/msgpack` endpoint (stable since Arc 26.09.1). The columnar envelope is decoded in a single streaming pass, with identical Grafana field types across all three protocols (int64/uint64 promoted to float64, all fields nullable). The legacy Use Arrow toggle is migrated automatically.
- MessagePack-aware error parsing: the msgpack endpoint encodes its errors as msgpack; these are now decoded to readable messages instead of falling through to a raw-bytes fallback.
- CI workflow: build, typecheck, lint, and tests for both frontend and backend on every push and pull request, plus a plugin-validator run against the packaged plugin.

### Fixed
- Arrow decoder: DuckDB DATE columns (Arrow date32) now decode to time fields instead of strings, matching the MessagePack path so switching protocols never changes a dashboard's field types.
- Health check: `Save & Test` failed on the default (Arrow) configuration because Arc rejects `SHOW DATABASES` on the Arrow endpoint. The probe now runs `SELECT 1` through the configured protocol.
- Release workflow: binary copy steps referenced the pre-rewrite Magefile output layout (`dist/linux_amd64/gpx_arc`), which no longer exists, so tag builds failed. The workflow now uses the flat `dist/gpx_arc_<os>_<arch>` layout the Magefile produces, pins Go from `go.mod` instead of a stale 1.21, runs tests before packaging, verifies the tag matches `plugin.json`, and re-enables plugin-validator.
- Grafana catalog readiness: README links converted to absolute URLs (required by the catalog), dead `docs.arc.io` link replaced with `docs.basekick.net/arc`, LICENSE upgraded to the full Apache 2.0 text, and stale documentation claims removed (editor auto-completion, `mage watch`, `npm run package`).

## [1.2.0] - 2026-05-14

### Fixed
- Promote INT64/UINT64 Arrow columns to float64 so Grafana panels (Stat, Time series) recognize aggregate results as numeric value fields.
- Aggregation detection now strips string literals before keyword matching, so a literal such as `'count of things'` no longer disables query splitting.
- Error chain preserved through request-failure wrapping for programmatic inspection via `errors.Is`/`errors.As`.

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

[Unreleased]: https://github.com/basekick-labs/grafana-arc-datasource/compare/v1.3.1...HEAD
[1.3.1]: https://github.com/basekick-labs/grafana-arc-datasource/compare/v1.3.0...v1.3.1
[1.3.0]: https://github.com/basekick-labs/grafana-arc-datasource/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/basekick-labs/grafana-arc-datasource/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/basekick-labs/grafana-arc-datasource/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/basekick-labs/grafana-arc-datasource/releases/tag/v1.0.0
